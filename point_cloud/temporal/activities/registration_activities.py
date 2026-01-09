from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Tuple

from temporalio import activity

from lidar_app.app.repo import Repo
from lidar_app.app.config import settings
from lidar_app.app.s3_store import S3Store, S3Ref, safe_segment

def _dsreg_prefix(company_id: str, dataset_version_id: str) -> str:
    # общий “namespace” регистрации на dataset_version
    cid = safe_segment(company_id)
    dvid = safe_segment(dataset_version_id)
    return f"tenants/{cid}/dataset_versions/{dvid}/registration"

def _identity_pose() -> dict:
    return {"t": [0.0, 0.0, 0.0], "R": [[1.0,0.0,0.0],[0.0,1.0,0.0],[0.0,0.0,1.0]]}

def _add(a, b):
    return [a[0]+b[0], a[1]+b[1], a[2]+b[2]]

def _sub(a, b):
    return [a[0]-b[0], a[1]-b[1], a[2]-b[2]]

def _dist(a, b) -> float:
    return ((a[0]-b[0])**2 + (a[1]-b[1])**2 + (a[2]-b[2])**2) ** 0.5

@activity.defn
async def collect_registration_graph(company_id: str, dataset_version_id: str, schema_version: str) -> Dict[str, Any]:
    """
    Собирает:
    - anchors всех scans
    - edges: из БД (scan_edges) если есть, иначе из edges_proposed.json
    Возвращает компактный граф для solver-а.
    """
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        scans = repo.list_scans_by_dataset_version(dataset_version_id)
        scan_ids = [s.id for s in scans]

        # anchors
        anchors: Dict[str, dict] = {}
        for sid in scan_ids:
            art = repo.find_derived_artifact(sid, "derived.registration_anchors", schema_version)
            if not art:
                continue
            data = json.loads(s3.get_bytes(S3Ref(art.s3_bucket, art.s3_key)).decode("utf-8"))
            anchors[sid] = data

        # edges: предпочтительно из БД (если ты их туда пишешь), иначе из артефактов
        edges: List[dict] = []
        try:
            # если Repo.add_scan_edges уже есть, то логично иметь и repo.list_scan_edges
            db_edges = repo.list_scan_edges(dataset_version_id)
            edges = [{
                "from": e.scan_id_from,
                "to": e.scan_id_to,
                "kind": e.kind,
                "weight": float(e.weight),
                "transform_guess": e.transform_guess or {},
                "meta": e.meta or {},
            } for e in db_edges]
        except Exception:
            # fallback: читаем edges_proposed у каждого скана
            for sid in scan_ids:
                art = repo.find_derived_artifact(sid, "derived.registration_edges", schema_version)
                if not art:
                    continue
                data = json.loads(s3.get_bytes(S3Ref(art.s3_bucket, art.s3_key)).decode("utf-8"))
                for e in data.get("edges", []):
                    edges.append(e)

        return {
            "company_id": company_id,
            "dataset_version_id": dataset_version_id,
            "schema_version": schema_version,
            "scan_ids": scan_ids,
            "anchors": anchors,
            "edges": edges,
        }

    activity.heartbeat({"stage": "collect_graph"})
    return await asyncio.to_thread(_run)

@activity.defn
async def solve_pose_graph(company_id: str, dataset_version_id: str, schema_version: str, graph: Dict[str, Any]) -> Dict[str, Any]:

    """
    MVP solver:
    - строит “цепочку” по минимальным tail->head расстояниям (если есть anchors)
    - трансформы только translation (R = I)
    Это MVP, но уже даёт порядок и грубую глобальную раскладку.
    """
    def _run() -> Dict[str, Any]:
        scan_ids: List[str] = graph["scan_ids"]
        anchors: Dict[str, dict] = graph.get("anchors", {})
        edges: List[dict] = graph.get("edges", [])

        # 1) если есть edges с transform_guess.t — используем их для построения ориентированного графа
        adj: Dict[str, List[Tuple[str, dict]]] = {sid: [] for sid in scan_ids}
        for e in edges:
            a = e.get("from")
            b = e.get("to")
            tg = e.get("transform_guess") or {}
            t = tg.get("t")
            if a in adj and b in adj and isinstance(t, list) and len(t) == 3:
                adj[a].append((b, {"t": t, "R": tg.get("R") or _identity_pose()["R"], "w": float(e.get("weight", 1.0))}))

        # 2) выбираем “корень”
        # MVP: первый scan_id, или тот у кого есть head
        root = next((sid for sid in scan_ids if anchors.get(sid, {}).get("head")), scan_ids[0])

        # 3) BFS: распространяем позы по ребрам
        poses: Dict[str, dict] = {root: _identity_pose()}
        q = [root]
        while q:
            cur = q.pop(0)
            cur_pose = poses[cur]
            for nxt, tr in adj.get(cur, []):
                if nxt in poses:
                    continue
                # composition: только t (R игнорируем в MVP)
                init_pose = {
                    "t": _add(cur_pose["t"], tr["t"]),
                    "R": tr.get("R") or _identity_pose()["R"],
                }
                poses[nxt] = init_pose
                q.append(nxt)

        # 4) если граф дырявый — попробуем простую “tail->head” эвристику добить
        unresolved = [sid for sid in scan_ids if sid not in poses and anchors.get(sid)]
        if unresolved and anchors.get(root):
            # наивно: приклеить ближайший head к текущему tail
            placed = set(poses.keys())
            for sid in unresolved:
                # найдём ближайший к любому placed
                best = None
                best_d = 1e18
                sid_head = anchors[sid].get("head")
                if not sid_head:
                    continue
                for pid in list(placed):
                    p_tail = anchors.get(pid, {}).get("tail")
                    if not p_tail:
                        continue
                    d = _dist(p_tail, sid_head)
                    if d < best_d:
                        best_d = d
                        best = pid
                if best is not None and best_d < 100.0:  # широкий порог для MVP
                    # “поставим” sid так, чтобы его head совпал с best.tail
                    bt = anchors[best]["tail"]
                    poses[sid] = {"t": _sub(bt, sid_head), "R": _identity_pose()["R"]}
                    placed.add(sid)

        for sid in scan_ids:
            poses.setdefault(sid, _identity_pose())

        diagnostics = {
            "root": root,
            "poses_count": len(poses),
            "scans_total": len(scan_ids),
            "unresolved": [sid for sid in scan_ids if sid not in poses],
            "edges_used": sum(len(v) for v in adj.values()),
        }

        return {"poses": poses, "diagnostics": diagnostics}

    activity.heartbeat({"stage": "solve_pose_graph"})
    return await asyncio.to_thread(_run)

@activity.defn
async def persist_pose_graph_solution(
    company_id: str,
    dataset_version_id: str,
    schema_version: str,
    solution: Dict[str, Any],
    force: bool,
) -> Dict[str, Any]:
    """
    Пишет solution+diagnostics в S3 и фиксирует позы в Postgres (scan_poses).
    """
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        prefix = _dsreg_prefix(company_id, dataset_version_id)
        sol_key = f"{prefix}/v{schema_version}/pose_graph_solution.json"
        dia_key = f"{prefix}/v{schema_version}/pose_graph_diagnostics.json"

        # если не force и уже есть — можно выйти (MVP: просто проверим S3 наличием)
        # (если хочешь — сделаем проверку через artifacts таблицу)
        # Здесь пропускаю “exists” ради простоты.

        body = json.dumps(solution["poses"], ensure_ascii=False, indent=2).encode("utf-8")
        etag1, size1 = s3.put_bytes(S3Ref(settings.s3_bucket, sol_key), body, content_type="application/json")

        dia = json.dumps(solution["diagnostics"], ensure_ascii=False, indent=2).encode("utf-8")
        etag2, size2 = s3.put_bytes(S3Ref(settings.s3_bucket, dia_key), dia, content_type="application/json")

        # SoT: записываем позы
        poses_written = 0
        for scan_id, pose in (solution["poses"] or {}).items():
            repo.upsert_scan_pose(company_id, dataset_version_id, scan_id, pose, quality=0.0)
            poses_written += 1

        return {
            "solution_key": sol_key,
            "diagnostics_key": dia_key,
            "poses_written": poses_written,
            "solution_etag": etag1,
            "diagnostics_etag": etag2,
        }

    activity.heartbeat({"stage": "persist_solution"})
    return await asyncio.to_thread(_run)
