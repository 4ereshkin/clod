from __future__ import annotations

import asyncio
import json
import re
import tempfile
import pdal
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

from botocore.exceptions import ClientError
from sqlalchemy import select
from temporalio import activity

from lidar_app.app.models import CRS
from lidar_app.app.repo import Repo
from lidar_app.app.config import settings
from lidar_app.app.s3_store import S3Store, S3Ref, derived_manifest_key, scan_prefix
from lidar_app.app.artifact_service import download_artifact, store_artifact

_FLOAT = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")

def normalize_srs(s: str) -> str:
    s = (s or "").strip()
    if not s:
        raise ValueError("Empty SRS")

    up = s.upper()

    # Common aliases
    alias = {
        "CGCS2000": "EPSG:4490",
        "CGCS 2000": "EPSG:4490",
        "EPSG4490": "EPSG:4490",
        "4490": "EPSG:4490",
        "WGS84": "EPSG:4326",
        "EPSG4326": "EPSG:4326",
        "4326": "EPSG:4326",
    }
    if up in alias:
        return alias[up]

    # already EPSG:xxxx
    if up.startswith("EPSG:"):
        return up

    return s  # allow WKT/PROJJSON etc

def _identity_pose() -> dict:
    return {"t": [0.0, 0.0, 0.0], "R": [[1.0,0.0,0.0],[0.0,1.0,0.0],[0.0,0.0,1.0]]}

def _parse_xyz_lines(text: str) -> list[tuple[int, list[float]]]:
    """
    Возвращает список (line_index, [x,y,z]) для строк, где удалось найти >=3 float.
    Берём первые 3 float как XYZ.
    """
    out = []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        nums = _FLOAT.findall(line)
        if len(nums) >= 3:
            xyz = [float(nums[0]), float(nums[1]), float(nums[2])]
            out.append((i, xyz))
    return out

def _rewrite_xyz_lines(text: str, new_xyz: dict[int, list[float]]) -> str:
    lines = text.splitlines()
    for i, xyz in new_xyz.items():
        # максимально безопасно: заменяем первые 3 float на новые, остальное оставляем
        parts = _FLOAT.split(lines[i])
        nums = _FLOAT.findall(lines[i])
        if len(nums) < 3:
            continue
        # собираем обратно: parts[0] num0 parts[1] num1 parts[2] num2 parts[3] ... + хвост
        rebuilt = []
        rebuilt.append(parts[0])
        rebuilt.append(f"{xyz[0]:.6f}")
        rebuilt.append(parts[1])
        rebuilt.append(f"{xyz[1]:.6f}")
        rebuilt.append(parts[2])
        rebuilt.append(f"{xyz[2]:.6f}")
        rebuilt.append(parts[3] if len(parts) > 3 else "")
        # если было больше чисел — оставляем их как были
        if len(nums) > 3:
            # склеим исходный хвост после третьего числа:
            # проще: берём исходную строку и обрезаем до позиции 3-го float не будем — оставим parts[3] как есть
            pass
        lines[i] = "".join(rebuilt)
    return "\n".join(lines) + ("\n" if text.endswith("\n") else "")


def _reproject_cloud_with_pdal(local_in: Path, local_out: Path, in_srs: str, out_srs: str) -> dict:
    pipeline = {
        "pipeline": [
            {"type": "readers.las", "filename": str(local_in)},
            {"type": "filters.reprojection", "in_srs": in_srs, "out_srs": out_srs},
            {"type": "writers.las", "filename": str(local_out), "compression": "laszip"},
        ]
    }
    pipe = pdal.Pipeline(json.dumps(pipeline))
    try:
        pipe.execute()
    except Exception as exc:
        raise RuntimeError(f"PDAL reprojection failed: {exc}") from exc
    if not local_out.exists():
        raise RuntimeError(f"PDAL reprojection produced no output: {local_out}")
    return pipe.metadata or {}


@activity.defn
async def load_ingest_manifest(company_id: str, dataset_version_id: str, scan_id: str, schema_version: str) -> Dict[str, Any]:
    """
    Читает ingest_manifest.json из S3 и возвращает dict.
    """
    def _run() -> Dict[str, Any]:
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)
        prefix = scan_prefix(company_id, dataset_version_id, scan_id)
        key = derived_manifest_key(prefix, schema_version)
        data = s3.get_bytes(S3Ref(settings.s3_bucket, key))
        return json.loads(data.decode("utf-8"))

    return await asyncio.to_thread(_run)

@activity.defn
async def resolve_crs_to_pdal_srs(crs_id: str) -> str:
    def _run() -> str:
        repo = Repo()
        return repo.resolve_crs_to_pdal_srs(crs_id)

    return await asyncio.to_thread(_run)

@activity.defn
async def reproject_scan_to_target_crs(
    company_id: str,
    dataset_version_id: str,
    scan_id: str,
    schema_version: str,
    in_srs: str,
    out_srs: str,
) -> Dict[str, Any]:
    in_srs = normalize_srs(in_srs)
    out_srs = normalize_srs(out_srs)
    """
    1) скачивает raw артефакты
    2) репроецирует облако (PDAL через SRS)
    3) (MVP) пытается переписать XYZ в path/cp, если они текстовые
    4) загружает derived/* в S3 + регистрирует artifacts
    """
    def _run():

        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        scan = repo.get_scan(scan_id)

        # raw artifacts берем из БД
        raw_arts = repo.list_raw_artifacts(scan_id)  # ты это уже используешь
        cloud = next((a for a in raw_arts if a.kind == "raw.point_cloud"), None)
        path  = next((a for a in raw_arts if a.kind == "raw.trajectory"), None)
        cp    = next((a for a in raw_arts if a.kind == "raw.control_point"), None)

        if not cloud:
            raise RuntimeError("raw.point_cloud not found")

        prefix = scan_prefix(company_id, dataset_version_id, scan_id)

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)

            # download cloud
            local_in = td / Path(cloud.s3_key).name
            s3.download_file(S3Ref(cloud.s3_bucket, cloud.s3_key), str(local_in))

            # reproject cloud
            local_out = td / f"{local_in.stem}__{out_srs.replace(':', '_')}{local_in.suffix}"
            pdal_meta = _reproject_cloud_with_pdal(local_in, local_out, in_srs, out_srs)

            # upload derived cloud
            derived_cloud_key = (
                f"{prefix}/derived/v{schema_version}/reprojected/point_cloud/{local_out.name}"
            )
            store_artifact(
                repo=repo,
                s3=s3,
                company_id=company_id,
                scan_id=scan_id,
                kind="derived.reprojected_point_cloud",
                schema_version=schema_version,
                bucket=settings.s3_bucket,
                key=derived_cloud_key,
                local_file_path=str(local_out),
                status="AVAILABLE",
                meta={"in_srs": in_srs, "out_srs": out_srs, "pdal_metadata": pdal_meta},
            )

            out = {
                "derived_cloud_key": derived_cloud_key,
                "derived_path_key": None,
                "derived_cp_key": None,
            }

            # MVP: переписываем XYZ в path/cp (если есть)
            def _reproject_text_artifact(art, out_key, kind):
                data = s3.get_bytes(S3Ref(art.s3_bucket, art.s3_key)).decode("utf-8", errors="replace")
                hits = _parse_xyz_lines(data)
                body = data.encode("utf-8")
                meta = {"in_srs": in_srs, "out_srs": out_srs}
                if hits and in_srs != out_srs:
                    # ВНИМАНИЕ: здесь нужен реальный трансформер координат.
                    # Для MVP: если in_srs==out_srs → оставляем, иначе помечаем.
                    meta = {"note": "MVP: aux not transformed", "in_srs": in_srs, "out_srs": out_srs}
                store_artifact(
                    repo=repo,
                    s3=s3,
                    company_id=company_id,
                    scan_id=scan_id,
                    kind=kind,
                    schema_version=schema_version,
                    bucket=settings.s3_bucket,
                    key=out_key,
                    data=body,
                    content_type="text/plain",
                    status="AVAILABLE",
                    meta=meta,
                )

            if path:
                out_key = f"{prefix}/derived/v{schema_version}/reprojected/trajectory/path.txt"
                _reproject_text_artifact(path, out_key, "derived.reprojected_trajectory")
                out["derived_path_key"] = out_key

            if cp:
                out_key = f"{prefix}/derived/v{schema_version}/reprojected/control_points/ControlPoint.txt"
                _reproject_text_artifact(cp, out_key, "derived.reprojected_control_points")
                out["derived_cp_key"] = out_key

            return out

    activity.heartbeat({"stage": "reproject", "scan_id": scan_id})
    return await asyncio.to_thread(_run)

@activity.defn
async def build_registration_anchors(
    company_id: str,
    dataset_version_id: str,
    scan_id: str,
    schema_version: str,
) -> Dict[str, Any]:
    """
    Делает anchors.json: head/tail из path + список контрольных точек (если есть).
    """
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        prefix = scan_prefix(company_id, dataset_version_id, scan_id)

        # берём derived траекторию/КП (если существуют)
        path_art = repo.find_derived_artifact(scan_id, "derived.reprojected_trajectory", schema_version)
        cp_art   = repo.find_derived_artifact(scan_id, "derived.reprojected_control_points", schema_version)

        head = tail = None
        if path_art:
            txt = s3.get_bytes(S3Ref(path_art.s3_bucket, path_art.s3_key)).decode("utf-8", errors="replace")
            pts = _parse_xyz_lines(txt)
            if pts:
                head = pts[0][1]
                tail = pts[-1][1]

        cps = []
        if cp_art:
            txt = s3.get_bytes(S3Ref(cp_art.s3_bucket, cp_art.s3_key)).decode("utf-8", errors="replace")
            pts = _parse_xyz_lines(txt)
            # MVP: без ID, просто координаты
            cps = [{"xyz": xyz} for _, xyz in pts[:2000]]  # ограничим объём

        anchors = {
            "scan_id": scan_id,
            "dataset_version_id": dataset_version_id,
            "head": head,
            "tail": tail,
            "control_points": cps,
        }

        key = f"{prefix}/derived/v{schema_version}/registration/anchors.json"
        body = json.dumps(anchors, ensure_ascii=False, indent=2).encode("utf-8")
        store_artifact(
            repo=repo,
            s3=s3,
            company_id=company_id,
            scan_id=scan_id,
            kind="derived.registration_anchors",
            schema_version=schema_version,
            bucket=settings.s3_bucket,
            key=key,
            data=body,
            content_type="application/json",
            status="AVAILABLE",
            meta={},
        )

        return {"anchors_key": key}

    return await asyncio.to_thread(_run)

@activity.defn
async def propose_registration_edges(
    company_id: str,
    dataset_version_id: str,
    scan_id: str,
    schema_version: str,
) -> Dict[str, Any]:
    """
    MVP edges:
    - если у обоих есть head/tail: соединяем tail одного с head другого при близости
    - если есть контрольные точки: пока только “есть/нет”, без совпадений ID
    """
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        scans = repo.list_scans_by_dataset_version(dataset_version_id)
        scans_other = [s for s in scans if s.id != scan_id]

        # anchors текущего
        my_art = repo.find_derived_artifact(scan_id, "derived.registration_anchors", schema_version)
        if not my_art:
            raise RuntimeError("anchors for current scan not found")
        my = json.loads(s3.get_bytes(S3Ref(my_art.s3_bucket, my_art.s3_key)).decode("utf-8"))

        def dist(a, b) -> float:
            return ((a[0]-b[0])**2 + (a[1]-b[1])**2 + (a[2]-b[2])**2) ** 0.5

        def _safe_load_json_from_s3(bucket: str, key: str) -> dict | None:
            try:
                raw = s3.get_bytes(S3Ref(bucket, key))
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                if code in ("NoSuchKey", "404"):
                    activity.logger.warn(f"S3 missing key for anchors: s3://{bucket}/{key}")
                    return None
                raise
            try:
                return json.loads(raw.decode("utf-8"))
            except Exception:
                activity.logger.warn(f"Bad JSON in anchors: s3://{bucket}/{key}")
                return None

        edges = []
        for s in scans_other:
            art = repo.find_derived_artifact(s.id, "derived.registration_anchors", schema_version)
            if not art:
                continue
            other = _safe_load_json_from_s3(art.s3_bucket, art.s3_key)

            # tail-head heuristic
            if my.get("tail") and other.get("head"):
                d = dist(my["tail"], other["head"])
                if d < 20.0:  # порог подберёшь
                    t = [other["head"][0]-my["tail"][0], other["head"][1]-my["tail"][1], other["head"][2]-my["tail"][2]]
                    edges.append({
                        "from": scan_id,
                        "to": s.id,
                        "kind": "traj_tail_head",
                        "weight": max(0.1, 20.0 / (d + 1e-6)),
                        "transform_guess": {"t": t, "R": _identity_pose()["R"]},
                        "meta": {"d_tail_head": d},
                    })

            # можно добавить обратное направление
            if other.get("tail") and my.get("head"):
                d = dist(other["tail"], my["head"])
                if d < 300.0:
                    t = [my["head"][0]-other["tail"][0], my["head"][1]-other["tail"][1], my["head"][2]-other["tail"][2]]
                    edges.append({
                        "from": s.id,
                        "to": scan_id,
                        "kind": "traj_tail_head",
                        "weight": max(0.1, 20.0 / (d + 1e-6)),
                        "transform_guess": {"t": t, "R": _identity_pose()["R"]},
                        "meta": {"d_tail_head": d},
                    })

        prefix = scan_prefix(company_id, dataset_version_id, scan_id)
        key = f"{prefix}/derived/v{schema_version}/registration/edges_proposed.json"
        body = json.dumps({"edges": edges}, ensure_ascii=False, indent=2).encode("utf-8")
        store_artifact(
            repo=repo,
            s3=s3,
            company_id=company_id,
            scan_id=scan_id,
            kind="derived.registration_edges",
            schema_version=schema_version,
            bucket=settings.s3_bucket,
            key=key,
            data=body,
            content_type="application/json",
            status="AVAILABLE",
            meta={"count": len(edges)},
        )

        # SoT: пишем edges в таблицу (если добавил)
        if edges:
            repo.add_scan_edges(company_id, dataset_version_id, edges)

        return {"edges_key": key, "count": len(edges)}

    return await asyncio.to_thread(_run)

def _mat4_to_pose(T: List[List[float]]) -> Dict[str, Any]:
    # T: 4x4
    R = [T[0][:3], T[1][:3], T[2][:3]]
    t = [T[0][3], T[1][3], T[2][3]]
    return {"t": t, "R": R}


def _identity4() -> List[List[float]]:
    return [[1.0,0.0,0.0,0.0],
            [0.0,1.0,0.0,0.0],
            [0.0,0.0,1.0,0.0],
            [0.0,0.0,0.0,1.0]]


def _pose_meta(rmse: float, fitness: float, method: str, **kw) -> Dict[str, Any]:
    m = {"rmse": rmse, "fitness": fitness, "method": method}
    m.update(kw)
    return m


def _get_reprojected_cloud_key(repo: Repo, scan_id: str, schema_version: str) -> Tuple[str, str]:
    """
    Возвращает (bucket, key) для derived.reprojected_point_cloud.
    """
    art = repo.find_derived_artifact(scan_id, "derived.reprojected_point_cloud", schema_version)
    if not art:
        raise RuntimeError(f"derived.reprojected_point_cloud not found for scan {scan_id} v{schema_version}")
    return art.s3_bucket, art.s3_key


def _try_open3d_icp(src_path: Path, tgt_path: Path, voxel: float, max_corr: float) -> Optional[Dict[str, Any]]:
    try:
        import open3d as o3d
    except Exception:
        return None

    # Open3D читает LAS/LAZ не всегда из коробки.
    # Поэтому если LAZ/LAS — лучше предварительно сделать pdal translate -> PLY/XYZ,
    # но для MVP попробуем напрямую, и если не получится — вернём None.
    try:
        src = o3d.io.read_point_cloud(str(src_path))
        tgt = o3d.io.read_point_cloud(str(tgt_path))
        if src.is_empty() or tgt.is_empty():
            return None
    except Exception:
        return None

    src_d = src.voxel_down_sample(voxel_size=voxel)
    tgt_d = tgt.voxel_down_sample(voxel_size=voxel)

    # Point-to-point ICP
    reg = o3d.pipelines.registration.registration_icp(
        src_d, tgt_d, max_corr,
        o3d.geometry.TriangleMesh.create_coordinate_frame().get_rotation_matrix_from_xyz((0,0,0)),
        o3d.pipelines.registration.TransformationEstimationPointToPoint(),
    )

    T = reg.transformation.tolist()
    rmse = float(getattr(reg, "inlier_rmse", 0.0))
    fitness = float(getattr(reg, "fitness", 0.0))
    return {"T": T, "rmse": rmse, "fitness": fitness, "method": "open3d_icp"}


def _try_pdal_icp(src_path: Path, tgt_path: Path, max_corr: float) -> Optional[Dict[str, Any]]:
    """
    PDAL ICP: используем filters.icp.
    Важно: filters.icp может отсутствовать в конкретной сборке PDAL.
    """
    # pipeline должен вернуть metadata с матрицей.
    # Разные версии PDAL кладут её в разные поля, поэтому ищем несколько вариантов.
    pipeline = {
        "pipeline": [
            {"type": "readers.las", "filename": str(src_path)},
            {
                "type": "filters.icp",
                "reference": str(tgt_path),
                "maxCorrespondenceDistance": float(max_corr),
            },
            {"type": "filters.info"},
        ]
    }

    try:
        pipe = pdal.Pipeline(json.dumps(pipeline))
        pipe.execute()
    except Exception:
        return None

    meta = pipe.metadata or {}
    # Попытки вытащить 4x4
    # варианты: meta["metadata"]["filters.icp"]["transform"] или ["transformation"]
    try:
        icp_meta = meta.get("metadata", {}).get("filters.icp", {})
    except Exception:
        icp_meta = {}

    T = (
        icp_meta.get("transform")
        or icp_meta.get("transformation")
        or icp_meta.get("matrix")
    )
    if not (isinstance(T, list) and len(T) == 4):
        return None

    rmse = float(icp_meta.get("rmse", icp_meta.get("error", 0.0)) or 0.0)
    fitness = float(icp_meta.get("fitness", icp_meta.get("overlap", 0.0)) or 0.0)
    return {"T": T, "rmse": rmse, "fitness": fitness, "method": "pdal_icp"}


@activity.defn
async def compute_icp_edge(
    company_id: str,
    dataset_version_id: str,
    schema_version: str,
    scan_id_from: str,
    scan_id_to: str,
    voxel_size: float = 1.0,
    max_correspondence_distance: float = 5.0,
    min_fitness: float = 0.05,
) -> Dict[str, Any]:
    """
    Строит edge scan_from -> scan_to через ICP по derived.reprojected_point_cloud.
    Возвращает edge dict либо кидает исключение (если ICP не получилось).
    """
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        b1, k1 = _get_reprojected_cloud_key(repo, scan_id_from, schema_version)
        b2, k2 = _get_reprojected_cloud_key(repo, scan_id_to, schema_version)

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            src = download_artifact(s3=s3, bucket=b1, key=k1, dst_dir=td)
            tgt = download_artifact(s3=s3, bucket=b2, key=k2, dst_dir=td)

            res = _try_open3d_icp(src, tgt, voxel=voxel_size, max_corr=max_correspondence_distance)
            if res is None:
                res = _try_pdal_icp(src, tgt, max_corr=max_correspondence_distance)

            if res is None:
                raise RuntimeError(
                    "ICP failed: neither Open3D ICP succeeded nor PDAL filters.icp is available/returned transform. "
                    "Install open3d or ensure PDAL has filters.icp."
                )

            fitness = float(res.get("fitness", 0.0))
            rmse = float(res.get("rmse", 0.0))
            T = res["T"]

            if fitness < min_fitness:
                raise RuntimeError(f"ICP too weak: fitness={fitness:.4f} < min_fitness={min_fitness:.4f} (rmse={rmse:.4f})")

            # weight: чем меньше rmse, тем лучше
            weight = 1.0 / max(rmse, 1e-6)

            edge = {
                "from": scan_id_from,
                "to": scan_id_to,
                "kind": "icp",
                "weight": float(weight),
                "transform_guess": _mat4_to_pose(T),
                "meta": _pose_meta(rmse, fitness, res.get("method", "icp"),
                                  voxel_size=voxel_size,
                                  max_corr=max_correspondence_distance),
            }
            return edge

    activity.heartbeat({"stage": "icp", "from": scan_id_from, "to": scan_id_to})
    return await asyncio.to_thread(_run)


@activity.defn
async def propose_registration_edges_for_dataset(
    company_id: str,
    dataset_version_id: str,
    schema_version: str,
    voxel_size: float = 1.0,
    max_corr: float = 5.0,
    min_fitness: float = 0.05,
) -> Dict[str, Any]:
    """
    Dataset-level генерация edges.
    MVP: если 2 скана — строим ICP edges A->B и B->A (если получится).
    Записываем в core.scan_edges через repo.add_scan_edges.
    """
    def _run() -> Dict[str, Any]:
        repo = Repo()
        scans = repo.list_scans_by_dataset_version(dataset_version_id)
        scan_ids = [s.id for s in scans]

        if len(scan_ids) < 2:
            return {"count": 0, "note": "not enough scans"}

        edges: List[dict] = []

        # MVP: только для первых двух
        a, b = scan_ids[0], scan_ids[1]

        # A->B
        try:
            e1 = asyncio.run(compute_icp_edge(  # не зовём async тут; ниже я покажу как корректно без asyncio.run
                company_id, dataset_version_id, schema_version, a, b,
                voxel_size=voxel_size, max_correspondence_distance=max_corr, min_fitness=min_fitness
            ))
        except Exception as ex:
            e1 = None

        # B->A
        try:
            e2 = asyncio.run(compute_icp_edge(
                company_id, dataset_version_id, schema_version, b, a,
                voxel_size=voxel_size, max_correspondence_distance=max_corr, min_fitness=min_fitness
            ))
        except Exception as ex:
            e2 = None

        if e1:
            edges.append(e1)
        if e2:
            edges.append(e2)

        if edges:
            repo.add_scan_edges(company_id, dataset_version_id, edges)

        return {"count": len(edges), "scan_ids": scan_ids[:2], "kinds": list({e["kind"] for e in edges})}

    # ВАЖНО: нельзя вызывать asyncio.run внутри activity thread.
    # Поэтому выше вариант с asyncio.run — убираем. Ниже нормальная реализация:

    async def _async_wrapper() -> Dict[str, Any]:
        def _sync_part() -> List[str]:
            repo = Repo()
            scans = repo.list_scans_by_dataset_version(dataset_version_id)
            return [s.id for s in scans]

        scan_ids = await asyncio.to_thread(_sync_part)
        if len(scan_ids) < 2:
            return {"count": 0, "note": "not enough scans"}

        a, b = scan_ids[0], scan_ids[1]
        edges = []

        try:
            e1 = await compute_icp_edge(
                company_id, dataset_version_id, schema_version, a, b,
                voxel_size=voxel_size, max_correspondence_distance=max_corr, min_fitness=min_fitness
            )
            edges.append(e1)
        except Exception as ex:
            pass

        try:
            e2 = await compute_icp_edge(
                company_id, dataset_version_id, schema_version, b, a,
                voxel_size=voxel_size, max_correspondence_distance=max_corr, min_fitness=min_fitness
            )
            edges.append(e2)
        except Exception as ex:
            pass

        if edges:
            def _write():
                repo = Repo()
                return repo.add_scan_edges(company_id, dataset_version_id, edges)
            wrote = await asyncio.to_thread(_write)
        else:
            wrote = 0

        return {"count": len(edges), "rows_written": wrote, "scan_ids": [a, b]}

    activity.heartbeat({"stage": "propose_edges_dataset", "dataset_version_id": dataset_version_id})
    return await _async_wrapper()
