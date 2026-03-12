import concurrent.futures
import os
import json
from pathlib import Path
from typing import Dict, Any, List

import pdal
import numpy as np
import open3d as o3d
from temporalio import activity
from temporalio.exceptions import ApplicationError

from application.common.config import get_settings
from application.common.contracts import FailedEvent, ScenarioResult, StatusEvent
from application.common.interfaces import EventPublisher, StatusStore
from infrastructure.s3 import S3Client


class RegistrationActivitiesV1:
    def __init__(
            self,
            s3_client: S3Client,
            publisher: EventPublisher,
            status_store: StatusStore
    ):
        self.settings = get_settings().s3
        self.s3_client = s3_client
        self.publisher = publisher
        self.status_store = status_store

    @activity.defn
    async def download_scan(self, key: str, dst_dir: str, filename: str) -> str:
        """Асинхронно скачивает объект из S3 по ключу."""
        activity.heartbeat({"stage": "downloading", "key": key})
        dst_path = Path(dst_dir)
        dst_path.mkdir(parents=True, exist_ok=True)
        local_file_path = dst_path / filename
        await self.s3_client.download_object(key=key, dest_path=str(local_file_path))
        return str(local_file_path)

    @activity.defn
    async def upload_s3_object(self, local_path: str, key: str) -> dict[str, str]:
        """Загружает файл в S3 и возвращает {'s3_key': key, 'etag': etag}."""
        etag, _ = self.s3_client.calc_md5(local_path)
        await self.s3_client.upload_object(file_path=local_path, object_name=key)
        return {"s3_key": key, "etag": etag}

    @activity.defn
    def save_dict_to_json(self, data: dict, dst_path: str) -> str:
        with open(dst_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return dst_path

    @activity.defn
    async def publish_status_activity(self, status_data: dict) -> None:
        event = StatusEvent.model_validate(status_data)
        await self.status_store.set_status(workflow_id=event.workflow_id, status=event.status.value,
                                           payload=status_data)
        await self.publisher.publish_status(event)

    @activity.defn
    async def publish_completed_activity(self, result_data: dict) -> None:
        event = ScenarioResult.model_validate(result_data)
        await self.status_store.set_status(workflow_id=event.workflow_id, status=event.status.value,
                                           payload=result_data)
        await self.publisher.publish_completed(event)

    @activity.defn
    async def publish_failed_activity(self, failed_data: dict) -> None:
        event = FailedEvent.model_validate(failed_data)
        await self.status_store.set_status(workflow_id=event.workflow_id, status=event.status.value,
                                           payload=failed_data)
        await self.publisher.publish_failed(event)

    @activity.defn
    def prepare_scan_for_registration(self, cloud_s3_key: str, trajectory_s3_key: str | None, voxel_size: float,
                                      dst_dir: str) -> dict[str, Any]:
        activity.heartbeat({'stage': 'streaming_copc_and_anchors'})

        os.environ['AWS_ACCESS_KEY_ID'] = self.settings.access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = self.settings.secret_key
        os.environ['AWS_S3_ENDPOINT'] = self.settings.endpoint.replace("http://", "").replace('https://', '')
        os.environ['AWS_HTTPS'] = 'NO' if 'http://' in self.settings.endpoint else 'YES'
        os.environ['AWS_VIRTUAL_HOSTING'] = 'FALSE'

        base_name = cloud_s3_key.split('/')[-1].split('.')[0]
        dst_path = Path(dst_dir)
        dst_path.mkdir(parents=True, exist_ok=True)

        downsampled_path = dst_path / f"{base_name}_voxel_{voxel_size}.ply"
        copc_vsi_url = f'/vsis3/{self.settings.bucket}/{cloud_s3_key}'

        pipeline_json = [
            {'type': 'readers.copc', 'filename': copc_vsi_url, 'resolution': voxel_size},
            {'type': 'filters.voxelcenternearestneighbor', 'cell': voxel_size},
            {'type': 'writers.ply', 'filename': str(downsampled_path), 'faces': False}
        ]

        try:
            pipeline = pdal.Pipeline(json.dumps(pipeline_json))
            pipeline.execute()
        except Exception as e:
            raise ApplicationError(f'Failed to execute streaming PDAL pipeline: \n{e}')

        pcd = o3d.io.read_point_cloud(str(downsampled_path))
        radius_normal = voxel_size * 2.0
        pcd.estimate_normals(search_param=o3d.geometry.KDTreeSearchParamHybrid(radius=radius_normal, max_nn=30))
        o3d.io.write_point_cloud(str(downsampled_path), pcd)

        head, tail = None, None
        local_trajectory_path = trajectory_s3_key  # Пока заглушка
        if local_trajectory_path and os.path.exists(local_trajectory_path):
            pts = np.loadtxt(local_trajectory_path, delimiter=None, usecols=(0, 1, 2))
            if len(pts) > 0:
                head = pts[0].tolist()
                tail = pts[-1].tolist()

        return {'downsampled_cloud_path': str(downsampled_path), 'anchors': {'head': head, 'tail': tail}}

    @activity.defn
    def propose_edges(self, scans_anchors: dict[str, dict[str, Any]], distance_threshold: float = 20.0) -> list[
        dict[str, Any]]:
        edges = []
        scan_ids = list(scans_anchors.keys())

        for i, sid_from in enumerate(scan_ids):
            for j, sid_to in enumerate(scan_ids):
                if i == j: continue

                my_tail = scans_anchors[sid_from].get('tail')
                other_head = scans_anchors[sid_to].get('head')

                if my_tail and other_head:
                    d = sum((a - b) ** 2 for a, b in zip(my_tail, other_head)) ** 0.5
                    if d < distance_threshold:
                        t = [other_head[0] - my_tail[0], other_head[1] - my_tail[1], other_head[2] - my_tail[2]]
                        R_identity = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
                        edges.append({
                            'from': sid_from, 'to': sid_to, 'kind': 'traj_tail_head',
                            'weight': max(0.1, distance_threshold / (d + 1e-6)),
                            'transform_guess': {'t': t, 'R': R_identity}, 'meta': {'distance': d}
                        })
        return edges

    @activity.defn
    def register_pair(self, source_path: str, target_path: str, edge: dict[str, Any], params: dict[str, Any]) -> dict[
        str, Any]:
        activity.heartbeat({"stage": "icp", "edge": f"{edge['from']} -> {edge['to']}"})

        src_pcd = o3d.io.read_point_cloud(source_path)
        tgt_pcd = o3d.io.read_point_cloud(target_path)

        if src_pcd.is_empty() or tgt_pcd.is_empty():
            return {"accepted": False, "reason": "empty_cloud"}

        # --- 1. GLOBAL REGISTRATION (Параллельно и Конфигурируемо) ---
        voxel_global = params.get("global_voxel_m", 1.0)
        radius_feature = voxel_global * 5.0
        max_nn = params.get("fpfh_max_nn", 100)
        search_param = o3d.geometry.KDTreeSearchParamHybrid(radius=radius_feature, max_nn=max_nn)

        # Вычисляем FPFH фичи в два потока (GIL-free C++!)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_src = executor.submit(o3d.pipelines.registration.compute_fpfh_feature, src_pcd, search_param)
            future_tgt = executor.submit(o3d.pipelines.registration.compute_fpfh_feature, tgt_pcd, search_param)

            src_fpfh = future_src.result()
            tgt_fpfh = future_tgt.result()

        # Настраиваем RANSAC (Чисто и читаемо)
        dist_threshold = voxel_global * 1.5
        edge_len_threshold = params.get("ransac_edge_length_threshold", 0.9)
        max_iters = params.get("ransac_max_iterations", 4000000)

        # Определяем объекты проверок и критериев отдельно
        estimator = o3d.pipelines.registration.TransformationEstimationPointToPoint(False)
        checkers = [
            o3d.pipelines.registration.CorrespondenceCheckerBasedOnEdgeLength(edge_len_threshold),
            o3d.pipelines.registration.CorrespondenceCheckerBasedOnDistance(dist_threshold)
        ]
        criteria = o3d.pipelines.registration.RANSACConvergenceCriteria(max_iteration=max_iters, confidence=0.999)

        # Запускаем сам RANSAC
        reg_global = o3d.pipelines.registration.registration_ransac_based_on_feature_matching(
            source=src_pcd,
            target=tgt_pcd,
            source_feature=src_fpfh,
            target_feature=tgt_fpfh,
            mutual_filter=True,
            max_correspondence_distance=dist_threshold,
            estimation_method=estimator,
            ransac_n=4,
            checkers=checkers,
            criteria=criteria
        )

        # --- 2. CASCADE ICP (Конфигурируемо) ---
        T_icp = reg_global.transformation
        fitness_final = 0.0

        voxel_sizes = params.get("cascade_voxels_m", [1.0, 0.3, 0.1])
        multipliers = params.get("cascade_max_corr_multipliers", [3.0, 2.0, 1.5])
        icp_max_iters = params.get("icp_max_iterations", 50)

        icp_estimator = o3d.pipelines.registration.TransformationEstimationPointToPlane()
        icp_criteria = o3d.pipelines.registration.ICPConvergenceCriteria(max_iteration=icp_max_iters)

        for voxel, max_corr_mul in zip(voxel_sizes, multipliers):
            max_corr = max(voxel * max_corr_mul, 0.1)
            reg_icp = o3d.pipelines.registration.registration_icp(
                source=src_pcd,
                target=tgt_pcd,
                max_correspondence_distance=max_corr,
                init=T_icp,
                estimation_method=icp_estimator,
                criteria=icp_criteria
            )
            T_icp = reg_icp.transformation
            fitness_final = float(reg_icp.fitness)

        # --- 3. ИТОГ ---
        min_fitness = params.get("min_fitness", 0.2)
        if fitness_final < min_fitness:
            return {"accepted": False, "reason": "low_fitness", "fitness": fitness_final}

        edge["transform_guess"] = {"matrix": T_icp.tolist()}
        edge["weight"] = max(0.1, fitness_final)
        edge["meta"]["fitness"] = fitness_final
        return {"accepted": True, "edge": edge}

    @activity.defn
    def solve_pose_graph(self, graph: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        activity.heartbeat({"stage": "solve_pose_graph"})

        scan_ids: list[str] = graph.get("scan_ids", [])
        edges: list[dict] = graph.get("edges", [])

        if not scan_ids:
            return {"poses": {}, "diagnostics": {"scans_total": 0}}

        # 1. Строим дерево (Начальное приближение для PGO)
        # Назначаем первый скан корнем (он стоит в начале координат, матрица = Identity)
        root = scan_ids[0]
        poses: dict[str, np.ndarray] = {root: np.eye(4)}

        # Строим списки смежности для BFS обхода
        adj: dict[str, list[tuple[str, np.ndarray]]] = {sid: [] for sid in scan_ids}
        for e in edges:
            a, b = e.get("from"), e.get("to")
            tg_matrix = e.get("transform_guess", {}).get("matrix")
            if not (a in adj and b in adj) or not tg_matrix:
                continue
            T = np.array(tg_matrix, dtype=float)
            adj[a].append((b, T))
            # Нам нужно и обратное ребро для связности графа
            adj[b].append((a, np.linalg.inv(T)))

        # Обход в ширину (BFS): вычисляем грубые глобальные координаты для всех сканов
        queue = [root]
        while queue:
            cur = queue.pop(0)
            cur_pose = poses[cur]
            for nxt, T in adj.get(cur, []):
                if nxt in poses:
                    continue
                # Глобальная матрица = Матрица родителя * Относительная матрица
                poses[nxt] = cur_pose @ T
                queue.append(nxt)

        # Если какие-то сканы вообще не соединились ни с чем (острова), оставляем их на месте
        for sid in scan_ids:
            poses.setdefault(sid, np.eye(4))

        # 2. Создаем граф Open3D
        pg = o3d.pipelines.registration.PoseGraph()
        index_by_scan = {sid: i for i, sid in enumerate(scan_ids)}

        # Добавляем узлы (Nodes) с нашими грубыми глобальными позами
        for sid in scan_ids:
            pg.nodes.append(o3d.pipelines.registration.PoseGraphNode(poses[sid]))

        # Добавляем ребра (Edges) - это пружинки, которые будут тянуть узлы
        for e in edges:
            a, b = e.get('from'), e.get('to')
            tg_matrix = e.get("transform_guess", {}).get("matrix")
            if a not in index_by_scan or b not in index_by_scan or not tg_matrix:
                continue

            T = np.array(tg_matrix, dtype=float)
            weight = float(e.get("weight", 1.0))

            # Информационная матрица - это уверенность ICP. Чем больше weight (fitness), тем жестче пружина.
            information = np.eye(6) * max(weight, 1e-3)
            pg.edges.append(
                o3d.pipelines.registration.PoseGraphEdge(
                    index_by_scan[a],
                    index_by_scan[b],
                    T,
                    information,
                    uncertain=False,
                )
            )

        # 3. Глобальная Оптимизация (Магия)
        # Параметры из DTO (если не переданы, используем дефолты)
        max_dist = params.get("pgo_max_correspondence_dist", 2.0)
        prune_threshold = params.get("pgo_edge_prune_threshold", 0.25)

        option = o3d.pipelines.registration.GlobalOptimizationOption(
            max_correspondence_distance=max_dist,
            edge_prune_threshold=prune_threshold,
            reference_node=0,
        )
        o3d.pipelines.registration.global_optimization(
            pg,
            o3d.pipelines.registration.GlobalOptimizationLevenbergMarquardt(),
            o3d.pipelines.registration.GlobalOptimizationConvergenceCriteria(),
            option,
        )

        # 4. Собираем результаты
        out_poses: dict[str, dict[str, list[float]]] = {}
        for sid in scan_ids:
            node = pg.nodes[index_by_scan[sid]]
            # Сохраняем итоговую абсолютную 4x4 матрицу (list of lists для JSON)
            out_poses[sid] = {"matrix": node.pose.tolist()}

        diagnostics = {
            "root_scan": root,
            "poses_count": len(out_poses),
            "edges_used": len(pg.edges),
        }

        return {"poses": out_poses, "diagnostics": diagnostics}