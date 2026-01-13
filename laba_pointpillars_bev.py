# -*- coding: utf-8 -*-
import os
import numpy as np
import laspy
import open3d as o3d

# Попробуем ускорить перенос цветов/классов на исходные точки через SciPy, если он установлен
try:
    from scipy.spatial import cKDTree  # type: ignore
    HAS_SCIPY = True
except Exception:
    HAS_SCIPY = False


# -----------------------------
# НАСТРОЙКИ
# -----------------------------
FILE_PATH = r"C:\Users\ceres\Desktop\новый год\merged.laz"
OUT_PATH = r"C:\Users\ceres\Desktop\новый год\merged_colored_classified.laz"  # куда сохранить

# Ускорение обработки (на каком облаке считаем сегментацию/кластеризацию)
VOXEL_SIZE = 0.10  # м (0.05–0.30)

# Шумоподавление на "обработочном" облаке
NB_NEIGHBORS = 20
STD_RATIO = 2.0

# Дорога (RANSAC)
PLANE_DIST_THRESHOLD = 0.20
RANSAC_N = 3
RANSAC_ITERS = 1000

# Нормали на дороге (для бордюров/перепадов)
NORMAL_RADIUS = 0.5
NORMAL_MAX_NN = 30
VERTICAL_Z_THRESHOLD = 0.85  # |nz| < threshold => резко наклонено (бордюр/перепад)

# Объекты (DBSCAN)
DBSCAN_EPS = 0.6
DBSCAN_MIN_POINTS = 20
MIN_CLUSTER_SIZE = 30
TALL_OBJECT_HEIGHT = 2.0

# Цвета (0..1)
COLOR_ROAD = np.array([0.2, 0.2, 0.2], dtype=np.float32)   # асфальт
COLOR_CURB = np.array([1.0, 0.0, 0.0], dtype=np.float32)   # бордюры/перепады
COLOR_TALL = np.array([0.0, 1.0, 0.0], dtype=np.float32)   # высокие объекты
COLOR_LOW  = np.array([0.0, 0.6, 1.0], dtype=np.float32)   # низкие объекты
COLOR_NOISE = np.array([0.5, 0.5, 0.5], dtype=np.float32)  # шум DBSCAN / неопределено

# Классы LAS (ASPRS). Можно поменять под твою задачу.
CLASS_UNCLASSIFIED = np.uint8(1)
CLASS_GROUND = np.uint8(2)
CLASS_LOW_VEG = np.uint8(3)
CLASS_BUILDING = np.uint8(6)
CLASS_NOISE = np.uint8(7)
CLASS_KEYPOINT = np.uint8(8)  # удобно для бордюров/ребер; при желании можно тоже в GROUND


# -----------------------------
# ВСПОМОГАТЕЛЬНОЕ
# -----------------------------
def ensure_rgb_point_format(las: laspy.LasData) -> int:
    """
    Возвращает point_format id, который содержит RGB.
    Если текущий формат уже содержит red/green/blue — возвращает его же.
    Иначе пытается "апгрейдить" на ближайший аналог с RGB.
    """
    dims = set(las.point_format.dimension_names)
    if {"red", "green", "blue"}.issubset(dims):
        return int(las.header.point_format.id)

    # Маппинг форматов без RGB -> с RGB (по LAS спецификации)
    upgrade_map = {0: 2, 1: 3, 4: 5, 6: 7, 9: 10}
    cur = int(las.header.point_format.id)
    return upgrade_map.get(cur, 3)  # если неизвестно — 3 (часто самый совместимый)


def build_processing_colors_and_classes(points_proc: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Делает обработку на downsample облаке и возвращает:
      - colors_proc (M,3) float32 in 0..1
      - class_proc  (M,)  uint8 LAS classification
    Важно: внутри делается remove_statistical_outlier, и возвращаемые массивы соответствуют
    точкам *после* outlier removal.
    """
    pcd = o3d.geometry.PointCloud(o3d.utility.Vector3dVector(points_proc))

    # 1) Удаление статистического шума (на обработочном облаке)
    pcd, _ = pcd.remove_statistical_outlier(nb_neighbors=NB_NEIGHBORS, std_ratio=STD_RATIO)

    pts = np.asarray(pcd.points)
    m = pts.shape[0]

    colors = np.tile(COLOR_NOISE, (m, 1)).astype(np.float32)
    classes = np.full((m,), CLASS_UNCLASSIFIED, dtype=np.uint8)

    if m < 50:
        return colors, classes

    # 2) Дорога RANSAC
    _, inliers = pcd.segment_plane(
        distance_threshold=PLANE_DIST_THRESHOLD,
        ransac_n=RANSAC_N,
        num_iterations=RANSAC_ITERS
    )

    inliers = np.asarray(inliers, dtype=np.int64)
    mask_road = np.zeros(m, dtype=bool)
    mask_road[inliers] = True

    # базовый цвет/класс дороги
    colors[mask_road] = COLOR_ROAD
    classes[mask_road] = CLASS_GROUND

    road_pcd = pcd.select_by_index(inliers)
    objects_pcd = pcd.select_by_index(inliers, invert=True)

    # 3) Бордюры/перепады на дороге по нормалям
    if len(road_pcd.points) > 50:
        road_pcd.estimate_normals(
            search_param=o3d.geometry.KDTreeSearchParamHybrid(radius=NORMAL_RADIUS, max_nn=NORMAL_MAX_NN)
        )
        normals = np.asarray(road_pcd.normals)
        curb_mask_local = (np.abs(normals[:, 2]) < VERTICAL_Z_THRESHOLD)

        road_indices = inliers
        curb_indices = road_indices[curb_mask_local]

        colors[curb_indices] = COLOR_CURB
        classes[curb_indices] = CLASS_KEYPOINT  # или CLASS_GROUND, если хочешь всё как ground

    # 4) DBSCAN для объектов
    if len(objects_pcd.points) > 0:
        labels = np.array(objects_pcd.cluster_dbscan(
            eps=DBSCAN_EPS,
            min_points=DBSCAN_MIN_POINTS,
            print_progress=True
        ))

        # индексы объектов в массиве "после outlier"
        obj_global_idx = np.where(~mask_road)[0]

        # DBSCAN: -1 это шум
        noise_local = np.where(labels == -1)[0]
        if noise_local.size > 0:
            classes[obj_global_idx[noise_local]] = CLASS_NOISE
            # цвет остаётся COLOR_NOISE

        valid = labels[labels >= 0]
        if valid.size > 0:
            max_label = int(valid.max())
            for i in range(max_label + 1):
                local_idx = np.where(labels == i)[0]
                if local_idx.size < MIN_CLUSTER_SIZE:
                    continue

                cluster = objects_pcd.select_by_index(local_idx)
                bbox = cluster.get_axis_aligned_bounding_box()
                height = float(bbox.get_extent()[2])

                global_idx = obj_global_idx[local_idx]
                if height > TALL_OBJECT_HEIGHT:
                    colors[global_idx] = COLOR_TALL
                    classes[global_idx] = CLASS_BUILDING
                else:
                    colors[global_idx] = COLOR_LOW
                    classes[global_idx] = CLASS_LOW_VEG

    return colors, classes


def transfer_nn_values(points_full: np.ndarray, points_proc: np.ndarray, values_proc: np.ndarray) -> np.ndarray:
    """
    Назначает каждому исходному point_full значение values_proc ближайшей точки из processing-облака.
    values_proc может быть (M,3) или (M,)
    """
    if points_proc.shape[0] == 0:
        if values_proc.ndim == 2:
            return np.tile(COLOR_NOISE, (points_full.shape[0], 1)).astype(values_proc.dtype)
        return np.full((points_full.shape[0],), CLASS_UNCLASSIFIED, dtype=values_proc.dtype)

    if HAS_SCIPY:
        tree = cKDTree(points_proc)
        _, nn = tree.query(points_full, k=1, workers=-1)
        return values_proc[nn]

    # fallback: Open3D KDTreeFlann (медленнее на больших облаках)
    pcd_proc = o3d.geometry.PointCloud(o3d.utility.Vector3dVector(points_proc))
    kdtree = o3d.geometry.KDTreeFlann(pcd_proc)

    if values_proc.ndim == 2:
        out = np.empty((points_full.shape[0], values_proc.shape[1]), dtype=values_proc.dtype)
    else:
        out = np.empty((points_full.shape[0],), dtype=values_proc.dtype)

    for i, p in enumerate(points_full):
        _, idx, _ = kdtree.search_knn_vector_3d(p, 1)
        out[i] = values_proc[idx[0]]
        if (i + 1) % 500000 == 0:
            print(f"Перенос NN значений: {i+1:,}/{points_full.shape[0]:,}")

    return out


def main():
    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError(f"Файл не найден: {FILE_PATH}")

    las = laspy.read(FILE_PATH)

    # Исходные точки (N,3)
    points_full = np.vstack((las.x, las.y, las.z)).T.astype(np.float64)
    print(f"Исходных точек: {points_full.shape[0]:,}")

    # Облако для обработки (downsample)
    pcd_full = o3d.geometry.PointCloud(o3d.utility.Vector3dVector(points_full))
    if VOXEL_SIZE and VOXEL_SIZE > 0:
        pcd_proc = pcd_full.voxel_down_sample(voxel_size=VOXEL_SIZE)
    else:
        pcd_proc = pcd_full

    points_proc = np.asarray(pcd_proc.points).astype(np.float64)
    print(f"Точек для обработки (после voxel): {points_proc.shape[0]:,}")

    # 1) Обработка на points_proc -> цвета/классы для "после outlier"
    colors_proc, class_proc = build_processing_colors_and_classes(points_proc)

    # Чтобы перенос был корректным, нужно взять те же точки "после outlier".
    # Повторяем outlier removal теми же параметрами, чтобы получить points_proc2.
    pcd_proc2 = o3d.geometry.PointCloud(o3d.utility.Vector3dVector(points_proc))
    pcd_proc2, _ = pcd_proc2.remove_statistical_outlier(nb_neighbors=NB_NEIGHBORS, std_ratio=STD_RATIO)
    points_proc2 = np.asarray(pcd_proc2.points).astype(np.float64)

    if points_proc2.shape[0] != colors_proc.shape[0] or points_proc2.shape[0] != class_proc.shape[0]:
        raise RuntimeError("Несовпадение размеров processing точек и массива цветов/классов. Проверьте параметры/код.")

    # 2) Перенос цветов/классов на исходные точки
    print("Переносим цвета и classification на исходный LAS...")
    colors_full = transfer_nn_values(points_full, points_proc2, colors_proc).astype(np.float32)
    class_full = transfer_nn_values(points_full, points_proc2, class_proc).astype(np.uint8)

    # 3) Подготовка LAS с RGB (classification есть в большинстве форматов и так)
    new_pf = ensure_rgb_point_format(las)
    new_header = laspy.LasHeader(point_format=new_pf, version=las.header.version)

    # переносим масштаб/смещения (важно!)
    new_header.scales = las.header.scales
    new_header.offsets = las.header.offsets

    new_las = laspy.LasData(new_header)

    # копируем общие измерения (что совпадает по именам)
    src_dims = list(las.point_format.dimension_names)
    dst_dims = set(new_las.point_format.dimension_names)

    for d in src_dims:
        if d in dst_dims and d not in ("red", "green", "blue"):
            new_las[d] = las[d]

    # гарантированно копируем XYZ
    new_las.x = las.x
    new_las.y = las.y
    new_las.z = las.z

    # 4) Запись RGB (LAS хранит RGB как uint16)
    rgb16 = np.clip(colors_full * 65535.0, 0, 65535).astype(np.uint16)
    new_las.red = rgb16[:, 0]
    new_las.green = rgb16[:, 1]
    new_las.blue = rgb16[:, 2]

    # 5) Запись classification
    # В LAS поле называется classification (uint8)
    if "classification" in dst_dims:
        new_las.classification = class_full
    else:
        # крайне редкий случай: формат без classification
        raise RuntimeError(
            f"В выбранном point_format={new_pf} нет поля 'classification'. "
            f"Доступные dims: {sorted(dst_dims)}"
        )

    # обновим bounds (на всякий случай)
    new_las.header.mins = np.min(points_full, axis=0)
    new_las.header.maxs = np.max(points_full, axis=0)

    # 6) Сохранение
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    new_las.write(OUT_PATH)
    print(f"Готово: {OUT_PATH}")
    print("Цвета записаны в red/green/blue (uint16).")
    print("Классы записаны в classification (uint8).")


if __name__ == "__main__":
    main()
