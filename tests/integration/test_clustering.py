from pathlib import Path

import pytest


@pytest.mark.integration
def test_hdbscan_clustering(tmp_path):
    input_file = Path(r"D:\1_prod\user_data\НПС Крутое\wg\wgs84_utm_clouds\t100pro_2025-04-28-08-36-08_filter_map.laz")

    if not input_file.exists():
        pytest.skip(f"Input file not found: {input_file}")

    import sys
    sys.path.insert(0, str(Path(__file__).parents[2]))
    from test_clouds_pretrained import run

    run(
        in_path=input_file,
        out_dir=tmp_path,
        voxel=0.3,
        min_cluster_size=15,
        min_samples=5,
        cluster_selection_epsilon=0.5,
    )

    results = list(tmp_path.glob("*.laz"))
    assert len(results) > 0, "No output .laz file produced"
    assert results[0].stat().st_size > 0
