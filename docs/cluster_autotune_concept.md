# Cluster autotuning concept (placeholder)

This document captures the intended auto-tuning concept for the cluster pipeline.
Implementation is deferred, but the pipeline is structured so that tuning can be
added without redesign.

## Goal

Automatically select heuristic parameters for the tile-level clustering step
(currently based on `laba_pointpillars_bev.py` heuristics) so the output quality
is consistent across datasets while minimizing manual parameter updates.

## Proposed architecture

### Option 1: Auto-tune activity

1. Select a small sample of tiles (uniform or stratified by density/height).
2. Run multiple clustering trials on the sample tiles.
3. Evaluate each trial with a scoring function (see “Metrics”).
4. Return the best parameter set to the main workflow.

### Option 2: Auto-tune child workflow (preferred if long running)

A dedicated child workflow that:

1. Samples tiles.
2. Launches multiple `cluster_tile` activity runs with different parameter sets.
3. Computes scores and aggregates results.
4. Returns the best `cluster_params` to the parent `ClusterPipeline`.

This isolates longer compute tasks, improves observability, and allows
independent retries.

## Parameter search

- Grid search for first iteration (simple, deterministic).
- Follow-up iterations can use random search or Bayesian optimization.

Key parameters to tune:

- `voxel_size`
- `plane_dist_threshold`
- `dbscan_eps`
- `dbscan_min_points`
- `min_cluster_size`
- `tall_object_height`

## Metrics (initial ideas)

- Cluster count stability vs. parameter changes.
- Ground/off-ground consistency metrics (ratio thresholds).
- Spatial compactness / density of clusters.
- Optional human-in-the-loop checkpoints.

## Integration point

The `ClusterPipeline` should optionally call the autotune step before processing
all tiles. If `autotune: false`, the pipeline uses the default heuristic params.
