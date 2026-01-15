# Cluster pipeline contracts

## Paths

**Merged input**

```
point_cloud/tmp/cluster/<dataset_version_id>/raw/merged.laz
```

**Tiles after splitter**

```
point_cloud/tmp/cluster/<dataset_version_id>/tiles/tile_<n>/raw/tile_<n>.laz
```

**Ground / off-ground (CSF)**

```
point_cloud/tmp/cluster/<dataset_version_id>/tiles/tile_<n>/ground/unclassified/ground_tile_<n>.laz
point_cloud/tmp/cluster/<dataset_version_id>/tiles/tile_<n>/ground/classified/ground_tile_<n>.laz
point_cloud/tmp/cluster/<dataset_version_id>/tiles/tile_<n>/ground/unclassified/offground_tile_<n>.laz
point_cloud/tmp/cluster/<dataset_version_id>/tiles/tile_<n>/ground/classified/offground_tile_<n>.laz
```

**Cropped tiles (buffer removed)**

```
point_cloud/tmp/cluster/<dataset_version_id>/tiles/tile_<n>/cropped/tile_<n>.laz
```

**Merged output**

```
point_cloud/tmp/cluster/<dataset_version_id>/derived/merged_classified_<dataset_version_id>.laz
```

## Activities

### `download_dataset_version_artifact`

**Input**

- `dataset_version_id: str`
- `kind: str` (expected `derived.merged_point_cloud`)
- `schema_version: str`
- `dst_dir: str` (expected `point_cloud/tmp/cluster/<dataset_version_id>/raw`)

**Output**

```json
{
  "local_path": "point_cloud/tmp/cluster/<dataset_version_id>/raw/merged.laz",
  "bucket": "<s3_bucket>",
  "key": "<s3_key>",
  "etag": "<etag>",
  "size_bytes": 123
}
```

### `extract_scale_offset`

**Input**

- `point_cloud_file: str`

**Output**

```json
{
  "scale": [sx, sy, sz],
  "offset": [ox, oy, oz],
  "metadata": { "...": "..." }
}
```

### `split_into_tiles`

**Input**

```json
{
  "input_file": ".../raw/merged.laz",
  "output_dir": ".../tiles",
  "tile_size": 50.0,
  "buffer": 3.0
}
```

**Output**

```json
{
  "tiles": [
    ".../tiles/tile_<n>/raw/tile_<n>.laz"
  ]
}
```

### `split_ground_offground`

**Input**

```json
{
  "tile_path": ".../tiles/tile_<n>/raw/tile_<n>.laz",
  "output_dir": ".../tiles/tile_<n>/ground",
  "csf_params": { "...": "..." }
}
```

**Output**

```json
{
  "ground_unclassified": ".../ground/unclassified/ground_tile_<n>.laz",
  "ground_classified": ".../ground/classified/ground_tile_<n>.laz",
  "offground_unclassified": ".../ground/unclassified/offground_tile_<n>.laz",
  "offground_classified": ".../ground/classified/offground_tile_<n>.laz"
}
```

### `cluster_tile`

**Input**

```json
{
  "input_file": ".../ground/classified/ground_tile_<n>.laz",
  "output_file": ".../ground/classified/ground_tile_<n>.laz",
  "params": { "...": "..." }
}
```

**Output**

```json
{
  "classified_file": ".../ground/classified/ground_tile_<n>.laz"
}
```

### `crop_buffer`

**Input**

```json
{
  "input_file": ".../ground/classified/merged_tile_<n>.laz",
  "output_file": ".../tiles/tile_<n>/cropped/tile_<n>.laz",
  "buffer": 3.0,
  "scale": [sx, sy, sz],
  "offset": [ox, oy, oz]
}
```

**Output**

```json
{
  "cropped_tile": ".../tiles/tile_<n>/cropped/tile_<n>.laz"
}
```

### `merge_tiles`

**Input**

```json
{
  "tiles": [".../tiles/tile_<n>/cropped/tile_<n>.laz"],
  "output_file": ".../derived/merged_classified_<dataset_version_id>.laz",
  "scale": [sx, sy, sz],
  "offset": [ox, oy, oz]
}
```

**Output**

```json
{
  "merged_file": ".../derived/merged_classified_<dataset_version_id>.laz"
}
```

## Workflow outline

1. Download merged cloud into `raw/`.
2. Extract scale/offset.
3. Split into tiles with buffer.
4. For each tile:
   - CSF ground/off-ground.
   - Cluster each classified tile (heuristics).
   - Merge ground+offground classified.
   - Crop buffer and write into `cropped/`.
5. Merge cropped tiles into final LAZ in `derived/`.
