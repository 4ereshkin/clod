from __future__ import annotations

import asyncio
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Iterable
from pathlib import Path

import json
import pdal

from temporalio import activity
from temporalio.exceptions import ApplicationError

from lidar_app.app.artifact_service import store_artifact
from lidar_app.app.config import settings
from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import scan_prefix, S3Store


@activity.defn
async def point_cloud_meta(
        point_cloud_file: str,
        geojson_dst: str) -> Dict[str, Any]:

    def _run():
        output_path = Path(geojson_dst)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        pipeline_json = [
            {
                "type": "readers.las",
             "filename": fr"{point_cloud_file}"
            },
            {
                "type": "filters.info"
            },
            {
                "type": "filters.stats"
            },
            {
                "type": "filters.hexbin",
             "density": str(output_path),
            },
        ]
        try:
            pipeline_spec = json.dumps(pipeline_json)
        except Exception as exc:
            raise ApplicationError(f"Failed to serialize PDAL pipeline:: \n{exc}")

        pipeline = pdal.Pipeline(pipeline_spec  )
        try:
            pipeline.execute()
        except Exception as exc:
            raise ApplicationError(f"Failed to execute PDAL pipeline: \n{exc}")

        raw_metadata = pipeline.metadata['metadata']

        try:
            metadata = json.loads(raw_metadata) if isinstance(raw_metadata, str) else raw_metadata
        except Exception as exc:
            raise ApplicationError(f"Failed to decode PDAL metadata: \n{exc}")

        return metadata.get('metadata', metadata)

    activity.heartbeat(
        {'stage':'point_cloud_meta',
         'file':point_cloud_file,
         '.geojson from filters.hexbin':geojson_dst})
    return await asyncio.to_thread(_run)


@activity.defn
async def read_cloud_hexbin(geojson_dst: str,) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        geojson_path = Path(geojson_dst)
        if not geojson_path.exists():
            raise ApplicationError(f"Hexbin GeoJSON not found: {geojson_path}")

        try:
            return json.loads(geojson_path.read_text(encoding='utf-8'))
        except Exception as exc:
            raise ApplicationError(f"Failed to read hexbin GeoJSON: {geojson_path}")

    activity.heartbeat({
        "stage":"read_cloud_hexbin",
        "geojson_dst":geojson_dst,
    })
    return await asyncio.to_thread(_run)


def _percentile(values: Iterable[float], percentile: float) -> float:
    if not 0 <= percentile <= 1:
        raise ValueError("percentile must be between 0 and 1")
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = int(round((len(ordered) - 1) * percentile))
    return float(ordered[index])

def _extract_threshold(geojson: Dict[str, Any]) -> Optional[float]:
    props = geojson.get("properties", {})
    if isinstance(props, dict) and "threshold" in props:
        try:
            return float(props["threshold"])
        except (TypeError, ValueError):
            return None
    features = geojson.get("features", [])
    if features:
        feature_props = features[0].get("properties", {})
        if isinstance(feature_props, dict) and "threshold" in feature_props:
            try:
                return float(feature_props["threshold"])
            except (TypeError, ValueError):
                return None
    return None


@activity.defn
async def extract_hexbin_fields(geojson_text: Dict[str, Any]) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        features = geojson_text.get('features', [])
        if not isinstance(features, list) or not features:
            raise ApplicationError('Hexbin GeoJSON has no features to aggregate')

        counts: list[float] = []
        for feature in features:
            props = feature.get('properties', {})
            if not isinstance(props, dict):
                continue
            count = props.get('count')
            if count is None:
                continue
            try:
                counts.append(float(count))
            except (TypeError, ValueError):
                continue

        if not counts:
            raise ApplicationError('Hexbin GeoJSON has no numeric count values')

        total = float(len(counts))
        count_sum = float(sum(counts))
        count_min = float(min(counts))
        count_max = float(max(counts))
        count_mean = count_sum / total

        threshold = _extract_threshold(geojson_text)
        below = None
        above = None
        if threshold is not None:
            below = sum(1 for value in counts if value < threshold)
            above = sum(1 for value in counts if value > threshold)

        return {
            "cells_total": int(total),
            "count_sum": count_sum,
            "count_min": count_min,
            "count_max": count_max,
            "count_mean": count_mean,
            "count_p50": _percentile(counts, 0.50),
            "count_p90": _percentile(counts, 0.90),
            "count_p99": _percentile(counts, 0.99),
            "cells_below_threshold": below,
            "cells_above_threshold": above,
            "threshold": threshold,
        }
    activity.heartbeat({
        'stage':'extract_hexbin_fields',
    })
    return await asyncio.to_thread(_run)


@activity.defn
async def upload_hexbin(
        scan_id: str,
        geojson_path: str,
        kind: str = 'derived.profiling_hexbin'
) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        repo = Repo()
        scan = repo.get_scan(scan_id)

        local_path = Path(geojson_path)
        if not local_path.exists():
            raise ApplicationError(f"Hexbin GeoJSON not found: {local_path}")

        prefix = scan_prefix(scan.company_id, scan.dataset_version_id, scan_id)
        filename = local_path.name
        key = f"{prefix}/derived/v{scan.schema_version}/profiling/hexbin/{filename}"

        s3 = S3Store(
            settings.s3_endpoint,
            settings.s3_access_key,
            settings.s3_secret_key,
            settings.s3_region,
        )

        result = store_artifact(
            repo=repo,
            s3=s3,
            company_id=scan.company_id,
            scan_id=scan_id,
            kind=kind,
            schema_version=scan.schema_version,
            bucket=settings.s3_bucket,
            key=key,
            local_file_path=str(local_path),
            content_type="application/geo+json",
            status="READY",
            meta={"filename": filename},
            upsert=True,
        )

        return {
            "bucket": result["bucket"],
            "key": result["key"],
            "etag": result["etag"],
            "size_bytes": result["size_bytes"],
            "kind": result["kind"],
        }

    activity.heartbeat({
        "stage": "upload_hexbin",
        "scan_id": scan_id,
        "geojson_path": geojson_path,
    })

    return await asyncio.to_thread(_run)


@activity.defn
async def aggregate_metadata(clouds_meta: List[Dict[str, Any]]) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        pass

    pass