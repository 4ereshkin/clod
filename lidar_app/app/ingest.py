import json
from datetime import datetime, timezone
from pathlib import Path
import sys

# add project root to PYTHONPATH
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from .models import IngestRun, Scan, Artifact


def build_ingest_manifest(*, run: IngestRun, scan: Scan, raw_arts: list[Artifact]) -> dict:
    def a_to_dict(a: Artifact) -> dict:
        return {
            'kind': a.kind,
            'bucket': a.s3_bucket,
            'key': a.s3_key,
            'etag': a.etag,
            'size_bytes': a.size_bytes,
            'status': a.status,
            'meta': a.meta or {},
        }

    return {
        'run_id': int(run.id),
        'company_id': run.company_id,
        'scan_id': run.scan_id,
        'schema_version': run.schema_version,
        'input_fingerprint': run.input_fingerprint,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'scan': {
            'id': scan.id,
            'dataset_id': scan.dataset_id,
            'crs_id': scan.crs_id,
            'status': scan.status,
            'schema_version': scan.schema_version,
            'meta': scan.meta or {},
        },
        'raw_artifacts': [a_to_dict(a) for a in raw_arts],
    }