# app/cli.py
import argparse
from pathlib import Path

from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Store
from lidar_app.app.artifact_service import store_artifact
from legacy_env_vars import settings



def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    cco = sub.add_parser("ensure-company")
    cco.add_argument("--company", required=True)
    cco.add_argument("--name", required=True)

    crs = sub.add_parser("ensure-crs")
    crs.add_argument("--crs", required=True)
    crs.add_argument("--name", required=True)
    crs.add_argument("--zone-degree", type=int, required=True)
    crs.add_argument("--epsg", type=int, required=False)
    crs.add_argument("--units", default="m")
    crs.add_argument("--axis-order", default="x_east,y_north,z_up")

    ds = sub.add_parser("create-dataset")
    ds.add_argument("--dataset", required=True)
    ds.add_argument("--crs", required=True)
    ds.add_argument("--company", required=True)
    ds.add_argument("--name", required=False)

    dv = sub.add_parser("ensure-dataset-version")
    dv.add_argument("--dataset-id", required=True)
    dv.add_argument("--bump", action="store_true")

    sc = sub.add_parser("create-scan")
    sc.add_argument("--company", required=True)
    sc.add_argument("--dataset-version-id", required=True)

    up = sub.add_parser("upload-raw")
    up.add_argument("--company", required=True)
    up.add_argument("--dataset", required=True)
    up.add_argument("--scan", required=True)
    up.add_argument("--cloud", required=True)
    up.add_argument("--path", required=False)
    up.add_argument("--cp", required=False)

    ing = sub.add_parser('ingest-raw')
    ing.add_argument("--company", required=True)
    ing.add_argument("--scan", required=True)
    ing.add_argument("--schema-version", default='1.1.0')
    ing.add_argument("--force", action='store_true')

    args = p.parse_args()

    repo = Repo()

    if args.cmd == "ensure-company":
        repo.ensure_company(args.company, args.name)
        print("OK company:", args.company)
        return

    if args.cmd == "ensure-crs":
        repo.ensure_crs(
            args.crs,
            name=args.name,
            zone_degree=args.zone_degree,
            epsg=args.epsg,
            units=args.units,
            axis_order=args.axis_order,
        )
        print("OK crs:", args.crs)
        return

    if args.cmd == "create-dataset":
        dataset_id = repo.ensure_dataset(company_id=args.company, name=(args.name or args.dataset), crs_id=args.crs)
        print("OK dataset:", (args.name or args.dataset), 'id:', dataset_id)
        return

    if args.cmd == "ensure-dataset-version":
        if args.bump:
            v = repo.bump_dataset_version(args.dataset_id)
        else:
            v = repo.ensure_dataset_version(args.dataset_id)
        print("OK dataset_version:", v.id, "version:", v.version)
        return

    if args.cmd == "create-scan":
        scan_id = repo.create_scan(args.company, args.dataset_version_id)
        print("OK scan:", scan_id)
        return

    if args.cmd == "upload-raw":
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        scan = repo.get_scan(args.scan)
        if scan.company_id != args.company:
            raise RuntimeError("scan does not belong to provided company")

        cloud = store_artifact(
            repo=repo,
            s3=s3,
            company_id=args.company,
            scan_id=args.scan,
            kind="raw.point_cloud",
            local_file_path=args.cloud,
            bucket=settings.s3_bucket,
            dataset_version_id=scan.dataset_version_id,
        )
        print("OK raw cloud:", cloud["key"])

        # path (optional)
        if args.path:
            path = store_artifact(
                repo=repo,
                s3=s3,
                company_id=args.company,
                scan_id=args.scan,
                kind="raw.trajectory",
                local_file_path=args.path,
                bucket=settings.s3_bucket,
                dataset_version_id=scan.dataset_version_id,
                filename=Path(args.path).name,
            )
            print("OK raw path:", path["key"])

        # control point (optional)
        if args.cp:
            cp = store_artifact(
                repo=repo,
                s3=s3,
                company_id=args.company,
                scan_id=args.scan,
                kind="raw.control_point",
                local_file_path=args.cp,
                bucket=settings.s3_bucket,
                dataset_version_id=scan.dataset_version_id,
                filename=Path(args.cp).name,
            )
            print("OK raw control point:", cp["key"])

        print("DONE upload-raw")
        return

    if args.cmd == 'ingest-raw':
        scan = repo.get_scan(args.scan)
        if scan.company_id != args.company:
            raise RuntimeError("scan does not belong to provided company")

        raw_arts = repo.list_raw_artifacts(args.scan)

        cloud = next((a for a in raw_arts if a.kind == "raw.point_cloud"), None)
        if not cloud:
            raise RuntimeError("raw.point_cloud is required before ingest-raw (run upload-raw)")

        fp = repo.compute_fingerprint(scan_id=scan.id)

        existing = repo.find_ingest_run(
            company_id=args.company,
            scan_id=args.scan,
            schema_version=args.schema_version,
            input_fingerprint=fp,
        )
        if existing and not args.force:
            print("SKIP ingest-raw: existing ingest_run:", existing.id, "status:", existing.status)
            return

        run_id = repo.create_ingest_run(
            company_id=args.company,
            scan_id=args.scan,
            schema_version=args.schema_version,
            input_fingerprint=fp,
            status="QUEUED",
        )
        print("OK ingest_run queued:", run_id)
        return

if __name__ == "__main__":
    main()
