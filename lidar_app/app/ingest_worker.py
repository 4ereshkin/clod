from __future__ import annotations

import argparse
import time
import traceback

from lidar_app.app.repo import Repo


def validate_inputs(repo: Repo, scan_id: str) -> None:
    raw = repo.list_raw_artifacts(scan_id)

    cloud = next((a for a in raw if a.kind == "raw.point_cloud"), None)
    if cloud is None:
        raise RuntimeError("raw.point_cloud is missing (run upload-raw)")

    # path/cp optional – ок


def process_run(repo: Repo, run_id: int) -> None:
    run = repo.get_ingest_run(run_id)

    # 1) проверяем наличие raw
    validate_inputs(repo, run.scan_id)

    # 2) TODO: тут будет настоящая ingest-логика:
    #    - скачать/прочитать raw из S3
    #    - прогнать пайплайн
    #    - загрузить derived в S3
    #    - repo.register_artifact(... schema_version=run.schema_version)
    #
    # Пока сделаем “пустой ingest” = только валидация.

    return


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--once", action="store_true", help="Process one loop and exit")
    p.add_argument("--sleep", type=float, default=2.0, help="Idle sleep seconds")
    p.add_argument("--limit", type=int, default=10, help="How many queued runs to peek per loop")
    p.add_argument("--schema-version", default=None)
    p.add_argument("--company", default=None)
    args = p.parse_args()

    repo = Repo()

    while True:
        runs = repo.list_queued_ingest_runs(
            schema_version=args.schema_version,
            company_id=args.company,
            limit=args.limit,
        )

        if not runs:
            if args.once:
                print("No queued ingest runs.")
                return
            time.sleep(args.sleep)
            continue

        for r in runs:
            if not repo.claim_ingest_run(int(r.id)):
                continue  # кто-то другой успел

            print(f"CLAIMED run_id={r.id} scan={r.scan_id} company={r.company_id} schema={r.schema_version}")

            try:
                repo.set_ingest_run_status(run_id=int(r.id), status="RUNNING")
                process_run(repo, int(r.id))
            except Exception as e:
                err = {
                    "type": e.__class__.__name__,
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                }
                repo.set_ingest_run_status(run_id=int(r.id), status="FAILED", error=err, set_finished_at=True)
                print(f"FAILED run_id={r.id}: {e}")
                continue

            repo.set_ingest_run_status(run_id=int(r.id), status="SUCCEEDED", error={}, set_finished_at=True)
            print(f"SUCCEEDED run_id={r.id}")

        if args.once:
            return


if __name__ == "__main__":
    main()