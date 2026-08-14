from __future__ import annotations

import argparse
from datetime import datetime

from millrace.orchestration.runtime import (
    RunDescriptor,
    build_candidate,
    cleanup_temporary_files,
    emit_metrics,
    promote_candidate,
    run_spark,
    stable_run_id,
    test_candidate,
    validate_candidate,
    wait_for_readiness,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m millrace.orchestration")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("backfill-spark", "backfill-run"):
        command = subparsers.add_parser(name)
        command.add_argument("--batch-id", required=True, type=int)
        command.add_argument("--interval-start", required=True)
        command.add_argument("--interval-end", required=True)
    subparsers.choices["backfill-run"].add_argument("--promote", action="store_true")
    return parser


def _descriptor(arguments: argparse.Namespace) -> RunDescriptor:
    interval_start = datetime.fromisoformat(str(arguments.interval_start).replace("Z", "+00:00"))
    interval_end = datetime.fromisoformat(str(arguments.interval_end).replace("Z", "+00:00"))
    batch_id = int(arguments.batch_id)
    return {
        "run_id": stable_run_id(interval_start, interval_end, batch_id),
        "batch_id": batch_id,
        "interval_start": interval_start.isoformat(),
        "interval_end": interval_end.isoformat(),
    }


def main() -> int:
    arguments = _parser().parse_args()
    descriptor = _descriptor(arguments)
    wait_for_readiness()
    run_spark(descriptor, backfill=True)
    if arguments.command == "backfill-run":
        try:
            build_candidate(descriptor)
            report_path = validate_candidate(descriptor)
            test_candidate(descriptor)
            if bool(arguments.promote):
                promote_candidate(descriptor, report_path)
                emit_metrics(descriptor)
        finally:
            cleanup_temporary_files(descriptor)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
