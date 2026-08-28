"""ECHO CLI entrypoint."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

from echo.db import get_session, init_db
from echo.models import ConversationIn, JobIn, MessageIn
from echo.service import ContinuityService


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(prog="echo", description="ECHO Continuity CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("health", help="Runtime health + stats")
    ingest = sub.add_parser("ingest", help="Ingest a simple conversation")
    ingest.add_argument("--source", default="manual")
    ingest.add_argument("--external-id", required=True)
    ingest.add_argument("--title", required=True)
    ingest.add_argument(
        "--content", required=True, help="role: content (or plain text)"
    )
    search = sub.add_parser("search", help="Search conversations")
    search.add_argument("query", nargs="?", default="")
    job = sub.add_parser("job", help="Enqueue + run a supported job")
    job.add_argument("--type", required=True)
    job.add_argument("--payload", default="{}")
    job.add_argument("--idempotency-key", required=True)
    sub.add_parser("verify", help="Run isolated verification pulse")

    args = parser.parse_args(argv)
    if args.cmd == "verify":
        with tempfile.TemporaryDirectory(prefix="echo-verify-") as directory:
            return _verify(Path(directory) / "verify.db")

    engine = init_db()
    with get_session(engine) as session:
        service = ContinuityService(session)
        if args.cmd == "health":
            print(json.dumps(service.health(), indent=2))
            return 0
        if args.cmd == "ingest":
            role, separator, content = args.content.partition(":")
            if not separator:
                role, content = "user", role
            out = service.ingest_conversation(
                ConversationIn(
                    source=args.source,
                    external_id=args.external_id,
                    title=args.title,
                    messages=[MessageIn(role=role.strip(), content=content.strip())],
                )
            )
            print(json.dumps(out.model_dump(mode="json"), indent=2))
            return 0
        if args.cmd == "search":
            results = service.search(q=args.query)
            print(
                json.dumps([item.model_dump(mode="json") for item in results], indent=2)
            )
            return 0
        if args.cmd == "job":
            queued = service.enqueue_job(
                JobIn(
                    job_type=args.type,
                    payload=json.loads(args.payload),
                    idempotency_key=args.idempotency_key,
                ),
                actor="echo:cli",
                scope="echo:execute",
            )
            result = service.run_job(queued.id)
            print(json.dumps(result.model_dump(mode="json"), indent=2))
            return 0
    return 1


def _verify(path: Path) -> int:
    engine = init_db(path)
    with get_session(engine) as session:
        service = ContinuityService(session)
        conversation = service.ingest_conversation(
            ConversationIn(
                source="echo.verify",
                external_id="verification-pulse",
                title="verify-seed",
                messages=[MessageIn(role="system", content="verification pulse")],
            )
        )
        integrity = service.verify_integrity(conversation.id)
        job = service.enqueue_job(
            JobIn(
                job_type="echo.ping",
                payload={"verify": True},
                idempotency_key="verify-ping",
            ),
            actor="akos:verify",
            scope="echo:execute",
        )
        ran = service.run_job(job.id)
        assert service.health()["status"] == "ok"
        assert integrity.valid
        assert ran.status == "succeeded"
        print("VERIFIED")
        print(
            json.dumps({"conversation_id": conversation.id, "job_id": ran.id}, indent=2)
        )
        return 0


if __name__ == "__main__":
    sys.exit(main())
