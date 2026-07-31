"""ECHO CLI entrypoint."""

from __future__ import annotations

import argparse
import json
import sys

from echo.db import get_engine, get_session, init_db
from echo.models import ConversationIn, JobIn, MessageIn
from echo.service import ContinuityService


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(prog="echo", description="ECHO Continuity CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_health = sub.add_parser("health", help="Runtime health + stats")
    p_ingest = sub.add_parser("ingest", help="Ingest a simple conversation")
    p_ingest.add_argument("--title", required=True)
    p_ingest.add_argument("--content", required=True, help="role: content (or plain text)")
    p_search = sub.add_parser("search", help="Search conversations")
    p_search.add_argument("query", nargs="?", default="")
    p_job = sub.add_parser("job", help="Enqueue + run a job")
    p_job.add_argument("--type", required=True)
    p_job.add_argument("--payload", default="{}")
    p_verify = sub.add_parser("verify", help="Run verification suite")

    args = parser.parse_args(argv)
    engine = init_db()

    with get_session(engine) as session:
        svc = ContinuityService(session)

        if args.cmd == "health":
            print(json.dumps(svc.health(), indent=2))
            return 0

        if args.cmd == "ingest":
            role, _, content = args.content.partition(":")
            if not content:
                role, content = "user", role
            body = ConversationIn(
                title=args.title,
                messages=[MessageIn(role=role.strip(), content=content.strip())],
            )
            out = svc.ingest_conversation(body)
            print(json.dumps(out.model_dump(mode="json"), indent=2))
            return 0

        if args.cmd == "search":
            results = svc.search(q=args.query)
            print(json.dumps([r.model_dump(mode="json") for r in results], indent=2))
            return 0

        if args.cmd == "job":
            payload = json.loads(args.payload)
            job = svc.enqueue_job(JobIn(job_type=args.type, payload=payload))
            result = svc.run_job(job.id)
            print(json.dumps(result.model_dump(mode="json"), indent=2))
            return 0

        if args.cmd == "verify":
            # lightweight self-check
            h = svc.health()
            assert h["status"] == "ok"
            body = ConversationIn(
                title="verify-seed",
                messages=[MessageIn(role="system", content="verification pulse")],
            )
            conv = svc.ingest_conversation(body)
            assert conv.content_hash
            job = svc.enqueue_job(JobIn(job_type="echo.ping", payload={"verify": True}))
            ran = svc.run_job(job.id)
            assert ran.status == "succeeded"
            print("VERIFIED")
            print(json.dumps({"health": h, "conversation_id": conv.id, "job_id": ran.id}, indent=2))
            return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
