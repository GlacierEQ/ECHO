#!/usr/bin/env python3
"""Export conversations from an old ECHO database, rebuild with governed schema, re-import.

Usage:
    python scripts/rebuild_db.py --old echo_data/echo.db --new echo_data/echo_rebuilt.db

After verifying the rebuilt database, replace the old one:
    mv echo_data/echo.db echo_data/echo.db.bak
    mv echo_data/echo_rebuilt.db echo_data/echo.db
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from echo.db import get_engine, get_session, init_db
from echo.models import ConversationORM, MessageORM, canonical_json, content_sha256, stable_uuid, utcnow


def export_conversations(old_db: Path) -> list[dict]:
    engine = get_engine(old_db)
    records = []
    with get_session(engine) as session:
        convs = session.query(ConversationORM).all()
        for conv in convs:
            msgs = (
                session.query(MessageORM)
                .filter(MessageORM.conversation_id == conv.id)
                .order_by(MessageORM.sequence)
                .all()
            )
            records.append({
                "source": conv.source,
                "external_id": conv.external_id,
                "title": conv.title,
                "participants": conv.participants or [],
                "labels": conv.labels or [],
                "metadata": conv.metadata_ or {},
                "messages": [
                    {"role": m.role, "content": m.content, "metadata": m.metadata_ or {}}
                    for m in msgs
                ],
            })
    print(f"  Exported {len(records)} conversations from {old_db}")
    return records


def import_conversations(records: list[dict], new_db: Path) -> None:
    engine = init_db(new_db)
    imported = 0
    with get_session(engine) as session:
        for rec in records:
            messages = rec["messages"]
            seed = f"{rec['source']}:{rec['external_id']}"
            conv_id = stable_uuid(seed)

            canonical = canonical_json({
                "source": rec["source"],
                "external_id": rec["external_id"],
                "messages": [{"role": m["role"], "content": m["content"]} for m in messages],
            })
            content_hash = content_sha256(canonical)

            summary_parts = [m["content"][:120] for m in messages[:3]]
            summary = " | ".join(summary_parts)

            existing = session.get(ConversationORM, conv_id)
            if existing:
                print(f"  Skipping duplicate: {rec['title']!r}")
                continue

            conv = ConversationORM(
                id=conv_id,
                source=rec["source"],
                external_id=rec["external_id"],
                title=rec["title"],
                participants=rec["participants"],
                labels=rec["labels"],
                metadata_=rec["metadata"],
                summary=summary,
                content_hash=content_hash,
                integrity_status="verified",
                message_count=len(messages),
                created_at=utcnow(),
                updated_at=utcnow(),
            )
            session.add(conv)

            for seq, msg in enumerate(messages):
                msg_id = stable_uuid(f"{conv_id}:{seq}:{msg['role']}")
                session.add(MessageORM(
                    id=msg_id,
                    conversation_id=conv_id,
                    role=msg["role"],
                    content=msg["content"],
                    content_hash=content_sha256(msg["content"]),
                    sequence=seq,
                    metadata_=msg.get("metadata", {}),
                    created_at=utcnow(),
                ))
            imported += 1
    print(f"  Imported {imported} conversations into {new_db}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild ECHO database with governed schema")
    parser.add_argument("--old", default="echo_data/echo.db", help="Path to old v0.1 database")
    parser.add_argument("--new", default="echo_data/echo_rebuilt.db", help="Path for rebuilt database")
    parser.add_argument("--dump", help="Optional: dump exported JSON to this file")
    args = parser.parse_args()

    old_path = Path(args.old)
    new_path = Path(args.new)

    if not old_path.exists():
        print(f"ERROR: Old database not found: {old_path}")
        sys.exit(1)

    if new_path.exists():
        print(f"ERROR: Target database already exists: {new_path} — remove it first")
        sys.exit(1)

    print(f"Exporting from {old_path} ...")
    records = export_conversations(old_path)

    if args.dump:
        dump_path = Path(args.dump)
        dump_path.write_text(json.dumps(records, indent=2, default=str))
        print(f"  Dump written to {dump_path}")

    print(f"Rebuilding into {new_path} ...")
    import_conversations(records, new_path)

    print("Done. Verify with: python scripts/verify.py")
    print(f"Then: mv {old_path} {old_path}.bak && mv {new_path} {old_path}")


if __name__ == "__main__":
    main()
