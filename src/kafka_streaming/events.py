"""Turns raw Wikimedia recentchange JSON into records matching schemas/recentchange.avsc."""

from datetime import datetime
from pathlib import Path
from typing import Any

SCHEMA_PATH = Path(__file__).parents[2] / "schemas" / "recentchange.avsc"

REQUIRED = ("meta", "wiki", "server_name", "type", "namespace", "title", "user", "bot")


def load_schema() -> str:
    return SCHEMA_PATH.read_text()


def to_record(raw: dict[str, Any]) -> dict[str, Any]:
    """Map a raw event to the Avro record shape. Raises ValueError on missing required fields."""
    missing = [f for f in REQUIRED if f not in raw]
    if missing:
        raise ValueError(f"missing fields: {missing}")
    meta = raw["meta"]
    if "id" not in meta or "dt" not in meta:
        raise ValueError("missing meta.id or meta.dt")

    length = raw.get("length") or {}
    return {
        "event_id": meta["id"],
        "event_time": _parse_dt(meta["dt"]),
        "wiki": raw["wiki"],
        "server_name": raw["server_name"],
        "type": raw["type"],
        "namespace": int(raw["namespace"]),
        "title": raw["title"],
        "user": raw["user"],
        "bot": bool(raw["bot"]),
        "minor": raw.get("minor"),
        "comment": raw.get("comment"),
        "length_old": length.get("old"),
        "length_new": length.get("new"),
    }


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
