import json
from datetime import UTC, datetime

import pytest
from fastavro import parse_schema, validate

from kafka_streaming.events import load_schema, to_record

RAW = {
    "$schema": "/mediawiki/recentchange/1.0.0",
    "meta": {"id": "a1279648-c229-46cf-b692-0aaa9872a1cd", "dt": "2026-09-29T16:29:24.787Z"},
    "id": 2074228455,
    "type": "edit",
    "namespace": 0,
    "title": "Kafka (software)",
    "comment": "typo",
    "timestamp": 1790699363,
    "user": "SomeEditor",
    "bot": False,
    "minor": True,
    "length": {"old": 100, "new": 120},
    "wiki": "enwiki",
    "server_name": "en.wikipedia.org",
}


def test_maps_fields():
    record = to_record(RAW)

    assert record["event_id"] == RAW["meta"]["id"]
    assert record["event_time"] == datetime(2026, 9, 29, 16, 29, 24, 787000, tzinfo=UTC)
    assert record["server_name"] == "en.wikipedia.org"
    assert record["bot"] is False
    assert record["minor"] is True
    assert (record["length_old"], record["length_new"]) == (100, 120)


def test_optional_fields_default_to_none():
    raw = {k: v for k, v in RAW.items() if k not in ("comment", "minor", "length")}

    record = to_record(raw)

    assert record["comment"] is None
    assert record["minor"] is None
    assert record["length_old"] is None and record["length_new"] is None


def test_record_validates_against_avro_schema():
    schema = parse_schema(json.loads(load_schema()))

    assert validate(to_record(RAW), schema)


@pytest.mark.parametrize("missing", ["meta", "wiki", "server_name", "user", "bot"])
def test_missing_required_field_raises(missing):
    raw = {k: v for k, v in RAW.items() if k != missing}

    with pytest.raises(ValueError, match=missing):
        to_record(raw)


def test_missing_meta_id_raises():
    raw = {**RAW, "meta": {"dt": RAW["meta"]["dt"]}}

    with pytest.raises(ValueError, match="meta.id"):
        to_record(raw)
