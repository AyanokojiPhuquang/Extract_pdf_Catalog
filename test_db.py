"""Unit tests for db.py module."""

import tempfile
from pathlib import Path

from db import ExtractionRecord, get_history, get_record, init_db, save_record


def _tmp_db() -> Path:
    """Return a path to a temporary database file."""
    return Path(tempfile.mktemp(suffix=".db"))


def test_init_db_creates_table():
    db = _tmp_db()
    init_db(db)
    import sqlite3

    conn = sqlite3.connect(str(db))
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='extraction_history'"
    )
    assert cursor.fetchone() is not None
    conn.close()
    db.unlink(missing_ok=True)


def test_save_and_get_record():
    db = _tmp_db()
    init_db(db)
    record = ExtractionRecord(
        id=None,
        file_id="abc123",
        filename="test.pdf",
        model_name="anthropic/claude-sonnet-4",
        start_page=0,
        end_page=10,
        product_count=5,
        csv_data="h1,h2\nv1,v2",
        total_cost=0.005,
        prompt_tokens=100,
        completion_tokens=50,
        created_at="2025-01-01 12:00:00",
    )
    rid = save_record(record, db)
    assert isinstance(rid, int)
    assert rid > 0

    fetched = get_record(rid, db)
    assert fetched is not None
    assert fetched.file_id == "abc123"
    assert fetched.filename == "test.pdf"
    assert fetched.csv_data == "h1,h2\nv1,v2"
    assert fetched.total_cost == 0.005
    assert fetched.prompt_tokens == 100
    assert fetched.completion_tokens == 50
    db.unlink(missing_ok=True)


def test_get_record_not_found():
    db = _tmp_db()
    init_db(db)
    assert get_record(999, db) is None
    db.unlink(missing_ok=True)


def test_get_history_excludes_csv_data():
    db = _tmp_db()
    init_db(db)
    record = ExtractionRecord(
        id=None,
        file_id="f1",
        filename="a.pdf",
        model_name="model-a",
        start_page=0,
        end_page=5,
        product_count=3,
        csv_data="big csv content",
        total_cost=0.01,
        prompt_tokens=200,
        completion_tokens=100,
        created_at="2025-01-01 12:00:00",
    )
    save_record(record, db)
    history = get_history(db_path=db)
    assert len(history) == 1
    assert "csv_data" not in history[0]
    assert history[0]["filename"] == "a.pdf"
    db.unlink(missing_ok=True)


def test_get_history_sorted_desc():
    db = _tmp_db()
    init_db(db)
    for i, ts in enumerate(["2025-01-01 10:00:00", "2025-01-03 10:00:00", "2025-01-02 10:00:00"]):
        r = ExtractionRecord(
            id=None,
            file_id=f"f{i}",
            filename=f"file{i}.pdf",
            model_name="m",
            start_page=0,
            end_page=1,
            product_count=0,
            csv_data="",
            total_cost=None,
            prompt_tokens=None,
            completion_tokens=None,
            created_at=ts,
        )
        save_record(r, db)
    history = get_history(db_path=db)
    timestamps = [h["created_at"] for h in history]
    assert timestamps == sorted(timestamps, reverse=True)
    db.unlink(missing_ok=True)


def test_get_history_limit_offset():
    db = _tmp_db()
    init_db(db)
    for i in range(5):
        r = ExtractionRecord(
            id=None,
            file_id=f"f{i}",
            filename=f"file{i}.pdf",
            model_name="m",
            start_page=0,
            end_page=1,
            product_count=i,
            csv_data="",
            total_cost=None,
            prompt_tokens=None,
            completion_tokens=None,
            created_at=f"2025-01-0{i+1} 10:00:00",
        )
        save_record(r, db)
    history = get_history(limit=2, offset=0, db_path=db)
    assert len(history) == 2
    history_offset = get_history(limit=2, offset=2, db_path=db)
    assert len(history_offset) == 2
    assert history[0]["file_id"] != history_offset[0]["file_id"]
    db.unlink(missing_ok=True)
