"""Database module for extraction history persistence using SQLite."""

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

DB_DIR = Path(__file__).parent / "data"
DB_DIR.mkdir(exist_ok=True)
DB_PATH = DB_DIR / "data.db"


@dataclass
class ExtractionRecord:
    id: Optional[int]
    file_id: str
    filename: str
    model_name: str
    start_page: int
    end_page: int
    product_count: int
    json_data: str
    total_cost: Optional[float]
    prompt_tokens: Optional[int]
    completion_tokens: Optional[int]
    created_at: str


def _get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path = DB_PATH) -> None:
    conn = _get_connection(db_path)
    try:
        conn.executescript("""\
CREATE TABLE IF NOT EXISTS extraction_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id TEXT NOT NULL,
    filename TEXT NOT NULL,
    model_name TEXT NOT NULL,
    start_page INTEGER NOT NULL,
    end_page INTEGER NOT NULL,
    product_count INTEGER NOT NULL DEFAULT 0,
    json_data TEXT NOT NULL DEFAULT '[]',
    total_cost REAL,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_history_created_at ON extraction_history(created_at DESC);
""")
        conn.commit()
    finally:
        conn.close()


def save_record(record: ExtractionRecord, db_path: Path = DB_PATH) -> int:
    conn = _get_connection(db_path)
    try:
        cursor = conn.execute(
            """INSERT INTO extraction_history
                (file_id, filename, model_name, start_page, end_page,
                 product_count, json_data, total_cost, prompt_tokens,
                 completion_tokens, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (record.file_id, record.filename, record.model_name,
             record.start_page, record.end_page, record.product_count,
             record.json_data, record.total_cost, record.prompt_tokens,
             record.completion_tokens, record.created_at),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def get_history(limit: int = 50, offset: int = 0, db_path: Path = DB_PATH) -> list[dict]:
    conn = _get_connection(db_path)
    try:
        rows = conn.execute(
            """SELECT id, file_id, filename, model_name, start_page, end_page,
                      product_count, total_cost, prompt_tokens, completion_tokens, created_at
               FROM extraction_history ORDER BY created_at DESC LIMIT ? OFFSET ?""",
            (limit, offset),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_record(record_id: int, db_path: Path = DB_PATH) -> Optional[ExtractionRecord]:
    conn = _get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM extraction_history WHERE id = ?", (record_id,)
        ).fetchone()
        if row is None:
            return None
        return ExtractionRecord(**dict(row))
    finally:
        conn.close()


def update_record_json(record_id: int, json_data: str, product_count: int, db_path: Path = DB_PATH) -> bool:
    conn = _get_connection(db_path)
    try:
        cursor = conn.execute(
            "UPDATE extraction_history SET json_data = ?, product_count = ? WHERE id = ?",
            (json_data, product_count, record_id),
        )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def delete_record(record_id: int, db_path: Path = DB_PATH) -> bool:
    conn = _get_connection(db_path)
    try:
        cursor = conn.execute(
            "DELETE FROM extraction_history WHERE id = ?", (record_id,)
        )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()
