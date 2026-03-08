from __future__ import annotations

import sqlite3

import pytest

from hiring_compass_au.infra.storage.schema import init_all_tables


@pytest.fixture()
def conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    init_all_tables(conn)
    try:
        yield conn
    finally:
        conn.close()
