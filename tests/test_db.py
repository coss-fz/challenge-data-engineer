"""
Tests for db.py
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import psycopg2
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from src.db import get_engine, get_psycopg2_connection, execute_sql_file # pylint: disable=wrong-import-position




ENV_VARS = {
    "POSTGRES_USER": "user",
    "POSTGRES_PASSWORD": "password",
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "testdb",
}

@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    """Ensure all required env vars are set by default"""
    for k, v in ENV_VARS.items():
        monkeypatch.setenv(k, v)


def test_raises_when_env_var_missing(monkeypatch):
    """Test that get_engine raises ValueError when an env var is missing"""
    monkeypatch.delenv("POSTGRES_USER")
    with pytest.raises(ValueError, match="Missing environment variables"):
        get_engine()
    with pytest.raises(ValueError, match="Missing environment variables"):
        get_psycopg2_connection()


# Tests for get_engine
def test_returns_engine_when_env_vars_present():
    """Test that get_engine returns an engine when all env vars are set"""
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    with patch("src.db.create_engine", return_value=mock_engine):
        engine = get_engine()
        assert engine is mock_engine

def test_raises_runtime_error_on_sqlalchemy_error():
    """Test that get_engine raises RuntimeError when SQLAlchemyError occurs"""
    with patch("src.db.create_engine", side_effect=SQLAlchemyError("conn failed")):
        with pytest.raises(RuntimeError, match="Failed to create database engine"):
            get_engine()


# Tests for get_psycopg2_connection
def test_returns_connection_when_env_vars_present():
    """Test that get_psycopg2_connection returns a connection when all env vars are set"""
    mock_conn = MagicMock()
    with patch("src.db.psycopg2.connect", return_value=mock_conn):
        conn = get_psycopg2_connection()
        assert conn is mock_conn

def test_raises_runtime_error_on_operational_error():
    """Test that get_psycopg2_connection raises RuntimeError when OperationalError occurs"""
    with patch("src.db.psycopg2.connect",
                side_effect=psycopg2.OperationalError("cannot connect")):
        with pytest.raises(RuntimeError, match="Failed to connect to PostgreSQL"):
            get_psycopg2_connection()

def test_raises_runtime_error_on_generic_psycopg2_error():
    """Test that get_psycopg2_connection raises RuntimeError when a generic psycopg2.Error occurs"""
    with patch("src.db.psycopg2.connect",
                side_effect=psycopg2.Error("unexpected")):
        with pytest.raises(RuntimeError, match="Unexpected error connecting to PostgreSQL"):
            get_psycopg2_connection()


# Tests for execute_sql_file
def test_executes_sql_and_commits(tmp_path):
    """Test that execute_sql_file executes the SQL and commits the transaction"""
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("SELECT 1;")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    execute_sql_file(mock_conn, str(sql_file))
    mock_cursor.execute.assert_called_once_with("SELECT 1;")
    mock_conn.commit.assert_called_once()

def test_raises_runtime_error_if_file_not_found():
    """Test that execute_sql_file raises RuntimeError if the SQL file does not exist"""
    mock_conn = MagicMock()
    with pytest.raises(RuntimeError, match="Failed to read SQL file"):
        execute_sql_file(mock_conn, "/nonexistent/path/query.sql")

def test_rolls_back_and_raises_on_psycopg2_error(tmp_path):
    """Test that execute_sql_file rolls back and raises RuntimeError if a psycopg2.Error occurs"""
    sql_file = tmp_path / "bad.sql"
    sql_file.write_text("SELECT *;")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = psycopg2.Error("syntax error")
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with pytest.raises(RuntimeError, match="Failed to execute SQL file"):
        execute_sql_file(mock_conn, str(sql_file))
    mock_conn.rollback.assert_called_once()
    mock_conn.commit.assert_not_called()
