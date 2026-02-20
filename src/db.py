"""
Database connection utilities
"""

import os
import logging
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


logger = logging.getLogger(__name__)

load_dotenv()




def get_engine():
    """Create and return a SQLAlchemy engine using the environment-based connection string"""
    required_vars = [
        'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_DB'
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

    try:
        conn_string = (
            f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
            f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        )
        engine = create_engine(conn_string)

        with engine.connect() as _:
            pass
        logger.info("SQLAlchemy engine created successfully")
        return engine
    except SQLAlchemyError as e:
        raise RuntimeError(f"Failed to create database engine: {e}") from e


def get_psycopg2_connection():
    """Create and return a psycopg2 connection using environment variables"""
    required_vars = [
        'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_DB'
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        logger.info("psycopg2 connection established successfully")
        return conn
    except psycopg2.OperationalError as e:
        raise RuntimeError(f"Failed to connect to PostgreSQL: {e}") from e
    except psycopg2.Error as e:
        raise RuntimeError(f"Unexpected error connecting to PostgreSQL: {e}") from e


def execute_sql_file(conn, filepath:str) -> None:
    """Execute a SQL file against the given psycopg2 connection"""

    try:
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"SQL file not found: {filepath}")

        with open(filepath, "r", encoding='utf-8') as f:
            sql = f.read()
    except Exception as e: #pylint: disable=broad-exception-caught
        raise RuntimeError(f"Failed to read SQL file {filepath}: {e}") from e

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info("File successfully executed: '%s'", filepath)
    except psycopg2.Error as e:
        conn.rollback()
        raise RuntimeError(f"Failed to execute SQL file {filepath}: {e}") from e
