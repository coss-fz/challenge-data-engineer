"""
Database connection utilities
"""

import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine


load_dotenv()




def get_engine():
    """Create and return a SQLAlchemy engine using the environment-based connection string"""
    conn_string = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )
    return create_engine(conn_string)


def get_psycopg2_connection():
    """Create and return a psycopg2 connection using environment variables"""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def execute_sql_file(conn, filepath:str) -> None:
    """Execute a SQL file against the given psycopg2 connection"""
    with open(filepath, "r", encoding='utf-8') as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"  File successfully executed: '{filepath}'")
