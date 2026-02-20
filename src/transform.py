"""
Run silver and gold SQL transformations
"""

import os
from src.db import execute_sql_file




SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")


def run_silver(conn) -> None:
    """Execute the Silver layer transformations for the star schema"""
    print("\n[SILVER] Running star schema transformations...")
    execute_sql_file(conn, os.path.join(SQL_DIR, "create_olap_silver.sql"))


def run_gold(conn) -> None:
    """Execute the Gold layer transformations for profit and margin analytics"""
    print("\n[GOLD] Running profit & margin analytics...")
    execute_sql_file(conn, os.path.join(SQL_DIR, "create_olap_gold.sql"))
