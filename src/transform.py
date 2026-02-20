"""
Run silver and gold SQL transformations
"""

import os
import logging

from src.db import execute_sql_file


logger = logging.getLogger(__name__)




SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")


def run_silver(conn) -> None:
    """Execute the Silver layer transformations for the star schema"""
    try:
        execute_sql_file(conn, os.path.join(SQL_DIR, "create_olap_silver.sql"))
        logger.info("Silver layer transformations completed successfully")
    except Exception as e:
        raise RuntimeError(f"Silver layer transformation failed: {e}") from e


def run_gold(conn) -> None:
    """Execute the Gold layer transformations for profit and margin analytics"""
    try:
        execute_sql_file(conn, os.path.join(SQL_DIR, "create_olap_gold.sql"))
        logger.info("Gold layer transformations completed successfully")
    except Exception as e:
        raise RuntimeError(f"Gold layer transformation failed: {e}") from e
