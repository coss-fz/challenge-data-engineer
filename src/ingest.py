"""
CSV ingestion into 'olap_bronze'
"""

import os
import re
import logging
import pandas as pd
from sqlalchemy import inspect, Engine
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm

from src.db import execute_sql_file


logger = logging.getLogger(__name__)




SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")


def _snake(name:str) -> str:
    """Convert camelCase / TitleCase / mixed to snake_case"""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    return s.strip().lower().replace(" ", "_")


def map_columns(df:pd.DataFrame) -> pd.DataFrame:
    """Normalize and map DataFrame columns according to the target table configuration"""
    df.columns = [_snake(c) for c in df.columns]
    return df


def load_table(df:pd.DataFrame, table:str, engine:Engine,
               schema:str='olap_bronze', batch_size:int=10000) -> None:
    """
    Load a DataFrame into a database table using SQLAlchemy
    with a progress bar for monitoring
    """
    inspector = inspect(engine)
    try:
        if not inspector.has_table(table, schema=schema):
            raise ValueError(f"Target table '{schema}.{table}' does not exist in the database")
        with tqdm(total=len(df), desc="     Progress") as pbar:
            for i in range(0, len(df), batch_size):
                chunk = df.iloc[i : i + batch_size]
                chunk.to_sql(
                    table,
                    engine,
                    schema=schema,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                pbar.update(len(chunk))
        logger.info("Successfully loaded %d rows to 'olap_bronze.%s'", len(df), table)
    except SQLAlchemyError as e:
        raise RuntimeError(f"Failed to load table '{schema}.{table}': {e}") from e


def ingest_all(conn, data_dir:str, engine:Engine) -> None:
    """
    - Ingest all six CSV files into 'olap_bronze'
    - Returns a dict with row counts per table
    """
    if not os.path.isdir(data_dir):
        raise ValueError(f"Data directory does not exist: {data_dir}")

    try:
        execute_sql_file(conn, os.path.join(SQL_DIR, "create_olap_bronze.sql"))
    except RuntimeError as e:
        raise RuntimeError(f"Failed to create bronze schema: {e}") from e

    file_table_map = {
        "SalesFINAL12312016.csv":        "sales",
        "BegInvFINAL12312016.csv":       "beg_inventory",
        "EndInvFINAL12312016.csv":       "end_inventory",
        "PurchasesFINAL12312016.csv":    "purchases",
        "InvoicePurchases12312016.csv":  "invoice_purchases",
        "2017PurchasePricesDec.csv":     "purchase_prices",
    }

    for filename, table in file_table_map.items():
        filepath = os.path.join(data_dir, filename)

        try:
            if not os.path.exists(filepath):
                error_msg = f"CSV file not found: '{filepath}'"
                raise FileNotFoundError(error_msg)

            df = pd.read_csv(filepath)
            if df.empty:
                logger.warning("CSV file '%s' is empty", filename)
                continue
            df = map_columns(df)
            load_table(df, table, engine)
        except Exception as e: #pylint: disable=broad-exception-caught
            raise RuntimeError(f"Failed to ingest file '{filename}': {e}") from e
