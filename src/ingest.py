"""
CSV ingestion into 'olap_bronze'
"""

import os
import re
import pandas as pd
from sqlalchemy import Engine
from tqdm import tqdm

from src.db import execute_sql_file




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
               schema:str='olap_bronze', batch_size:int=10000) -> int:
    """
    Load a DataFrame into a database table using SQLAlchemy
    with a progress bar for monitoring
    """
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
    return len(df)


def ingest_all(conn, data_dir:str, engine:Engine) -> dict:
    """
    - Ingest all six CSV files into 'olap_bronze'
    - Returns a dict with row counts per table
    """
    print("\n[BRONZE] Running csv replicas...")
    execute_sql_file(conn, os.path.join(SQL_DIR, "create_olap_bronze.sql"))

    file_table_map = {
        "SalesFINAL12312016.csv":        "sales",
        "BegInvFINAL12312016.csv":       "beg_inventory",
        "EndInvFINAL12312016.csv":       "end_inventory",
        "PurchasesFINAL12312016.csv":    "purchases",
        "InvoicePurchases12312016.csv":  "invoice_purchases",
        "2017PurchasePricesDec.csv":     "purchase_prices",
    }

    counts = {}
    for filename, table in file_table_map.items():
        filepath = os.path.join(data_dir, filename)
        print(f"  Ingesting '{filename}' into 'olap_bronze.{table}'...")
        df = pd.read_csv(filepath)
        df = map_columns(df)
        n = load_table(df, table, engine)
        counts[table] = n
        print(f"     Loaded {n} rows")

    return counts
