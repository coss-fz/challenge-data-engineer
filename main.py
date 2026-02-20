"""
Full pipeline entry point
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from src.db import get_engine, get_psycopg2_connection # pylint: disable=wrong-import-position
from src.ingest import ingest_all # pylint: disable=wrong-import-position
from src.transform import run_silver, run_gold # pylint: disable=wrong-import-position




def step_ingest(data_dir:str) -> None:
    """Ingest CSV files from the given directory into the olap_bronze schema"""
    engine = get_engine()
    conn = get_psycopg2_connection()
    counts = ingest_all(conn, data_dir, engine)
    print(f"\nIngested tables: {counts}")


def step_transform() -> None:
    """Run Silver and Gold transformation steps using a PostgreSQL connection"""
    conn = get_psycopg2_connection()
    try:
        run_silver(conn)
        run_gold(conn)
    finally:
        conn.close()


def main() -> None:
    """Parse CLI arguments and execute the selected pipeline steps"""
    parser = argparse.ArgumentParser(description="Liquor distribution pipeline")
    parser.add_argument("--step", choices=["ingest", "transform", "report", "all"],
                        default="all", help="Pipeline step to run (all by default)")
    parser.add_argument("--data-dir", default=os.getenv("DATA_DIR", "./data"),
                        help="Directory containing the CSV files")
    args = parser.parse_args()

    print("\nThe pipeline has started!")

    if args.step in ("ingest", "all"):
        step_ingest(args.data_dir)

    if args.step in ("transform", "all"):
        step_transform()

    print("\nThe pipeline finished succesfully!")


if __name__ == "__main__":
    main()
