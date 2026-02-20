"""
Full pipeline entry point
"""

import argparse
import os
import sys
import logging

sys.path.insert(0, os.path.dirname(__file__))

from src.db import get_engine, get_psycopg2_connection # pylint: disable=wrong-import-position
from src.ingest import ingest_all # pylint: disable=wrong-import-position
from src.transform import run_silver, run_gold # pylint: disable=wrong-import-position
from src.report import export_views_to_excel # pylint: disable=wrong-import-position


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)




def step_ingest(data_dir:str) -> None:
    """Ingest CSV files from the given directory into the olap_bronze schema"""
    try:
        engine = get_engine()
    except RuntimeError as e:
        logger.error("Database engine creation failed: %s", e, exc_info=True)
        raise

    try:
        conn = get_psycopg2_connection()
    except RuntimeError as e:
        logger.error("Database connection failed: %s", e, exc_info=True)
        raise

    try:
        ingest_all(conn, data_dir, engine)
        logger.info("Ingestion step completed successfully")
    except RuntimeError as e:
        logger.error("Ingestion failed: %s", e, exc_info=True)
        raise
    finally:
        conn.close()
        engine.dispose()


def step_transform() -> None:
    """Run Silver and Gold transformation steps using a PostgreSQL connection"""
    try:
        conn = get_psycopg2_connection()
    except RuntimeError as e:
        logger.error("Database connection failed: %s", e, exc_info=True)
        raise

    try:
        run_silver(conn)
        run_gold(conn)
        logger.info("Transform step completed successfully")
    except RuntimeError as e:
        logger.error("Transformation failed: %s", e, exc_info=True)
        raise
    finally:
        conn.close()


def step_report(reports_dir:str) -> None:
    """Generate Excel and PDF reports from the 'olap_gold' views"""
    try:
        engine = get_engine()
    except RuntimeError as e:
        logger.error("Database engine creation failed: %s", e, exc_info=True)
        raise

    try:
        export_views_to_excel(engine, reports_dir)
        logger.info("Report generation completed successfully")
    except RuntimeError as e:
        logger.error("Report generation failed: %s", e, exc_info=True)
        raise
    finally:
        engine.dispose()


def main() -> None:
    """Parse CLI arguments and execute the selected pipeline steps"""
    parser = argparse.ArgumentParser(description="Liquor distribution pipeline")
    parser.add_argument("--step", choices=["ingest", "transform", "report", "all"],
                        default="all", help="Pipeline step to run (all by default)")
    parser.add_argument("--data-dir", default=os.getenv("DATA_DIR", "./data"),
                        help="Directory containing the CSV files")
    parser.add_argument("--reports-dir", default=os.getenv("REPORTS_DIR", "./reports"),
                        help="Directory to save generated reports")
    args = parser.parse_args()

    logger.info("Pipeline started with step='%s', data_dir='%s', reports_dir='%s'",
                args.step, args.data_dir, args.reports_dir)

    try:
        if args.step in ("ingest", "all"):
            logger.info("Starting ingestion step (Bronze layer)")
            step_ingest(args.data_dir)
    except RuntimeError:
        sys.exit(1)

    try:
        if args.step in ("transform", "all"):
            logger.info("Starting transformation step")
            step_transform()
    except RuntimeError:
        sys.exit(1)

    try:
        if args.step in ("report", "all"):
            logger.info("Starting report generation step")
            step_report(args.reports_dir)
    except RuntimeError:
        sys.exit(1)

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()
