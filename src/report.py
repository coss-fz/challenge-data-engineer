"""
Generate Excel reports from the 'olap_gold' views
"""

import os
import logging
import pandas as pd


logger = logging.getLogger(__name__)




VIEW_MAPPING = {
    "vw_top_10_products_profit":    "Top 10 Products by Profit",
    "vw_top_10_products_margin":    "Top 10 Products by Margin",
    "vw_top_10_brands_profit":      "Top 10 Brands by Profit",
    "vw_top_10_brands_margin":      "Top 10 Brands by Margin",
    "vw_drop_candidates_products":  "Drop Candidates (Products)",
    "vw_drop_candidates_brands":    "Drop Candidates (Brands)",
}


def _ensure_reports_dir(path: str) -> None:
    """Create the reports directory if it does not exist"""
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)  # pragma: no cover


def export_views_to_excel(engine, reports_dir:str ="reports",
                          excel_name:str="liquor_distribution_reports.xlsx") -> None:
    """Export each view to a sheet in an Excel workbook"""
    _ensure_reports_dir(reports_dir)
    out_path = os.path.join(reports_dir, excel_name)

    with pd.ExcelWriter(out_path, engine='openpyxl') as writer: # pylint: disable=abstract-class-instantiated
        for view, name in VIEW_MAPPING.items():
            try:
                df = pd.read_sql(f"SELECT * FROM olap_gold.{view}", con=engine)
            except Exception as e: #pylint: disable=broad-exception-caught
                logger.warning("Failed to read view %s: %s (skipping)", view, e, exc_info=True)
                pd.DataFrame({"error": [str(e)]}).to_excel(writer, sheet_name=name, index=False)
                continue
            df.to_excel(writer, sheet_name=name, index=False)

            worksheet = writer.sheets[name]
            for i, col in enumerate(df.columns):
                column_len = max(
                    df[col].astype(str).map(len).max(),
                    len(str(col))
                ) + 2
                worksheet.column_dimensions[chr(65 + i)].width = column_len

    logger.info("Excel file written to '%s'", out_path)
