"""
Tests for report.py
"""

import sys
from pathlib import Path
import pandas as pd
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from src.report import export_views_to_excel, VIEW_MAPPING # pylint: disable=wrong-import-position




def test_creates_excel_file(tmp_path):
    """Test that the function creates an Excel file with the expected sheets"""
    df = pd.DataFrame({"product": ["Beer", "Wine"], "profit": [100.0, 200.0]})
    with patch("src.report.pd.read_sql", return_value=df):
        export_views_to_excel(MagicMock(), reports_dir=str(tmp_path))

    files = list(tmp_path.iterdir())
    assert any(f.suffix == ".xlsx" for f in files)

def test_read_sql_called_for_each_view(tmp_path):
    """Test that pd.read_sql is called for each view in the mapping"""
    df = pd.DataFrame({"product": ["Beer", "Wine"], "profit": [100.0, 200.0]})
    with patch("src.report.pd.read_sql", return_value=df) as mock_read:
        export_views_to_excel(MagicMock(), reports_dir=str(tmp_path))
    assert mock_read.call_count == len(VIEW_MAPPING)

def test_all_views_fail(tmp_path):
    """Test that if every view fails, all sheets should contain an error column"""
    with patch("src.report.pd.read_sql", side_effect=Exception("all broken")):
        export_views_to_excel(MagicMock(), reports_dir=str(tmp_path))
    excel_file = pd.ExcelFile(str(tmp_path / "liquor_distribution_reports.xlsx"))
    for sheet in excel_file.sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet)
        assert "error" in df.columns
