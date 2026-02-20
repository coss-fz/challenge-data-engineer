"""
Tests for ingest.py
"""

import sys
from pathlib import Path
import logging
from unittest.mock import patch, MagicMock
import pytest
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from src.ingest import load_table, ingest_all # pylint: disable=wrong-import-position


pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")




REQUIRED_FILES = [
    "SalesFINAL12312016.csv",
    "BegInvFINAL12312016.csv",
    "EndInvFINAL12312016.csv",
    "PurchasesFINAL12312016.csv",
    "InvoicePurchases12312016.csv",
    "2017PurchasePricesDec.csv",
]

def _make_inspector(has_table=True):
    inspector = MagicMock()
    inspector.has_table.return_value = has_table
    return inspector


# Tests for load_table
def test_raises_if_table_does_not_exist():
    """Test that load_table raises ValueError if the target table does not exist in the database"""
    inspector = MagicMock()
    inspector.has_table.return_value = False
    engine = MagicMock()

    df = pd.DataFrame({"a": [1]})
    with patch("src.ingest.inspect", return_value=_make_inspector(has_table=False)):
        with pytest.raises(ValueError, match="does not exist"):
            load_table(df, "missing_table", engine)

def test_loads_data_in_batches():
    """Test that load_table correctly loads data in batches and updates the progress bar"""
    engine = MagicMock()
    df = pd.DataFrame({"a": range(25)})
    with patch("src.ingest.inspect", return_value=_make_inspector()):
        with patch.object(df.__class__, "to_sql"):
            load_table(df, "table", engine, batch_size=10)

def test_raises_runtime_error_on_sqlalchemy_error():
    """Test that load_table raises RuntimeError if a SQLAlchemyError occurs during loading"""
    engine = MagicMock()
    df = pd.DataFrame({"a": [1, 2, 3]})
    with patch("src.ingest.inspect", return_value=_make_inspector()):
        with patch("pandas.DataFrame.to_sql",
                    side_effect=SQLAlchemyError("insert failed")):
            with pytest.raises(RuntimeError, match="Failed to load table"):
                load_table(df, "table", engine)


# Tests for ingest_all
def test_raises_if_bronze_sql_fails(tmp_path):
    """Test that ingest_all raises RuntimeError if executing the bronze schema SQL fails"""
    with patch("src.ingest.execute_sql_file",
                side_effect=RuntimeError("SQL error")):
        with pytest.raises(RuntimeError, match="Failed to create bronze schema"):
            ingest_all(MagicMock(), tmp_path, MagicMock())

def test_skips_empty_csv_with_warning(tmp_path, caplog):
    """Test that ingest_all skips empty CSV files and logs a warning instead of loading"""
    data_dir = tmp_path
    for name in REQUIRED_FILES:
        (tmp_path / name).write_text("ColA,ColB")

    with patch("src.ingest.execute_sql_file"):
        with patch("src.ingest.load_table") as mock_load:
            with caplog.at_level(logging.WARNING, logger="src.ingest"):
                ingest_all(MagicMock(), data_dir, MagicMock())
    assert "is empty" in caplog.text
    assert mock_load.call_count == 0

def test_raises_if_csv_not_exists(tmp_path):
    """Test that ingest_all raises RuntimeError if a required CSV file is missing"""
    data_dir = tmp_path
    with patch("src.ingest.execute_sql_file"):
        with pytest.raises(RuntimeError, match="CSV file not found"):
            ingest_all(MagicMock(), data_dir, MagicMock())
