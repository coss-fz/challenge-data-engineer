"""
Tests for transform.py
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from src.transform import run_silver, run_gold # pylint: disable=wrong-import-position




def test_calls_execute_sql_file_with_correct_path():
    """Test that run_silver and run_gold call execute_sql_file with the correct SQL file paths"""
    mock_conn = MagicMock()
    with patch("src.transform.execute_sql_file") as mock_exec_silver:
        run_silver(mock_conn)
        mock_exec_silver.assert_called_once()
        call_args = mock_exec_silver.call_args
        assert call_args[0][0] is mock_conn
        assert "create_olap_silver.sql" in call_args[0][1]
    with patch("src.transform.execute_sql_file") as mock_exec_gold:
        run_gold(mock_conn)
        mock_exec_gold.assert_called_once()
        call_args = mock_exec_gold.call_args
        assert call_args[0][0] is mock_conn
        assert "create_olap_gold.sql" in call_args[0][1]

def test_raises_runtime_error_on_failure():
    """Test that run_silver and run_gold raise RuntimeError when execute_sql_file fails"""
    mock_conn = MagicMock()
    with patch("src.transform.execute_sql_file", side_effect=Exception("disk full")):
        with pytest.raises(RuntimeError, match="Silver layer transformation failed"):
            run_silver(mock_conn)
    with patch("src.transform.execute_sql_file", side_effect=Exception("disk full")):
        with pytest.raises(RuntimeError, match="Gold layer transformation failed"):
            run_gold(mock_conn)
