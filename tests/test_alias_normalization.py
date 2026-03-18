import pytest
from app.tools import normalize_column_name, _normalize_rows

def test_normalize_column_name():
    assert normalize_column_name("COUNT(*)") == "count"
    assert normalize_column_name("AVG(price)") == "avg_price"
    assert normalize_column_name("SumOfSales") == "sumofsales"
    assert normalize_column_name("1+1") == "n_1_1"
    assert normalize_column_name("  trimmed  ") == "trimmed"
    assert normalize_column_name("multiple   spaces") == "multiple_spaces"
    assert normalize_column_name("!@#$%^") == "col"

def test_normalize_rows_basic():
    rows = [{"COUNT(*)": 10, "AVG(delay)": 5.5}]
    norm_rows = _normalize_rows(rows)
    assert norm_rows == [{"count": 10, "avg_delay": 5.5}]

def test_normalize_rows_duplicates():
    # Test collision handling: same base name
    rows = [{"count": 10, "COUNT": 10, "Count": 10}]
    norm_rows = _normalize_rows(rows)
    # First one wins, subsequent get suffixes
    assert norm_rows[0]["count"] == 10
    assert norm_rows[0]["count_1"] == 10
    assert norm_rows[0]["count_2"] == 10

def test_normalize_rows_expressions():
    # Verify that expressions with parentheses are handled descriptively but uniquely
    rows = [{"count(*)": 10, "count(id)": 10}]
    norm_rows = _normalize_rows(rows)
    assert "count" in norm_rows[0]
    assert "count_id" in norm_rows[0]

def test_normalize_rows_empty():
    assert _normalize_rows([]) == []

def test_normalize_rows_mixed():
    rows = [{"A": 1, "a": 2}] # Case insensitive collision
    norm_rows = _normalize_rows(rows)
    assert norm_rows[0]["a"] == 1
    assert norm_rows[0]["a_1"] == 2
