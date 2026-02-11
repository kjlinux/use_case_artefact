import pytest
from src.main import parse_date


def test_valid_date():
    result = parse_date("20250616")
    assert result.year == 2025
    assert result.month == 6
    assert result.day == 16


def test_invalid_date():
    with pytest.raises(Exception):
        parse_date("not-a-date")


def test_wrong_format():
    with pytest.raises(Exception):
        parse_date("2025-06-16")
