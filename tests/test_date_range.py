import pytest

from src.date_range import processing_dates


def test_processing_dates_is_inclusive():
    assert processing_dates("2023-06-29", "2023-07-02") == [
        "2023-06-29", "2023-06-30", "2023-07-01", "2023-07-02"]


@pytest.mark.parametrize("start,end", [("2023-06-02", "2023-06-01"),
                                        ("2023-01-01", "2023-04-03")])
def test_processing_dates_rejects_invalid_ranges(start, end):
    with pytest.raises(ValueError):
        processing_dates(start, end)
