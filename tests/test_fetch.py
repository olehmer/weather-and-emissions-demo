"""
Unit tests for fetch.py.

All network calls and DB connections are mocked — no live credentials needed.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import fetch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_response(json_data, status_code=200):
    """Return a mock requests.Response."""
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = json_data
    mock.raise_for_status = MagicMock()
    return mock


DALLAS = {"city": "Dallas", "state": "TX", "grid": "ERCO", "lat": 32.78, "lon": -96.80}


# ---------------------------------------------------------------------------
# fetch_weather_for_city
# ---------------------------------------------------------------------------

OPEN_METEO_RESPONSE = {
    "hourly": {
        "time":             ["2024-01-01T00:00", "2024-01-01T01:00"],
        "temperature_2m":   [5.0, 6.5],
        "precipitation":    [0.0, 0.2],
        "windspeed_10m":    [12.0, 14.3],
    }
}


def test_fetch_weather_for_city_returns_one_row_per_hour():
    with patch("requests.get", return_value=make_response(OPEN_METEO_RESPONSE)):
        rows = fetch.fetch_weather_for_city(DALLAS)

    assert len(rows) == 2


def test_fetch_weather_for_city_row_fields():
    with patch("requests.get", return_value=make_response(OPEN_METEO_RESPONSE)):
        rows = fetch.fetch_weather_for_city(DALLAS)

    row = rows[0]
    assert row["city"] == "Dallas"
    assert row["state"] == "TX"
    assert row["grid_region"] == "ERCO"
    assert row["latitude"] == 32.78
    assert row["longitude"] == -96.80
    assert row["temperature_c"] == 5.0
    assert row["precipitation_mm"] == 0.0
    assert row["windspeed_kmh"] == 12.0


def test_fetch_weather_for_city_timestamps_are_utc():
    with patch("requests.get", return_value=make_response(OPEN_METEO_RESPONSE)):
        rows = fetch.fetch_weather_for_city(DALLAS)

    assert rows[0]["timestamp_utc"] == datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert rows[1]["timestamp_utc"] == datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc)


def test_fetch_weather_for_city_empty_hourly():
    empty_response = {"hourly": {"time": [], "temperature_2m": [], "precipitation": [], "windspeed_10m": []}}
    with patch("requests.get", return_value=make_response(empty_response)):
        rows = fetch.fetch_weather_for_city(DALLAS)

    assert rows == []


def test_fetch_weather_for_city_propagates_http_error():
    mock_resp = make_response({}, status_code=500)
    mock_resp.raise_for_status.side_effect = Exception("500 Server Error")
    with patch("requests.get", return_value=mock_resp):
        with pytest.raises(Exception, match="500 Server Error"):
            fetch.fetch_weather_for_city(DALLAS)


# ---------------------------------------------------------------------------
# fetch_all_weather
# ---------------------------------------------------------------------------

def test_fetch_all_weather_skips_failed_cities():
    """A failure for one city should not abort the rest."""
    good_rows = [{"city": "Dallas", "state": "TX"}]

    call_count = 0

    def side_effect(city):
        nonlocal call_count
        call_count += 1
        if city["city"] == "Pittsburgh":
            raise Exception("timeout")
        return good_rows

    with patch("fetch.fetch_weather_for_city", side_effect=side_effect):
        result = fetch.fetch_all_weather()

    # All 15 cities attempted; Pittsburgh failed, others succeeded
    assert call_count == len(fetch.WEATHER_CITIES)
    # 14 cities × 1 row each
    assert len(result) == (len(fetch.WEATHER_CITIES) - 1) * len(good_rows)


# ---------------------------------------------------------------------------
# fetch_grid_generation_for_respondent
# ---------------------------------------------------------------------------

EIA_RESPONSE = {
    "response": {
        "data": [
            {"period": "2024-01-01T05", "fueltype": "NG",  "value": "1234.5"},
            {"period": "2024-01-01T05", "fueltype": "WND", "value": "567"},
            {"period": "2024-01-01T05", "fueltype": "NUC", "value": None},
        ]
    }
}


def test_fetch_grid_generation_row_count():
    with patch("requests.get", return_value=make_response(EIA_RESPONSE)):
        rows = fetch.fetch_grid_generation_for_respondent("PJM", "fake_key")

    assert len(rows) == 3


def test_fetch_grid_generation_numeric_value():
    with patch("requests.get", return_value=make_response(EIA_RESPONSE)):
        rows = fetch.fetch_grid_generation_for_respondent("PJM", "fake_key")

    ng_row = next(r for r in rows if r["fuel_type"] == "NG")
    assert ng_row["generation_mwh"] == pytest.approx(1234.5)


def test_fetch_grid_generation_null_value_becomes_none():
    with patch("requests.get", return_value=make_response(EIA_RESPONSE)):
        rows = fetch.fetch_grid_generation_for_respondent("PJM", "fake_key")

    nuc_row = next(r for r in rows if r["fuel_type"] == "NUC")
    assert nuc_row["generation_mwh"] is None


def test_fetch_grid_generation_fuel_type_name_lookup():
    with patch("requests.get", return_value=make_response(EIA_RESPONSE)):
        rows = fetch.fetch_grid_generation_for_respondent("PJM", "fake_key")

    wnd_row = next(r for r in rows if r["fuel_type"] == "WND")
    assert wnd_row["fuel_type_name"] == "Wind"


def test_fetch_grid_generation_unknown_fuel_type_uses_code():
    eia_resp = {"response": {"data": [
        {"period": "2024-01-01T05", "fueltype": "XYZ", "value": "100"}
    ]}}
    with patch("requests.get", return_value=make_response(eia_resp)):
        rows = fetch.fetch_grid_generation_for_respondent("PJM", "fake_key")

    assert rows[0]["fuel_type_name"] == "XYZ"


def test_fetch_grid_generation_skips_bad_timestamp():
    eia_resp = {"response": {"data": [
        {"period": "not-a-date", "fueltype": "NG", "value": "100"},
        {"period": "2024-01-01T05", "fueltype": "NG", "value": "200"},
    ]}}
    with patch("requests.get", return_value=make_response(eia_resp)):
        rows = fetch.fetch_grid_generation_for_respondent("PJM", "fake_key")

    assert len(rows) == 1
    assert rows[0]["generation_mwh"] == pytest.approx(200.0)


def test_fetch_grid_generation_non_numeric_value_becomes_none():
    eia_resp = {"response": {"data": [
        {"period": "2024-01-01T05", "fueltype": "NG", "value": "n/a"}
    ]}}
    with patch("requests.get", return_value=make_response(eia_resp)):
        rows = fetch.fetch_grid_generation_for_respondent("PJM", "fake_key")

    assert rows[0]["generation_mwh"] is None


def test_fetch_grid_generation_empty_response():
    eia_resp = {"response": {"data": []}}
    with patch("requests.get", return_value=make_response(eia_resp)):
        rows = fetch.fetch_grid_generation_for_respondent("PJM", "fake_key")

    assert rows == []


def test_fetch_grid_generation_missing_response_key():
    with patch("requests.get", return_value=make_response({})):
        rows = fetch.fetch_grid_generation_for_respondent("PJM", "fake_key")

    assert rows == []


def test_fetch_grid_generation_timestamp_is_utc():
    with patch("requests.get", return_value=make_response(EIA_RESPONSE)):
        rows = fetch.fetch_grid_generation_for_respondent("PJM", "fake_key")

    assert rows[0]["timestamp_utc"] == datetime(2024, 1, 1, 5, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# fetch_all_grid_generation
# ---------------------------------------------------------------------------

def test_fetch_all_grid_generation_raises_without_api_key(monkeypatch):
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    with pytest.raises(EnvironmentError, match="EIA_API_KEY"):
        fetch.fetch_all_grid_generation()


def test_fetch_all_grid_generation_skips_failed_respondents(monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", "fake_key")

    good_rows = [{"respondent": "PJM"}]

    def side_effect(respondent, api_key):
        if respondent == "ERCO":
            raise Exception("API error")
        return good_rows

    with patch("fetch.fetch_grid_generation_for_respondent", side_effect=side_effect):
        result = fetch.fetch_all_grid_generation()

    # PJM and CISO succeed (2 rows), ERCO fails
    assert len(result) == 2
