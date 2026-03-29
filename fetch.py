"""
fetch.py

Pulls hourly data from two sources and stores it in Neon Postgres:
  1. Open-Meteo  — weather for 15 cities across PJM, ERCOT, and CAISO regions.
  2. EIA API v2  — hourly grid generation by fuel type for PJM, ERCO, CISO.

Cities are chosen to represent the geographic spread of each grid region,
with emphasis on data center hubs and renewable energy zones. Since this
project is using the free tier of Neon I'm grabbing just 5 cities for each
grid region to keep data volume limited.

Usage:
    python fetch.py

Environment variables:
    NEON_HOST
    NEON_USER
    NEON_PASSWORD
    NEON_DATABASE
    EIA_API_KEY

Tables created (if not exist):
    raw_weather_hourly       — one row per (hour, city)
    raw_grid_generation      — one row per (hour, respondent, fuel_type)
"""

import os
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Weather cities — 5 per grid region
# Each city chosen to represent load centers, renewable zones, or data center hubs
# ---------------------------------------------------------------------------

WEATHER_CITIES = [
    # --- PJM ---
    # Ashburn VA: world's largest data center concentration
    {"city": "Ashburn",     "state": "VA", "grid": "PJM",   "lat": 39.04,  "lon": -77.49},
    # Pittsburgh PA: wind/coal corridor, western PJM
    {"city": "Pittsburgh",  "state": "PA", "grid": "PJM",   "lat": 40.44,  "lon": -79.99},
    # Columbus OH: central PJM load center
    {"city": "Columbus",    "state": "OH", "grid": "PJM",   "lat": 39.96,  "lon": -82.99},
    # Chicago IL: western PJM edge, ComEd zone
    {"city": "Chicago",     "state": "IL", "grid": "PJM",   "lat": 41.88,  "lon": -87.63},
    # Raleigh NC: southern PJM, strong solar growth
    {"city": "Raleigh",     "state": "NC", "grid": "PJM",   "lat": 35.78,  "lon": -78.64},

    # --- ERCOT ---
    # Dallas TX: largest data center market in Texas
    {"city": "Dallas",      "state": "TX", "grid": "ERCO",  "lat": 32.78,  "lon": -96.80},
    # Houston TX: major load center, Gulf coast
    {"city": "Houston",     "state": "TX", "grid": "ERCO",  "lat": 29.76,  "lon": -95.37},
    # Abilene TX: heart of West Texas wind country
    {"city": "Abilene",     "state": "TX", "grid": "ERCO",  "lat": 32.45,  "lon": -99.73},
    # San Antonio TX: growing data center hub
    {"city": "San Antonio", "state": "TX", "grid": "ERCO",  "lat": 29.42,  "lon": -98.49},
    # Midland TX: Permian Basin solar and wind zone
    {"city": "Midland",     "state": "TX", "grid": "ERCO",  "lat": 31.99,  "lon": -102.08},

    # --- CAISO ---
    # San Jose CA: Silicon Valley data centers
    {"city": "San Jose",    "state": "CA", "grid": "CISO",  "lat": 37.34,  "lon": -121.89},
    # Fresno CA: Central Valley solar
    {"city": "Fresno",      "state": "CA", "grid": "CISO",  "lat": 36.74,  "lon": -119.79},
    # Bakersfield CA: Tehachapi wind + Mojave solar edge
    {"city": "Bakersfield", "state": "CA", "grid": "CISO",  "lat": 35.37,  "lon": -119.02},
    # Los Angeles CA: largest load center in CAISO
    {"city": "Los Angeles", "state": "CA", "grid": "CISO",  "lat": 34.05,  "lon": -118.24},
    # Palm Springs CA: desert solar and wind corridor
    {"city": "Palm Springs","state": "CA", "grid": "CISO",  "lat": 33.83,  "lon": -116.54},
]

# EIA balancing authority codes to fetch
EIA_RESPONDENTS = ["PJM", "ERCO", "CISO"]

# EIA fuel type codes to human-readable names
FUEL_TYPE_NAMES = {
    "NG":  "Natural Gas",
    "NUC": "Nuclear",
    "WAT": "Hydro",
    "WND": "Wind",
    "SUN": "Solar",
    "OIL": "Oil",
    "OTH": "Other",
    "COL": "Coal",
    "BAT": "Battery Storage",
}

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_connection():
    """Return a psycopg2 connection using env vars."""
    return psycopg2.connect(
        host=os.environ["NEON_HOST"],
        port=5432,
        user=os.environ["NEON_USER"],
        password=os.environ["NEON_PASSWORD"],
        dbname=os.environ["NEON_DATABASE"],
        sslmode="require",
    )


def ensure_tables(conn):
    """Create raw tables if they don't already exist."""
    with conn.cursor() as cur:

        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_weather_hourly (
                id                  SERIAL PRIMARY KEY,
                fetched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                timestamp_utc       TIMESTAMPTZ NOT NULL,
                city                TEXT NOT NULL,
                state               TEXT NOT NULL,
                grid_region         TEXT NOT NULL,
                latitude            DOUBLE PRECISION NOT NULL,
                longitude           DOUBLE PRECISION NOT NULL,
                temperature_c       DOUBLE PRECISION,
                precipitation_mm    DOUBLE PRECISION,
                windspeed_kmh       DOUBLE PRECISION,
                UNIQUE (timestamp_utc, city, state)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_grid_generation (
                id                  SERIAL PRIMARY KEY,
                fetched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                timestamp_utc       TIMESTAMPTZ NOT NULL,
                respondent          TEXT NOT NULL,
                fuel_type           TEXT NOT NULL,
                fuel_type_name      TEXT,
                generation_mwh      DOUBLE PRECISION,
                UNIQUE (timestamp_utc, respondent, fuel_type)
            );
        """)

    conn.commit()
    log.info("Tables verified / created.")


# ---------------------------------------------------------------------------
# Open-Meteo — weather
# ---------------------------------------------------------------------------

def fetch_weather_for_city(city: dict) -> list:
    """
    Fetch the last 2 days of hourly weather for a single city from Open-Meteo.
    Returns a list of row dicts.

    Uses past_days=2 so delayed or slow runs don't produce gaps.
    Each city is fetched individually — Open-Meteo is fast and has no
    rate limit concerns at this scale (15 cities per run).
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":     city["lat"],
        "longitude":    city["lon"],
        "hourly":       "temperature_2m,precipitation,windspeed_10m",
        "timezone":     "UTC",
        "past_days":    1,
        "forecast_days": 0,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    hourly = data["hourly"]
    rows = []
    for i, ts_str in enumerate(hourly["time"]):
        ts_utc = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
        rows.append({
            "timestamp_utc":    ts_utc,
            "city":             city["city"],
            "state":            city["state"],
            "grid_region":      city["grid"],
            "latitude":         city["lat"],
            "longitude":        city["lon"],
            "temperature_c":    hourly["temperature_2m"][i],
            "precipitation_mm": hourly["precipitation"][i],
            "windspeed_kmh":    hourly["windspeed_10m"][i],
        })
    return rows


def fetch_all_weather() -> list:
    """
    Fetch weather for all 15 cities. Each city is fetched independently —
    a failure for one city is logged and skipped rather than aborting the batch.
    Returns all successfully fetched rows.
    """
    all_rows = []
    for city in WEATHER_CITIES:
        try:
            rows = fetch_weather_for_city(city)
            all_rows.extend(rows)
            log.info(f"  {city['city']}, {city['state']}: {len(rows)} rows fetched.")
        except Exception as e:
            log.error(f"  {city['city']}, {city['state']}: fetch failed — {e}")
    log.info(f"Weather fetch complete: {len(all_rows)} total rows across {len(WEATHER_CITIES)} cities.")
    return all_rows


def load_weather(conn, rows: list):
    """Upsert weather rows (idempotent); this is safe to re-run."""
    if not rows:
        log.warning("No weather rows to load.")
        return

    records = [
        (
            r["timestamp_utc"],
            r["city"],
            r["state"],
            r["grid_region"],
            r["latitude"],
            r["longitude"],
            r["temperature_c"],
            r["precipitation_mm"],
            r["windspeed_kmh"],
        )
        for r in rows
    ]

    sql = """
        INSERT INTO raw_weather_hourly
            (timestamp_utc, city, state, grid_region, latitude, longitude,
             temperature_c, precipitation_mm, windspeed_kmh)
        VALUES %s
        ON CONFLICT (timestamp_utc, city, state) DO UPDATE SET
            temperature_c    = EXCLUDED.temperature_c,
            precipitation_mm = EXCLUDED.precipitation_mm,
            windspeed_kmh    = EXCLUDED.windspeed_kmh,
            fetched_at       = NOW();
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, records)
    conn.commit()
    log.info(f"Upserted {len(records)} weather rows.")


# ---------------------------------------------------------------------------
# EIA API v2 — grid generation by fuel type
# ---------------------------------------------------------------------------

def fetch_grid_generation_for_respondent(respondent: str, api_key: str) -> list:
    """
    Fetch hourly generation by fuel type for a single EIA respondent.
    Fetches the last day to give a buffer for EIA's typical 1-2 hour data lag
    and to accomodate our pipeline running every few hours (and possibly 
    getting dropped since we're using free Github triggers with no SLO).

    Returns a list of row dicts.
    """
    end_dt    = datetime.now(timezone.utc)
    start_dt  = end_dt - timedelta(days=1)
    start_str = start_dt.strftime("%Y-%m-%dT%H")
    end_str   = end_dt.strftime("%Y-%m-%dT%H")

    url = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
    params = {
        "api_key":                  api_key,
        "frequency":                "hourly",
        "data[0]":                  "value",
        "facets[respondent][]":     respondent,
        "start":                    start_str,
        "end":                      end_str,
        "sort[0][column]":          "period",
        "sort[0][direction]":       "desc",
        # 24 hrs × 9 fuel types = 216 rows, so limiting the return to 500 is ok.
        "length":                   500,
        "offset":                   0,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    records_raw = data.get("response", {}).get("data", [])
    if not records_raw:
        log.warning(f"  {respondent}: EIA returned no data.")
        return []

    rows = []
    for rec in records_raw:
        ts_str = rec.get("period", "")
        try:
            ts_utc = datetime.strptime(ts_str, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
        except ValueError:
            log.warning(f"  {respondent}: could not parse period {ts_str!r}, skipping.")
            continue

        fuel_type  = rec.get("fueltype", "UNK")
        value_str  = rec.get("value")

        try:
            generation_mwh = float(value_str) if value_str is not None else None
        except (ValueError, TypeError):
            generation_mwh = None

        rows.append({
            "timestamp_utc":  ts_utc,
            "respondent":     respondent,
            "fuel_type":      fuel_type,
            "fuel_type_name": FUEL_TYPE_NAMES.get(fuel_type, fuel_type),
            "generation_mwh": generation_mwh,
        })

    return rows


def fetch_all_grid_generation() -> list:
    """
    Fetch grid generation for all three respondents (PJM, ERCO, CISO).
    Each respondent is fetched independently — a failure for one doesn't
    abort the others.
    """
    api_key = os.environ.get("EIA_API_KEY")
    if not api_key:
        raise EnvironmentError("EIA_API_KEY is not set.")

    all_rows = []
    for respondent in EIA_RESPONDENTS:
        try:
            rows = fetch_grid_generation_for_respondent(respondent, api_key)
            all_rows.extend(rows)
            log.info(f"  {respondent}: {len(rows)} rows fetched.")
        except Exception as e:
            log.error(f"  {respondent}: fetch failed — {e}")

    log.info(f"Grid fetch complete: {len(all_rows)} total rows across {len(EIA_RESPONDENTS)} respondents.")
    return all_rows


def load_grid_generation(conn, rows: list):
    """Upsert grid generation rows — idempotent, safe to re-run."""
    if not rows:
        log.warning("No grid generation rows to load.")
        return

    records = [
        (
            r["timestamp_utc"],
            r["respondent"],
            r["fuel_type"],
            r["fuel_type_name"],
            r["generation_mwh"],
        )
        for r in rows
    ]

    sql = """
        INSERT INTO raw_grid_generation
            (timestamp_utc, respondent, fuel_type, fuel_type_name, generation_mwh)
        VALUES %s
        ON CONFLICT (timestamp_utc, respondent, fuel_type) DO UPDATE SET
            fuel_type_name  = EXCLUDED.fuel_type_name,
            generation_mwh  = EXCLUDED.generation_mwh,
            fetched_at      = NOW();
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, records)
    conn.commit()
    log.info(f"Upserted {len(records)} grid generation rows.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=== fetch.py starting ===")

    conn = get_connection()
    try:
        ensure_tables(conn)

        # Weather — all 15 cities, each fails independently
        log.info("--- Fetching weather ---")
        try:
            weather_rows = fetch_all_weather()
            load_weather(conn, weather_rows)
        except Exception as e:
            log.error(f"Weather stage failed: {e}", exc_info=True)

        # EIA grid generation — all 3 respondents, each fails independently
        log.info("--- Fetching grid generation ---")
        try:
            grid_rows = fetch_all_grid_generation()
            load_grid_generation(conn, grid_rows)
        except Exception as e:
            log.error(f"Grid generation stage failed: {e}", exc_info=True)

    finally:
        conn.close()

    log.info("=== fetch.py complete ===")


if __name__ == "__main__":
    main()
