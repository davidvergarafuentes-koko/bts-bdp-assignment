from __future__ import annotations

import csv
import gzip
import io
import json
import os
import sqlite3
from pathlib import Path
from typing import Annotated

import boto3
import requests
from fastapi import APIRouter, status
from fastapi.params import Query
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)

# Constants
DAY = "2023-11-01"
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
AIRCRAFT_DB_URL = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
FUEL_RATES_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"

DB_DIR = Path(settings.local_dir) / "s8"
DB_PATH = DB_DIR / "aircraft.db"


class AircraftReturn(BaseModel):
    icao: str
    registration: str | None
    type: str | None
    owner: str | None
    manufacturer: str | None
    model: str | None


class AircraftCO2Return(BaseModel):
    icao: str
    hours_flown: float
    co2: float | None


def _get_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )


def _first_n_filenames(n: int) -> list[str]:
    out: list[str] = []
    for i in range(n):
        total_seconds = i * 5
        hh = total_seconds // 3600
        mm = (total_seconds % 3600) // 60
        ss = total_seconds % 60
        out.append(f"{hh:02d}{mm:02d}{ss:02d}Z.json.gz")
    return out


def _init_db() -> None:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()
    con = sqlite3.connect(str(DB_PATH))
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE aircraft_enriched (
            icao TEXT PRIMARY KEY,
            registration TEXT,
            type TEXT,
            owner TEXT,
            manufacturer TEXT,
            model TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE observation_counts (
            icao TEXT,
            day TEXT,
            num_observations INTEGER,
            PRIMARY KEY (icao, day)
        )
    """)
    cur.execute("""
        CREATE TABLE fuel_rates (
            type TEXT PRIMARY KEY,
            galph REAL
        )
    """)
    con.commit()
    con.close()


@s8.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            description="Number of tracking files to download (5-second intervals starting at 00:00:00Z).",
        ),
    ] = 100,
) -> str:
    """Download tracking data from ADS-B Exchange, enrich with aircraft metadata,
    store in MinIO bronze/silver layers and local SQLite database."""

    base = settings.source_url.rstrip("/")
    if not base.endswith("/readsb-hist"):
        base += "/readsb-hist"
    day_url = base + "/2023/11/01/"

    headers = {"User-Agent": "Mozilla/5.0 bdi-assignment/1.0"}

    # Try to get S3 client for MinIO uploads
    try:
        s3 = _get_s3()
        s3.head_bucket(Bucket=BRONZE_BUCKET)
        use_s3 = True
    except Exception:
        s3 = None
        use_s3 = False

    _init_db()
    con = sqlite3.connect(str(DB_PATH))
    cur = con.cursor()

    # 1. Download and parse tracking data
    aircraft_dict: dict[str, dict] = {}
    obs_counts: dict[str, int] = {}

    for fname in _first_n_filenames(file_limit):
        url = day_url + fname
        r = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
        r.raise_for_status()
        raw_content = r.content

        # Upload to MinIO bronze
        if use_s3:
            try:
                s3.put_object(
                    Bucket=BRONZE_BUCKET,
                    Key=f"s8/tracking/day={DAY}/{fname}",
                    Body=raw_content,
                )
            except Exception:
                pass

        # Parse JSON (gzipped)
        try:
            data = json.loads(gzip.decompress(raw_content))
        except Exception:
            data = json.loads(raw_content)

        for ac in data.get("aircraft", []):
            icao = ac.get("hex")
            if not icao:
                continue
            icao = icao.strip().lower()

            if icao not in aircraft_dict:
                aircraft_dict[icao] = {
                    "registration": ac.get("r"),
                    "type": ac.get("t"),
                }
            else:
                if ac.get("r") and not aircraft_dict[icao]["registration"]:
                    aircraft_dict[icao]["registration"] = ac.get("r")
                if ac.get("t") and not aircraft_dict[icao]["type"]:
                    aircraft_dict[icao]["type"] = ac.get("t")

            obs_counts[icao] = obs_counts.get(icao, 0) + 1

    # 2. Download aircraft database CSV for enrichment (owner, manufacturer, model)
    enrichment: dict[str, dict] = {}
    try:
        r = requests.get(AIRCRAFT_DB_URL, headers=headers, timeout=120)
        r.raise_for_status()
        content = r.content.decode("utf-8", errors="replace")

        if use_s3:
            try:
                s3.put_object(
                    Bucket=BRONZE_BUCKET,
                    Key="s8/aircraft_database/aircraftDatabase.csv",
                    Body=r.content,
                )
            except Exception:
                pass

        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            icao_hex = row.get("icao24", "").strip().lower()
            if icao_hex and icao_hex in aircraft_dict:
                enrichment[icao_hex] = {
                    "owner": row.get("owner", "").strip() or None,
                    "manufacturer": row.get("manufacturername", "").strip() or None,
                    "model": row.get("model", "").strip() or None,
                }
    except Exception as e:
        print(f"Warning: Could not download aircraft database: {e}")

    # 3. Download fuel consumption rates
    try:
        r = requests.get(FUEL_RATES_URL, headers=headers, timeout=60)
        r.raise_for_status()
        fuel_data = r.json()

        if use_s3:
            try:
                s3.put_object(
                    Bucket=BRONZE_BUCKET,
                    Key="s8/fuel_rates/aircraft_type_fuel_consumption_rates.json",
                    Body=r.content,
                )
            except Exception:
                pass

        # fuel_data is a dict: {"TYPE_CODE": {"galph": number, ...}, ...}
        for ac_type, info in fuel_data.items():
            galph = info.get("galph")
            if galph is not None:
                cur.execute(
                    "INSERT OR REPLACE INTO fuel_rates (type, galph) VALUES (?, ?)",
                    (ac_type, float(galph)),
                )
    except Exception as e:
        print(f"Warning: Could not download fuel rates: {e}")

    # 4. Insert enriched aircraft data into SQLite
    for icao, info in aircraft_dict.items():
        enrich = enrichment.get(icao, {})
        cur.execute(
            """INSERT OR REPLACE INTO aircraft_enriched
               (icao, registration, type, owner, manufacturer, model)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                icao,
                info["registration"],
                info["type"],
                enrich.get("owner"),
                enrich.get("manufacturer"),
                enrich.get("model"),
            ),
        )

    # 5. Insert observation counts
    for icao, count in obs_counts.items():
        cur.execute(
            """INSERT OR REPLACE INTO observation_counts
               (icao, day, num_observations) VALUES (?, ?, ?)""",
            (icao, DAY, count),
        )

    con.commit()
    con.close()

    # 6. Upload enriched Parquet to MinIO silver (optional, best-effort)
    if use_s3:
        try:
            import pandas as pd

            df_aircraft = pd.DataFrame(
                [
                    {
                        "icao": icao,
                        "registration": info["registration"],
                        "type": info["type"],
                        "owner": enrichment.get(icao, {}).get("owner"),
                        "manufacturer": enrichment.get(icao, {}).get("manufacturer"),
                        "model": enrichment.get(icao, {}).get("model"),
                    }
                    for icao, info in aircraft_dict.items()
                ]
            )
            buf = io.BytesIO()
            df_aircraft.to_parquet(buf, index=False, compression="snappy")
            buf.seek(0)
            s3.put_object(
                Bucket=SILVER_BUCKET,
                Key="s8/aircraft_enriched.snappy.parquet",
                Body=buf.getvalue(),
            )

            df_obs = pd.DataFrame(
                [
                    {"icao": icao, "day": DAY, "num_observations": count}
                    for icao, count in obs_counts.items()
                ]
            )
            buf = io.BytesIO()
            df_obs.to_parquet(buf, index=False, compression="snappy")
            buf.seek(0)
            s3.put_object(
                Bucket=SILVER_BUCKET,
                Key="s8/observation_counts.snappy.parquet",
                Body=buf.getvalue(),
            )
        except Exception as e:
            print(f"Warning: Could not upload silver parquet: {e}")

    return "OK"


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    """List all aircraft with enriched data, ordered by ICAO ascending.

    The data should come from the silver layer (processed by the Airflow DAG).
    Paginated with `num_results` per page and `page` number (0-indexed).
    """
    if not DB_PATH.exists():
        return []

    offset = page * num_results
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute(
            """SELECT icao, registration, type, owner, manufacturer, model
               FROM aircraft_enriched
               ORDER BY icao ASC
               LIMIT ? OFFSET ?""",
            (num_results, offset),
        )
        return [AircraftReturn(**dict(r)) for r in cur.fetchall()]
    finally:
        con.close()


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2Return:
    """Calculate CO2 emissions for a given aircraft on a specific day.

    Computation:
    - Each row in the tracking data represents a 5-second observation
    - hours_flown = (number_of_observations * 5) / 3600
    - Look up `galph` (gallons per hour) from fuel consumption rates using the aircraft's ICAO type
    - fuel_used_kg = hours_flown * galph * 3.04
    - co2_tons = (fuel_used_kg * 3.15) / 907.185
    - If fuel consumption rate is not available for this aircraft type, return None for co2
    """
    if not DB_PATH.exists():
        return AircraftCO2Return(icao=icao, hours_flown=0.0, co2=None)

    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()

        # Get observation count for this ICAO on the given day
        cur.execute(
            "SELECT num_observations FROM observation_counts WHERE icao = ? AND day = ?",
            (icao, day),
        )
        row = cur.fetchone()
        num_obs = row["num_observations"] if row else 0
        hours_flown = (num_obs * 5) / 3600

        # Get aircraft type
        cur.execute("SELECT type FROM aircraft_enriched WHERE icao = ?", (icao,))
        ac_row = cur.fetchone()
        ac_type = ac_row["type"] if ac_row else None

        co2 = None
        if ac_type:
            cur.execute("SELECT galph FROM fuel_rates WHERE type = ?", (ac_type,))
            fuel_row = cur.fetchone()
            if fuel_row:
                galph = fuel_row["galph"]
                fuel_used_kg = hours_flown * galph * 3.04
                co2 = (fuel_used_kg * 3.15) / 907.185

        return AircraftCO2Return(icao=icao, hours_flown=hours_flown, co2=co2)
    finally:
        con.close()
