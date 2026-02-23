from __future__ import annotations

import shutil
from pathlib import Path
from typing import Annotated

import requests
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

#2nd endpoint
import json
import sqlite3

#3rd endpoint
from fastapi import HTTPException

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)

#Constants (Global variables)
DAY = "20231101"

#Helper Functions
def _ensure_clean_dir(path: Path) -> None:
    """Delete folder if exists, then recreate empty."""
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _first_n_filenames(file_limit: int) -> list[str]:
    """
    Generates the first N filenames in ascending order, starting at 00:00:00Z
    with 5-second increments:
      000000Z.json.gz, 000005Z.json.gz, 000010Z.json.gz, ...
    """
    out: list[str] = []
    for i in range(int(file_limit)):
        total_seconds = i * 5
        hh = total_seconds // 3600
        mm = (total_seconds % 3600) // 60
        ss = total_seconds % 60
        out.append(f"{hh:02d}{mm:02d}{ss:02d}Z.json.gz")
    return out

def _init_db(db_path: Path) -> None:
    if db_path.exists():
        db_path.unlink()

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE aircraft (
            icao TEXT PRIMARY KEY,
            registration TEXT,
            type TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            icao TEXT,
            timestamp REAL,
            lat REAL,
            lon REAL,
            altitude_baro REAL,
            ground_speed REAL,
            had_emergency INTEGER
        );
    """)

    cur.execute("CREATE INDEX idx_positions_icao_ts ON positions(icao, timestamp);")

    con.commit()
    con.close()

def _get_db_path() -> Path:
    return Path(settings.prepared_dir) / f"day={DAY}" / "aircraft.sqlite"

@s1.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
Limits the number of files to download.
You must always start from the first and go in ascending order.
I'll test with increasing number of files starting from 100.
""",
        ),
    ] = 100,
) -> str:
    raw_day_dir = Path(settings.raw_dir) / f"day={DAY}"
    _ensure_clean_dir(raw_day_dir)

    # Build the correct base URL (avoid double readsb-hist)
    base = settings.source_url.rstrip("/")
    day_url = base + "/2023/11/01/"
    
    headers = {"User-Agent": "Mozilla/5.0 bdi-assignment/1.0"}

    for fname in _first_n_filenames(file_limit):
        url = day_url + fname
        r = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
        r.raise_for_status()
        (raw_day_dir / fname).write_bytes(r.content)

    return "OK"



@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """
    Prepare the raw gz files into a single SQLite database.

    Output:
      <prepared_dir>/day=20231101/aircraft.sqlite
    """
    raw_day_dir = Path(settings.raw_dir) / f"day={DAY}"
    prepared_day_dir = Path(settings.prepared_dir) / f"day={DAY}"

    #Clean prepared folder
    _ensure_clean_dir(prepared_day_dir)

    raw_files = sorted(raw_day_dir.glob("*.json.gz"))
    if not raw_files:
        raise RuntimeError("No raw files found. Run download first.")

    #Initialize DB
    db_path = prepared_day_dir / "aircraft.sqlite"
    _init_db(db_path)

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    #Process each file
    for fp in raw_files:
        with open(fp, "r", encoding="utf-8") as f:
            payload = json.load(f)

        timestamp = payload.get("now", 0.0)

        for a in payload.get("aircraft", []):
            icao = a.get("hex")
            if not icao:
                continue

            registration = a.get("r")
            ac_type = a.get("t")

            lat = a.get("lat")
            lon = a.get("lon")
            alt = a.get("alt_baro")
            gs = a.get("gs")

            emergency = 1 if a.get("emergency") else 0

            # Insert / update aircraft
            cur.execute("""
                INSERT INTO aircraft (icao, registration, type)
                VALUES (?, ?, ?)
                ON CONFLICT(icao) DO UPDATE SET
                    registration = excluded.registration,
                    type = excluded.type;
            """, (icao, registration, ac_type))

            # Insert position only if coordinates exist
            if lat is not None and lon is not None:
                cur.execute("""
                    INSERT INTO positions
                    (icao, timestamp, lat, lon, altitude_baro, ground_speed, had_emergency)
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                """, (icao, timestamp, lat, lon, alt, gs, emergency))

    con.commit()
    con.close()

    return "OK"


@s1.get("/aircraft/")
def list_aircraft(
    num_results: Annotated[int, Query(ge=1, le=1000)] = 100,
    page: Annotated[int, Query(ge=0)] = 0
) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
     Pagination:
      - page=0 => first page
      - page=1 => second page
      - offset = page * num_results
    """
    db_path = _get_db_path()
    if not db_path.exists():
        raise HTTPException(
            status_code=400,
            detail="Prepared database not found. Run /api/s1/aircraft/prepare first.",
        )

    offset = page * num_results

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT icao, registration, type
            FROM aircraft
            ORDER BY icao ASC
            LIMIT ? OFFSET ?;
            """,
            (num_results, offset),
        )
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    # TODO implement and return a list with dictionaries with those values.
    return [{"timestamp": 1609275898.6, "lat": 30.404617, "lon": -86.476566}]


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    # TODO Gather and return the correct statistics for the requested aircraft
    return {"max_altitude_baro": 300000, "max_ground_speed": 493, "had_emergency": False}