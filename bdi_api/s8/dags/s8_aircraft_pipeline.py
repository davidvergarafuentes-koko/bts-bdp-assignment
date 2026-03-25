"""
S8 Aircraft Data Pipeline DAG

Downloads aircraft tracking data from ADS-B Exchange, enriches with aircraft
metadata, stores in MinIO bronze/silver layers (Parquet), and loads into SQLite.
"""

import csv
import gzip
import io
import json
import os
import sqlite3
from datetime import datetime

import boto3
import pandas as pd
import requests
import s3fs
from airflow import DAG
from airflow.decorators import task

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL")
DAY = "2023-11-01"
FILE_LIMIT = 100

AIRCRAFT_DB_URL = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
FUEL_RATES_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
TRACKING_BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

HEADERS = {"User-Agent": "Mozilla/5.0 bdi-assignment/1.0"}


def _first_n_filenames(n):
    out = []
    for i in range(n):
        total_seconds = i * 5
        hh = total_seconds // 3600
        mm = (total_seconds % 3600) // 60
        ss = total_seconds % 60
        out.append(f"{hh:02d}{mm:02d}{ss:02d}Z.json.gz")
    return out


with DAG(
    dag_id="s8_aircraft_pipeline",
    start_date=datetime(2023, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1},
    tags=["s8", "aircraft"],
) as dag:

    @task()
    def download_tracking_to_bronze():
        """Download tracking JSON files from ADS-B Exchange and store in MinIO bronze."""
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)

        filenames = _first_n_filenames(FILE_LIMIT)
        for fname in filenames:
            url = TRACKING_BASE_URL + fname
            r = requests.get(url, headers=HEADERS, timeout=60, allow_redirects=True)
            r.raise_for_status()
            s3.put_object(
                Bucket=BRONZE_BUCKET,
                Key=f"s8/tracking/day={DAY}/{fname}",
                Body=r.content,
            )

        return f"s8/tracking/day={DAY}/"

    @task()
    def download_aircraft_db_to_bronze():
        """Download OpenSky aircraft database CSV to MinIO bronze."""
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)

        r = requests.get(AIRCRAFT_DB_URL, headers=HEADERS, timeout=120)
        r.raise_for_status()
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key="s8/aircraft_database/aircraftDatabase.csv",
            Body=r.content,
        )

        return "s8/aircraft_database/aircraftDatabase.csv"

    @task()
    def download_fuel_rates_to_bronze():
        """Download fuel consumption rates JSON to MinIO bronze."""
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)

        r = requests.get(FUEL_RATES_URL, headers=HEADERS, timeout=60)
        r.raise_for_status()
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key="s8/fuel_rates/aircraft_type_fuel_consumption_rates.json",
            Body=r.content,
        )

        return "s8/fuel_rates/aircraft_type_fuel_consumption_rates.json"

    @task()
    def process_to_silver(tracking_prefix, aircraft_db_key, fuel_rates_key):
        """Parse tracking data, enrich with aircraft metadata, store as Parquet in silver."""
        fs = s3fs.S3FileSystem(
            endpoint_url=S3_ENDPOINT,
            key="minioadmin",
            secret="minioadmin",
        )
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)

        # 1. Parse tracking data from bronze
        aircraft_dict = {}
        obs_counts = {}

        resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=tracking_prefix)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json.gz"):
                continue

            body = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read()
            try:
                data = json.loads(gzip.decompress(body))
            except Exception:
                data = json.loads(body)

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

        # 2. Load aircraft database CSV for enrichment
        enrichment = {}
        body = s3.get_object(Bucket=BRONZE_BUCKET, Key=aircraft_db_key)["Body"].read()
        content = body.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            icao_hex = row.get("icao24", "").strip().lower()
            if icao_hex and icao_hex in aircraft_dict:
                enrichment[icao_hex] = {
                    "owner": row.get("owner", "").strip() or None,
                    "manufacturer": row.get("manufacturername", "").strip() or None,
                    "model": row.get("model", "").strip() or None,
                }

        # 3. Load fuel rates
        body = s3.get_object(Bucket=BRONZE_BUCKET, Key=fuel_rates_key)["Body"].read()
        fuel_data = json.loads(body)

        # 4. Build enriched aircraft DataFrame
        records = []
        for icao, info in aircraft_dict.items():
            enrich = enrichment.get(icao, {})
            records.append(
                {
                    "icao": icao,
                    "registration": info["registration"],
                    "type": info["type"],
                    "owner": enrich.get("owner"),
                    "manufacturer": enrich.get("manufacturer"),
                    "model": enrich.get("model"),
                }
            )
        df_aircraft = pd.DataFrame(records)

        # 5. Build observation counts DataFrame
        obs_records = [
            {"icao": icao, "day": DAY, "num_observations": count}
            for icao, count in obs_counts.items()
        ]
        df_obs = pd.DataFrame(obs_records)

        # 6. Write to silver as Parquet
        silver_aircraft_key = f"{SILVER_BUCKET}/s8/aircraft_enriched.snappy.parquet"
        with fs.open(silver_aircraft_key, "wb") as f:
            df_aircraft.to_parquet(f, compression="snappy", index=False)

        silver_obs_key = f"{SILVER_BUCKET}/s8/observation_counts.snappy.parquet"
        with fs.open(silver_obs_key, "wb") as f:
            df_obs.to_parquet(f, compression="snappy", index=False)

        # Store fuel rates in silver
        s3.put_object(
            Bucket=SILVER_BUCKET,
            Key="s8/fuel_consumption_rates.json",
            Body=json.dumps(fuel_data),
        )

        return {
            "aircraft_count": len(records),
            "observation_icaos": len(obs_counts),
            "fuel_types": len(fuel_data),
        }

    @task()
    def load_to_database(silver_stats):
        """Load enriched data from silver layer into SQLite database."""
        fs = s3fs.S3FileSystem(
            endpoint_url=S3_ENDPOINT,
            key="minioadmin",
            secret="minioadmin",
        )
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)

        # Read from silver
        with fs.open(f"{SILVER_BUCKET}/s8/aircraft_enriched.snappy.parquet") as f:
            df_aircraft = pd.read_parquet(f)

        with fs.open(f"{SILVER_BUCKET}/s8/observation_counts.snappy.parquet") as f:
            df_obs = pd.read_parquet(f)

        body = s3.get_object(Bucket=SILVER_BUCKET, Key="s8/fuel_consumption_rates.json")[
            "Body"
        ].read()
        fuel_data = json.loads(body)

        # Write to SQLite
        db_path = "/opt/airflow/data/s8/aircraft.db"
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        if os.path.exists(db_path):
            os.remove(db_path)

        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE aircraft_enriched (
                icao TEXT PRIMARY KEY, registration TEXT, type TEXT,
                owner TEXT, manufacturer TEXT, model TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE observation_counts (
                icao TEXT, day TEXT, num_observations INTEGER,
                PRIMARY KEY (icao, day)
            )
        """)
        cur.execute("""
            CREATE TABLE fuel_rates (type TEXT PRIMARY KEY, galph REAL)
        """)

        for _, row in df_aircraft.iterrows():
            cur.execute(
                "INSERT OR REPLACE INTO aircraft_enriched VALUES (?, ?, ?, ?, ?, ?)",
                (
                    row["icao"],
                    row.get("registration"),
                    row.get("type"),
                    row.get("owner"),
                    row.get("manufacturer"),
                    row.get("model"),
                ),
            )

        for _, row in df_obs.iterrows():
            cur.execute(
                "INSERT OR REPLACE INTO observation_counts VALUES (?, ?, ?)",
                (row["icao"], row["day"], int(row["num_observations"])),
            )

        for ac_type, info in fuel_data.items():
            galph = info.get("galph")
            if galph is not None:
                cur.execute(
                    "INSERT OR REPLACE INTO fuel_rates VALUES (?, ?)",
                    (ac_type, float(galph)),
                )

        con.commit()
        con.close()

        return f"Database created at {db_path}"

    # DAG task dependencies
    tracking = download_tracking_to_bronze()
    aircraft_db = download_aircraft_db_to_bronze()
    fuel_rates = download_fuel_rates_to_bronze()
    stats = process_to_silver(tracking, aircraft_db, fuel_rates)
    load_to_database(stats)
