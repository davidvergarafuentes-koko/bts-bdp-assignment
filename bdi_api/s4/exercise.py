from __future__ import annotations

import shutil
from pathlib import Path
from typing import Annotated

import boto3
import requests
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)

DAY = "20231101"
S3_PREFIX = "raw/day=20231101/"  # required by homework


def _ensure_clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _first_n_filenames(file_limit: int) -> list[str]:
    # 5-second increments from 00:00:00Z
    out: list[str] = []
    for i in range(int(file_limit)):
        total_seconds = i * 5
        hh = total_seconds // 3600
        mm = (total_seconds % 3600) // 60
        ss = total_seconds % 60
        out.append(f"{hh:02d}{mm:02d}{ss:02d}Z.json.gz")
    return out


def _day_url() -> str:
    """
    Build base URL robustly, regardless of whether Settings().source_url
    includes /readsb-hist already.
    """
    base = settings.source_url.rstrip("/")
    if base.endswith("/readsb-hist"):
        return base + "/2023/11/01/"
    return base + "/readsb-hist/2023/11/01/"


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
Limits the number of files to download.
You must always start from the first file and go in ascending order.
I'll test with increasing number of files starting from 100.
""",
        ),
    ] = 100,
) -> str:
    """
    Downloads aircraft data files from ADS-B Exchange and stores them in S3
    under raw/day=20231101/
    """
    s3 = boto3.client("s3")
    bucket = settings.s3_bucket
    day_url = _day_url()

    headers = {"User-Agent": "Mozilla/5.0 bdi-assignment/1.0"}

    for fname in _first_n_filenames(file_limit):
        url = day_url + fname
        r = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
        r.raise_for_status()

        key = S3_PREFIX + fname
        s3.put_object(Bucket=bucket, Key=key, Body=r.content)

    return "OK"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """
    Reads the raw files from S3 and stores them locally (raw/),
    then prepares them locally (prepared/) in the same way as S1,
    so S1 query endpoints keep working.
    """
    s3 = boto3.client("s3")
    bucket = settings.s3_bucket

    # 1) Download raw files from S3 to local raw/day=20231101/
    local_raw_day_dir = Path(settings.raw_dir) / f"day={DAY}"
    _ensure_clean_dir(local_raw_day_dir)

    keys: list[str] = []
    token: str | None = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": S3_PREFIX}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/") or not key.endswith(".json.gz"):
                continue
            keys.append(key)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    keys.sort()  # ascending

    for key in keys:
        fname = key.split("/")[-1]
        s3.download_file(bucket, key, str(local_raw_day_dir / fname))

    # 2) Reuse S1 prepare to generate the local prepared DB
    from bdi_api.s1.exercise import prepare_data as s1_prepare

    return s1_prepare()
