import os
from fastapi.testclient import TestClient

from bdi_api.app import app

client = TestClient(app)

def test_s4_download_returns_ok():
    # Ensure env var is set when running tests
    assert os.getenv("BDI_S3_BUCKET"), "BDI_S3_BUCKET must be set for S4 tests"

    r = client.post("/api/s4/aircraft/download", params={"file_limit": 2})
    assert r.status_code == 200
    assert r.json() == "OK"

def test_s4_prepare_makes_s1_queries_work():
    assert os.getenv("BDI_S3_BUCKET"), "BDI_S3_BUCKET must be set for S4 tests"

    # Prepare from S3 into local prepared/
    r = client.post("/api/s4/aircraft/prepare")
    assert r.status_code == 200
    assert r.json() == "OK"

    # Now S1 endpoint should return data
    r2 = client.get("/api/s1/aircraft/", params={"num_results": 5, "page": 0})
    assert r2.status_code == 200
    assert isinstance(r2.json(), list)