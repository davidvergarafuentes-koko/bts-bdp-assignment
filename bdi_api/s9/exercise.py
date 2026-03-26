from datetime import datetime

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

s9 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s9",
    tags=["s9"],
)


class PipelineRun(BaseModel):
    id: str
    repository: str
    branch: str
    status: str
    triggered_by: str
    started_at: datetime
    finished_at: datetime | None
    stages: list[str]


class PipelineStage(BaseModel):
    name: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    logs_url: str


PIPELINES: list[dict] = [
    {
        "id": "run-001",
        "repository": "bts-bdp-assignment",
        "branch": "main",
        "status": "success",
        "triggered_by": "push",
        "started_at": "2026-03-10T10:00:00Z",
        "finished_at": "2026-03-10T10:05:30Z",
        "stages": ["lint", "test", "build"],
    },
    {
        "id": "run-002",
        "repository": "bts-bdp-assignment",
        "branch": "feat/add-endpoint",
        "status": "failure",
        "triggered_by": "pull_request",
        "started_at": "2026-03-09T14:00:00Z",
        "finished_at": "2026-03-09T14:03:00Z",
        "stages": ["lint", "test"],
    },
    {
        "id": "run-003",
        "repository": "bts-bdp-assignment",
        "branch": "main",
        "status": "success",
        "triggered_by": "push",
        "started_at": "2026-03-08T09:00:00Z",
        "finished_at": "2026-03-08T09:04:15Z",
        "stages": ["lint", "test", "build"],
    },
    {
        "id": "run-004",
        "repository": "bts-bdp-exercises",
        "branch": "main",
        "status": "success",
        "triggered_by": "push",
        "started_at": "2026-03-07T16:00:00Z",
        "finished_at": "2026-03-07T16:06:00Z",
        "stages": ["lint", "test", "build", "deploy"],
    },
    {
        "id": "run-005",
        "repository": "bts-bdp-assignment",
        "branch": "feat/ci-pipeline",
        "status": "running",
        "triggered_by": "pull_request",
        "started_at": "2026-03-06T11:30:00Z",
        "finished_at": None,
        "stages": ["lint", "test", "build"],
    },
    {
        "id": "run-006",
        "repository": "bts-bdp-assignment",
        "branch": "main",
        "status": "success",
        "triggered_by": "schedule",
        "started_at": "2026-03-05T08:00:00Z",
        "finished_at": "2026-03-05T08:05:00Z",
        "stages": ["lint", "test", "build"],
    },
    {
        "id": "run-007",
        "repository": "bts-bdp-exercises",
        "branch": "feat/docker",
        "status": "pending",
        "triggered_by": "manual",
        "started_at": "2026-03-04T12:00:00Z",
        "finished_at": None,
        "stages": ["lint", "test"],
    },
]

STAGES: dict[str, list[dict]] = {
    "run-001": [
        {
            "name": "lint",
            "status": "success",
            "started_at": "2026-03-10T10:00:00Z",
            "finished_at": "2026-03-10T10:00:45Z",
            "logs_url": "/api/s9/pipelines/run-001/stages/lint/logs",
        },
        {
            "name": "test",
            "status": "success",
            "started_at": "2026-03-10T10:00:45Z",
            "finished_at": "2026-03-10T10:03:20Z",
            "logs_url": "/api/s9/pipelines/run-001/stages/test/logs",
        },
        {
            "name": "build",
            "status": "success",
            "started_at": "2026-03-10T10:03:20Z",
            "finished_at": "2026-03-10T10:05:30Z",
            "logs_url": "/api/s9/pipelines/run-001/stages/build/logs",
        },
    ],
    "run-002": [
        {
            "name": "lint",
            "status": "success",
            "started_at": "2026-03-09T14:00:00Z",
            "finished_at": "2026-03-09T14:00:30Z",
            "logs_url": "/api/s9/pipelines/run-002/stages/lint/logs",
        },
        {
            "name": "test",
            "status": "failure",
            "started_at": "2026-03-09T14:00:30Z",
            "finished_at": "2026-03-09T14:03:00Z",
            "logs_url": "/api/s9/pipelines/run-002/stages/test/logs",
        },
    ],
    "run-003": [
        {
            "name": "lint",
            "status": "success",
            "started_at": "2026-03-08T09:00:00Z",
            "finished_at": "2026-03-08T09:00:40Z",
            "logs_url": "/api/s9/pipelines/run-003/stages/lint/logs",
        },
        {
            "name": "test",
            "status": "success",
            "started_at": "2026-03-08T09:00:40Z",
            "finished_at": "2026-03-08T09:02:50Z",
            "logs_url": "/api/s9/pipelines/run-003/stages/test/logs",
        },
        {
            "name": "build",
            "status": "success",
            "started_at": "2026-03-08T09:02:50Z",
            "finished_at": "2026-03-08T09:04:15Z",
            "logs_url": "/api/s9/pipelines/run-003/stages/build/logs",
        },
    ],
    "run-004": [
        {
            "name": "lint",
            "status": "success",
            "started_at": "2026-03-07T16:00:00Z",
            "finished_at": "2026-03-07T16:00:50Z",
            "logs_url": "/api/s9/pipelines/run-004/stages/lint/logs",
        },
        {
            "name": "test",
            "status": "success",
            "started_at": "2026-03-07T16:00:50Z",
            "finished_at": "2026-03-07T16:03:30Z",
            "logs_url": "/api/s9/pipelines/run-004/stages/test/logs",
        },
        {
            "name": "build",
            "status": "success",
            "started_at": "2026-03-07T16:03:30Z",
            "finished_at": "2026-03-07T16:05:10Z",
            "logs_url": "/api/s9/pipelines/run-004/stages/build/logs",
        },
        {
            "name": "deploy",
            "status": "success",
            "started_at": "2026-03-07T16:05:10Z",
            "finished_at": "2026-03-07T16:06:00Z",
            "logs_url": "/api/s9/pipelines/run-004/stages/deploy/logs",
        },
    ],
    "run-005": [
        {
            "name": "lint",
            "status": "success",
            "started_at": "2026-03-06T11:30:00Z",
            "finished_at": "2026-03-06T11:30:40Z",
            "logs_url": "/api/s9/pipelines/run-005/stages/lint/logs",
        },
        {
            "name": "test",
            "status": "running",
            "started_at": "2026-03-06T11:30:40Z",
            "finished_at": None,
            "logs_url": "/api/s9/pipelines/run-005/stages/test/logs",
        },
        {
            "name": "build",
            "status": "pending",
            "started_at": "2026-03-06T11:30:40Z",
            "finished_at": None,
            "logs_url": "/api/s9/pipelines/run-005/stages/build/logs",
        },
    ],
    "run-006": [
        {
            "name": "lint",
            "status": "success",
            "started_at": "2026-03-05T08:00:00Z",
            "finished_at": "2026-03-05T08:00:35Z",
            "logs_url": "/api/s9/pipelines/run-006/stages/lint/logs",
        },
        {
            "name": "test",
            "status": "success",
            "started_at": "2026-03-05T08:00:35Z",
            "finished_at": "2026-03-05T08:03:10Z",
            "logs_url": "/api/s9/pipelines/run-006/stages/test/logs",
        },
        {
            "name": "build",
            "status": "success",
            "started_at": "2026-03-05T08:03:10Z",
            "finished_at": "2026-03-05T08:05:00Z",
            "logs_url": "/api/s9/pipelines/run-006/stages/build/logs",
        },
    ],
    "run-007": [
        {
            "name": "lint",
            "status": "pending",
            "started_at": "2026-03-04T12:00:00Z",
            "finished_at": None,
            "logs_url": "/api/s9/pipelines/run-007/stages/lint/logs",
        },
        {
            "name": "test",
            "status": "pending",
            "started_at": "2026-03-04T12:00:00Z",
            "finished_at": None,
            "logs_url": "/api/s9/pipelines/run-007/stages/test/logs",
        },
    ],
}


@s9.get("/pipelines")
def list_pipelines(
    repository: str | None = None,
    status_filter: str | None = None,
    num_results: int = 100,
    page: int = 0,
) -> list[PipelineRun]:
    """List CI/CD pipeline runs with their status.

    Returns a list of pipeline runs, optionally filtered by repository and status.
    Ordered by started_at descending (most recent first).
    Paginated with `num_results` per page and `page` number (0-indexed).

    Valid statuses: "success", "failure", "running", "pending"
    Valid triggered_by values: "push", "pull_request", "schedule", "manual"
    """
    results = PIPELINES

    if repository:
        results = [p for p in results if p["repository"] == repository]

    if status_filter:
        results = [p for p in results if p["status"] == status_filter]

    results = sorted(results, key=lambda p: p["started_at"], reverse=True)

    start = page * num_results
    end = start + num_results
    return results[start:end]


@s9.get("/pipelines/{pipeline_id}/stages")
def get_pipeline_stages(pipeline_id: str) -> list[PipelineStage]:
    """Get the stages of a specific pipeline run.

    Returns the stages in execution order.
    Each stage has a name, status, timestamps, and a logs URL.

    Typical stages: "lint", "test", "build", "deploy"
    """
    if pipeline_id not in STAGES:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return STAGES[pipeline_id]
