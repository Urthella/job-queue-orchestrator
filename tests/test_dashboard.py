"""Dashboard REST API smoke tests via FastAPI TestClient."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


@pytest.fixture()
def client(tmp_path):
    from fastapi.testclient import TestClient

    from jobq.dashboard import create_app

    url = "sqlite:///" + str(tmp_path / "dash.db").replace("\\", "/")
    app = create_app(url)
    with TestClient(app) as c:
        yield c


def test_html_served(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "jobq dashboard" in r.text
    assert "text/html" in r.headers["content-type"]


def test_enqueue_and_list_and_detail(client):
    r = client.post("/api/jobs", json={"task": "add", "args": [2, 3]})
    assert r.status_code == 200
    jid = r.json()["id"]

    r = client.get("/api/jobs")
    assert r.status_code == 200
    ids = [j["id"] for j in r.json()]
    assert jid in ids

    r = client.get(f"/api/jobs/{jid}")
    assert r.status_code == 200
    assert r.json()["task"] == "add"
    assert r.json()["status"] == "queued"

    r = client.get("/api/stats")
    assert r.json()["counts"]["queued"] >= 1


def test_submit_workflow_and_detail(client):
    body = {
        "name": "diamond",
        "steps": [
            {"name": "A", "task": "gen_number", "args": [10], "depends_on": []},
            {"name": "B", "task": "add_const", "kwargs": {"c": 5}, "depends_on": ["A"]},
            {"name": "C", "task": "mul_const", "kwargs": {"m": 3}, "depends_on": ["A"]},
            {"name": "D", "task": "sum_upstream", "depends_on": ["B", "C"]},
        ],
    }
    r = client.post("/api/workflows", json=body)
    assert r.status_code == 200
    run_id = r.json()["run_id"]

    r = client.get(f"/api/workflows/{run_id}")
    assert r.status_code == 200
    detail = r.json()
    assert detail["name"] == "diamond"
    assert len(detail["steps"]) == 4


def test_cyclic_workflow_rejected(client):
    body = {
        "name": "bad",
        "steps": [
            {"name": "A", "task": "add", "depends_on": ["B"]},
            {"name": "B", "task": "add", "depends_on": ["A"]},
        ],
    }
    r = client.post("/api/workflows", json=body)
    assert r.status_code == 400


def test_retry_nonexistent_dead_job_400(client):
    r = client.post("/api/jobs/99999/retry")
    assert r.status_code == 400
