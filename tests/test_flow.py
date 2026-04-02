import json

import app.main as main_module
from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def _login_admin() -> dict[str, str]:
    res = client.post("/api/auth/login", json={"provider": "ldap", "username": "admin"})
    assert res.status_code == 200
    token = res.json()["token"]
    return {"Authorization": f"Bearer {token}"}


def _parse_ndjson_events(response_text: str) -> list[dict]:
    return [json.loads(line) for line in response_text.splitlines() if line.strip()]


def _run_mock_iteration(headers: dict[str, str], message: str = "list all flights") -> tuple[list[dict], str, str]:
    res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": message,
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete_event = next(event for event in events if event["type"] == "iteration_complete")
    return events, complete_event["data"]["session_id"], complete_event["data"]["proposal_id"]


def test_full_iterative_flow():
    headers = _login_admin()

    events, session_id, proposal_id = _run_mock_iteration(headers)

    event_types = [event["type"] for event in events]
    assert "thought" in event_types
    assert "result" in event_types
    assert "data" in event_types
    assert "iteration_complete" in event_types

    res = client.post(
        "/api/skills/save",
        headers=headers,
        json={"proposal_id": proposal_id, "name": "test-skill"},
    )
    assert res.status_code == 200
    assert res.json()["skill"]["name"] == "test-skill"

    res = client.post(
        "/api/chat/feedback",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "session_id": session_id,
            "feedback": "test feedback",
            "is_business_knowledge": True,
        },
    )
    assert res.status_code == 200
    assert res.json()["type"] == "business_knowledge"

    res = client.get(f"/api/chat/history?session_id={session_id}", headers=headers)
    assert res.status_code == 200
    assert len(res.json()["iterations"]) == 1


def test_table_limits():
    headers = _login_admin()

    res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "test",
            "selected_tables": ["t1", "t2", "t3", "t4", "t5", "t6"],
        },
    )
    assert res.status_code == 400
    assert "5" in res.json()["detail"]


def test_table_authorization():
    res = client.post("/api/auth/login", json={"provider": "oauth", "oauth_token": "oauth_marketing_bob"})
    assert res.status_code == 200
    headers = {"Authorization": f"Bearer {res.json()['token']}"}

    res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "test",
            "selected_tables": ["tutorial_flights", "secret_finance_table"],
        },
    )
    assert res.status_code == 403


def test_mount_skill_context_and_sandbox_payload(monkeypatch):
    headers = _login_admin()

    client.post(
        "/api/sandboxes/sb_flights_overview/skills",
        headers=headers,
        json={"skills": []},
    )

    _, session_id, proposal_id = _run_mock_iteration(headers, message="build a reusable skill")

    res = client.post(
        "/api/skills/save",
        headers=headers,
        json={
            "proposal_id": proposal_id,
            "name": "mounted-skill",
            "knowledge": ["rule-a", "rule-a", "rule-b"],
        },
    )
    assert res.status_code == 200
    skill_id = res.json()["skill"]["skill_id"]

    res = client.post(
        "/api/sandboxes/sb_flights_overview/skills",
        headers=headers,
        json={"skills": [skill_id, skill_id, ""]},
    )
    assert res.status_code == 200
    assert res.json()["skills"] == [skill_id]

    res = client.get("/api/sandboxes", headers=headers)
    assert res.status_code == 200
    sandbox = next(item for item in res.json()["sandboxes"] if item["sandbox_id"] == "sb_flights_overview")
    assert "knowledge_bases" in sandbox
    assert "mounted_skills" in sandbox
    assert sandbox["mounted_skills"] == [skill_id]

    captured: dict[str, list[str]] = {}

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        captured["business_knowledge"] = business_knowledge
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "",
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)

    res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "session_id": session_id,
            "message": "verify mounted skill context",
            "provider": "mock",
        },
    )
    assert res.status_code == 200

    business_knowledge = captured["business_knowledge"]
    assert "[mounted-skill]: rule-a" in business_knowledge
    assert "[mounted-skill]: rule-b" in business_knowledge
    assert business_knowledge.count("[mounted-skill]: rule-a") == 1


def test_mount_unknown_skill_returns_400():
    headers = _login_admin()

    res = client.post(
        "/api/sandboxes/sb_flights_overview/skills",
        headers=headers,
        json={"skills": ["sk_missing"]},
    )
    assert res.status_code == 400
    assert "Skills not found" in res.json()["detail"]
