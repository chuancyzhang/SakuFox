import json

import app.main as main_module
import pandas as pd
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


def test_iterate_uses_post_execution_synthesis(monkeypatch):
    headers = _login_admin()
    captured = {"saw_rows": False}

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [
                    {
                        "tool": "sql",
                        "code": "SELECT department, SUM(cost) AS total_cost FROM tutorial_flights GROUP BY department ORDER BY total_cost DESC LIMIT 3",
                    }
                ],
                "tools_used": ["execute_select_sql"],
                "conclusions": [{"text": "planner conclusion should be replaced", "confidence": 0.2}],
                "hypotheses": [],
                "action_items": [],
                "direct_answer": "planner answer should be replaced",
                "explanation": "planner only",
                "goal": "find highest cost department",
                "observation_focus": "",
                "continue_reason": "",
                "stop_if": "",
                "finalize": False,
            },
        }

    def fake_synthesize_iteration_result(*, message, sandbox, iteration_history, business_knowledge, planned_result, execution_result, incremental=True, provider=None, model=None):
        rows = execution_result.get("rows") or []
        assert rows
        captured["saw_rows"] = True
        top_department = rows[0]["department"]
        return {
            "steps": [],
            "tools_used": [],
            "conclusions": [{"text": f"{top_department} 成本最高", "confidence": 1.0}],
            "hypotheses": [],
            "action_items": [f"继续分析 {top_department} 的成本驱动因素"],
            "direct_answer": f"成本最高的部门是 {top_department}",
            "explanation": "post execution synthesis",
            "final_report_outline": [],
            "direct_report": "",
            "goal": planned_result.get("goal", ""),
            "observation_focus": "",
            "continue_reason": "",
            "stop_if": "",
            "finalize": True,
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(main_module, "synthesize_iteration_result", fake_synthesize_iteration_result)

    res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "哪个部门成本最高",
            "provider": "openai",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    result_events = [event for event in events if event["type"] == "result"]
    assert result_events
    final_result = result_events[-1]["data"]
    assert captured["saw_rows"] is True
    assert "planner answer should be replaced" not in json.dumps(final_result, ensure_ascii=False)
    assert "成本最高的部门是" in final_result["direct_answer"]
    assert final_result["conclusions"][0]["text"].endswith("成本最高")


def test_auto_analyze_stops_on_repeated_warning_loop(monkeypatch):
    headers = _login_admin()
    call_state = {"count": 0}

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        call_state["count"] += 1
        yield {
            "type": "result",
            "data": {
                "steps": [{"tool": "python", "code": "final_df = df0[['avg_cost']]"}],
                "tools_used": ["python_interpreter"],
                "conclusions": [],
                "hypotheses": [],
                "action_items": [],
                "direct_answer": "",
                "explanation": "planner",
                "goal": "inspect avg_cost",
                "observation_focus": "",
                "continue_reason": "",
                "stop_if": "",
                "finalize": False,
            },
        }

    def fake_execute_analysis_steps(*, result_data, sandbox, selected_tables, selected_files, sandbox_id, session_id):
        return {
            "rows": [],
            "tables": ["tutorial_flights"],
            "chart_specs": [],
            "step_results": [
                {
                    "rows": [],
                    "tables": ["tutorial_flights"],
                    "warning": "字段 avg_cost 不存在，当前仅有 department, destination_region, trip_count, total_cost",
                }
            ],
            "warnings": ["字段 avg_cost 不存在，当前仅有 department, destination_region, trip_count, total_cost"],
        }

    def fake_synthesize_iteration_result(*, message, sandbox, iteration_history, business_knowledge, planned_result, execution_result, incremental=True, provider=None, model=None):
        return {
            "steps": [],
            "tools_used": [],
            "conclusions": [{"text": "Python 分析访问 avg_cost 字段失败", "confidence": 0.9}],
            "hypotheses": [],
            "action_items": ["修正字段映射"],
            "direct_answer": "当前步骤失败，需要修正字段映射",
            "explanation": "warning reflected",
            "final_report_outline": [],
            "direct_report": "",
            "goal": planned_result.get("goal", ""),
            "observation_focus": "",
            "continue_reason": "",
            "stop_if": "",
            "finalize": False,
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(main_module, "_execute_analysis_steps", fake_execute_analysis_steps)
    monkeypatch.setattr(main_module, "synthesize_iteration_result", fake_synthesize_iteration_result)

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "自动分析成本问题",
            "provider": "openai",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete_event = next(event for event in events if event["type"] == "analysis_complete")
    assert complete_event["data"]["stop_reason"] == "repeated_warning_loop"
    assert call_state["count"] == 2


def test_auto_analyze_converges_on_repeated_topic(monkeypatch):
    headers = _login_admin()
    call_state = {"count": 0}

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        call_state["count"] += 1
        yield {
            "type": "result",
            "data": {
                "steps": [
                    {
                        "tool": "sql",
                        "code": "SELECT route, SUM(cost) AS total_cost FROM tutorial_flights GROUP BY route ORDER BY total_cost DESC LIMIT 8",
                    }
                ],
                "tools_used": ["execute_select_sql"],
                "conclusions": [],
                "hypotheses": [],
                "action_items": [],
                "direct_answer": "",
                "explanation": "planner",
                "goal": "review high-cost routes",
                "observation_focus": "",
                "continue_reason": "",
                "stop_if": "",
                "finalize": False,
            },
        }

    def fake_execute_analysis_steps(*, result_data, sandbox, selected_tables, selected_files, sandbox_id, session_id):
        return {
            "rows": [{"route": "A-B", "total_cost": 1000 + call_state["count"]}],
            "tables": ["tutorial_flights"],
            "chart_specs": [],
            "step_results": [{"rows": [{"route": "A-B", "total_cost": 1000 + call_state["count"]}], "tables": ["tutorial_flights"]}],
        }

    def fake_synthesize_iteration_result(*, message, sandbox, iteration_history, business_knowledge, planned_result, execution_result, incremental=True, provider=None, model=None):
        return {
            "steps": [],
            "tools_used": [],
            "conclusions": [{"text": f"销售部门TOP8航线成本对比确认，总成本约 {1000 + call_state['count']} 元", "confidence": 0.95}],
            "hypotheses": [{"id": "h1", "text": "继续检查相同航线成本结构"}],
            "action_items": ["继续审查这些航线"],
            "direct_answer": "TOP8航线成本结构已确认",
            "explanation": "same topic",
            "final_report_outline": [],
            "direct_report": "",
            "goal": planned_result.get("goal", ""),
            "observation_focus": "",
            "continue_reason": "",
            "stop_if": "",
            "finalize": False,
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(main_module, "_execute_analysis_steps", fake_execute_analysis_steps)
    monkeypatch.setattr(main_module, "synthesize_iteration_result", fake_synthesize_iteration_result)

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "自动分析成本问题",
            "provider": "openai",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete_event = next(event for event in events if event["type"] == "analysis_complete")
    assert complete_event["data"]["stop_reason"] == "repeated_topic"
    assert call_state["count"] == 3


def test_execute_analysis_steps_rebinds_df_aliases_per_round(monkeypatch):
    session_id = "sess_df_alias_rebind"
    sandbox_id = "sb_flights_overview"

    def fake_query_rows(sql: str, sandbox_id: str | None = None):
        sql_text = str(sql).lower()
        if "travel_class" in sql_text:
            return pd.DataFrame(
                [
                    {"department": "Sales", "travel_class": "Economy", "trip_count": 10},
                    {"department": "Sales", "travel_class": "Business", "trip_count": 4},
                ]
            )
        return pd.DataFrame(
            [
                {"department": "Sales", "trip_count": 14},
                {"department": "HR", "trip_count": 7},
            ]
        )

    monkeypatch.setattr(main_module, "_query_rows", fake_query_rows)

    round1 = main_module._execute_analysis_steps(
        result_data={
            "steps": [
                {
                    "tool": "sql",
                    "source": "main",
                    "code": "SELECT department, COUNT(*) AS trip_count FROM tutorial_flights GROUP BY department",
                },
                {
                    "tool": "python",
                    "code": "final_df = df0.copy()",
                },
            ]
        },
        sandbox={"uploads": {}, "upload_paths": {}},
        selected_tables=["tutorial_flights"],
        selected_files=[],
        sandbox_id=sandbox_id,
        session_id=session_id,
    )
    assert not round1.get("error")
    assert round1.get("rows")
    assert "travel_class" not in round1["rows"][0]

    round2 = main_module._execute_analysis_steps(
        result_data={
            "steps": [
                {
                    "tool": "sql",
                    "source": "main",
                    "code": "SELECT department, travel_class, COUNT(*) AS trip_count FROM tutorial_flights GROUP BY department, travel_class",
                },
                {
                    "tool": "python",
                    "code": "final_df = df0[['travel_class']].copy()",
                },
            ]
        },
        sandbox={"uploads": {}, "upload_paths": {}},
        selected_tables=["tutorial_flights"],
        selected_files=[],
        sandbox_id=sandbox_id,
        session_id=session_id,
    )
    try:
        assert not round2.get("error")
        assert round2.get("rows")
        assert "travel_class" in round2["rows"][0]
    finally:
        main_module.destroy_kernel(session_id)


def test_save_skill_persists_context_snapshot_metadata():
    headers = _login_admin()

    client.post(
        "/api/sandboxes/sb_flights_overview/skills",
        headers=headers,
        json={"skills": []},
    )
    client.post(
        "/api/sandboxes/sb_flights_overview/knowledge_bases",
        headers=headers,
        json={"knowledge_bases": []},
    )

    _, _, seed_proposal_id = _run_mock_iteration(headers, message="seed mounted context skill")
    seed_skill_res = client.post(
        "/api/skills/save",
        headers=headers,
        json={
            "proposal_id": seed_proposal_id,
            "name": "seed-mounted-context-skill",
            "knowledge": ["mounted-context-rule"],
        },
    )
    assert seed_skill_res.status_code == 200
    mounted_skill_id = seed_skill_res.json()["skill"]["skill_id"]

    mount_skill_res = client.post(
        "/api/sandboxes/sb_flights_overview/skills",
        headers=headers,
        json={"skills": [mounted_skill_id]},
    )
    assert mount_skill_res.status_code == 200

    kb_res = client.post(
        "/api/knowledge_bases",
        headers=headers,
        json={"name": "snapshot-kb", "content": "snapshot content"},
    )
    assert kb_res.status_code == 200
    kb_id = kb_res.json()["id"]

    mount_kb_res = client.post(
        "/api/sandboxes/sb_flights_overview/knowledge_bases",
        headers=headers,
        json={"knowledge_bases": [kb_id]},
    )
    assert mount_kb_res.status_code == 200

    upload_res = client.post(
        "/api/data/upload",
        headers=headers,
        data={"sandbox_id": "sb_flights_overview"},
        files=[("files", ("orders.csv", "a,b\n1,2\n", "text/csv"))],
    )
    assert upload_res.status_code == 200

    iterate_res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "analyze with full context snapshot",
            "provider": "mock",
            "selected_tables": ["tutorial_flights"],
            "selected_files": ["orders.csv"],
        },
    )
    assert iterate_res.status_code == 200
    iterate_events = _parse_ndjson_events(iterate_res.text)
    complete_event = next(event for event in iterate_events if event["type"] == "iteration_complete")
    session_id = complete_event["data"]["session_id"]
    proposal_id = complete_event["data"]["proposal_id"]

    save_res = client.post(
        "/api/skills/save",
        headers=headers,
        json={"proposal_id": proposal_id, "name": "snapshot-target-skill"},
    )
    assert save_res.status_code == 200
    saved_skill = save_res.json()["skill"]
    snapshot = saved_skill["layers"]["context_snapshot"]

    assert snapshot["source"]["session_id"] == session_id
    assert snapshot["source"]["sandbox_id"] == "sb_flights_overview"
    assert snapshot["source"]["sandbox_name"]
    assert snapshot["source"]["session_title"]
    assert snapshot["conversation_link"]["dashboard_path"] == f"/web/dashboard.html?session_id={session_id}"
    assert snapshot["tables"]["selected_tables"] == ["tutorial_flights"]
    assert "tutorial_flights" in snapshot["tables"]["sandbox_tables"]
    assert any(item["skill_id"] == mounted_skill_id for item in snapshot["mounted_skills"])
    assert any(item["id"] == kb_id for item in snapshot["knowledge_bases"])
    assert any(item["name"] == "orders.csv" and item["selected"] for item in snapshot["files"])
    assert snapshot["context_sources"]["selected_tables"] is True
    assert snapshot["context_sources"]["selected_files"] is True
    assert snapshot["context_sources"]["mounted_skills"] is True
    assert snapshot["context_sources"]["knowledge_bases"] is True


def test_auto_analyze_stops_when_model_stops_using_tools_and_persists_report(monkeypatch):
    headers = _login_admin()
    call_state = {"count": 0}

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        call_state["count"] += 1
        if call_state["count"] == 1:
            yield {
                "type": "thought",
                "content": "round one",
            }
            yield {
                "type": "result",
                "data": {
                    "steps": [{"tool": "sql", "code": "SELECT * FROM tutorial_flights LIMIT 1"}],
                    "conclusions": [{"text": "first conclusion", "confidence": 0.8}],
                    "hypotheses": [{"id": "h1", "text": "verify next"}],
                    "action_items": ["keep digging"],
                    "tools_used": ["execute_select_sql"],
                    "explanation": "first round",
                    "final_report_outline": ["summary"],
                },
            }
        else:
            yield {
                "type": "result",
                "data": {
                    "steps": [],
                    "conclusions": [{"text": "final conclusion", "confidence": 0.9}],
                    "hypotheses": [{"id": "h2", "text": "follow-up"}],
                    "action_items": ["ship report"],
                    "tools_used": [],
                    "explanation": "final round",
                    "final_report_outline": ["final"],
                },
            }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "Auto Report",
            "summary": "done",
            "html_document": "<!doctype html><html><body><h1>done</h1><div data-chart-id=\"chart_1\"></div></body></html>",
            "chart_bindings": [{"chart_id": "chart_1", "option": {"xAxis": {}, "yAxis": {}, "series": []}, "height": 320}],
            "legacy_markdown": "## Executive Summary\n- done",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## Executive Summary\n- done")

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "auto analyze flights",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    event_types = [event["type"] for event in events]
    assert "loop_status" in event_types
    assert "loop_round" in event_types
    assert "report" in event_types
    assert "analysis_complete" in event_types

    complete_event = next(event for event in events if event["type"] == "analysis_complete")
    assert complete_event["data"]["rounds_completed"] == 2
    assert complete_event["data"]["max_rounds_hit"] is False
    assert complete_event["data"]["report_url"].startswith("/web/report.html?iteration_id=")
    assert complete_event["data"]["report_title"] == "Auto Report"

    report_event = next(event for event in events if event["type"] == "report")
    assert report_event["data"]["title"] == "Auto Report"
    assert report_event["data"]["summary"] == "done"

    history = client.get(
        f"/api/chat/history?session_id={complete_event['data']['session_id']}",
        headers=headers,
    )
    assert history.status_code == 200
    saved = history.json()["iterations"][0]
    assert saved["mode"] == "auto_analysis"
    assert saved["final_report_md"].startswith("## Executive Summary")
    assert saved["report_title"] == "Auto Report"
    assert "data-chart-id" in saved["final_report_html"]
    assert saved["final_report_summary"] == "done"
    assert len(saved["final_report_chart_bindings"]) == 1
    assert len(saved["loop_rounds"]) == 2

    report_res = client.get(
        f"/api/reports/iterations/{complete_event['data']['iteration_id']}",
        headers=headers,
    )
    assert report_res.status_code == 200
    payload = report_res.json()
    assert payload["report_title"] == "Auto Report"
    assert payload["final_report_summary"] == "done"
    assert len(payload["final_report_chart_bindings"]) == 1


def test_auto_analyze_normalizes_wrapped_html_before_persist(monkeypatch):
    headers = _login_admin()

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "done", "confidence": 0.8}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "done",
                "final_report_outline": [],
            },
        }

    wrapped_html = (
        '{"title":"wrapped","summary":"wrapped","html_document":"<!doctype html>'
        '<html><body><h1>中文报告</h1></body></html>","chart_bindings":[]}'
    )
    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "Auto Wrapped",
            "summary": "wrapped",
            "html_document": wrapped_html,
            "chart_bindings": [],
            "legacy_markdown": "## 执行摘要\n- wrapped",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## 执行摘要\n- wrapped")

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "auto analyze wrapped html",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete_event = next(event for event in events if event["type"] == "analysis_complete")

    history = client.get(
        f"/api/chat/history?session_id={complete_event['data']['session_id']}",
        headers=headers,
    )
    assert history.status_code == 200
    saved = history.json()["iterations"][0]
    final_html = str(saved["final_report_html"])
    assert final_html.strip()
    assert not final_html.lstrip().startswith("{")
    assert "<html" in final_html.lower()
    assert "中文报告" in final_html


def test_auto_analyze_converts_markdown_html_field_to_real_html(monkeypatch):
    headers = _login_admin()

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "done", "confidence": 0.8}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "done",
                "final_report_outline": [],
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "Auto Markdown",
            "summary": "md summary",
            "html_document": "# 一级标题\n## 二级标题\n- 要点一\n- 要点二",
            "chart_bindings": [{"chart_id": "chart_1", "option": {"xAxis": {}, "yAxis": {}, "series": []}, "height": 320}],
            "legacy_markdown": "",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## 执行摘要\n- fallback")

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "auto analyze markdown html field",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete_event = next(event for event in events if event["type"] == "analysis_complete")

    history = client.get(
        f"/api/chat/history?session_id={complete_event['data']['session_id']}",
        headers=headers,
    )
    assert history.status_code == 200
    saved = history.json()["iterations"][0]
    final_html = str(saved["final_report_html"])
    assert "<html" in final_html.lower()
    assert "<h1>" in final_html.lower()
    assert "<ul>" in final_html.lower()
    assert "data-chart-id=\"chart_1\"" in final_html

    report_res = client.get(
        f"/api/reports/iterations/{complete_event['data']['iteration_id']}",
        headers=headers,
    )
    assert report_res.status_code == 200
    payload = report_res.json()
    assert "<h1>" in str(payload["final_report_html"]).lower()
    assert "data-chart-id=\"chart_1\"" in str(payload["final_report_html"])


def test_auto_analyze_round_message_respects_zh_language_header(monkeypatch):
    headers = _login_admin()
    headers["X-Language"] = "zh"
    captured: dict[str, str] = {}

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        captured["message"] = message
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "done", "confidence": 0.7}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "done",
                "final_report_outline": [],
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "Auto ZH",
            "summary": "zh",
            "html_document": "<!doctype html><html><body><h1>zh</h1></body></html>",
            "chart_bindings": [],
            "legacy_markdown": "## 执行摘要\n- zh",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## 执行摘要\n- zh")

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "请做一次自动分析",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    assert "必须使用简体中文" in captured["message"]


def test_auto_analyze_returns_localized_error_when_html_report_generation_fails(monkeypatch):
    headers = _login_admin()
    headers["X-Language"] = "zh"

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "done", "confidence": 0.7}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "done",
                "final_report_outline": [],
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: (_ for _ in ()).throw(
            RuntimeError("AI failed to generate qualified HTML report after 3 attempts: html_document is not a standalone HTML document")
        ),
    )

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "自动分析失败提示",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    error_event = next(event for event in events if event["type"] == "error")
    assert "连续 3 次都未生成合格的 HTML 报告" in error_event["message"]


def test_iterate_runtime_error_is_localized_and_does_not_raise_unboundlocal(monkeypatch):
    headers = _login_admin()
    headers["X-Language"] = "zh"

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        raise RuntimeError("AI failed to generate qualified HTML report after 3 attempts: html_document is not a standalone HTML document")
        yield  # pragma: no cover

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)

    res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "触发重试失败",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    error_event = next(event for event in events if event["type"] == "error")
    assert "连续 3 次都未生成合格的 HTML 报告" in error_event["message"]


def test_propose_skill_from_auto_analysis_returns_snapshot_and_non_empty_fields(monkeypatch):
    headers = _login_admin()

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "发现成本异常", "confidence": 0.8}],
                "hypotheses": [],
                "action_items": ["优化差旅审批策略"],
                "tools_used": [],
                "explanation": "done",
                "final_report_outline": [],
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "自动分析报告",
            "summary": "摘要内容",
            "html_document": "<!doctype html><html><body><h1>自动分析报告</h1></body></html>",
            "chart_bindings": [],
            "legacy_markdown": "## 执行摘要\n- 摘要内容",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## 执行摘要\n- 摘要内容")
    monkeypatch.setattr(main_module, "generate_skill_proposal", lambda **kwargs: {"name": "", "description": "", "tags": [], "knowledge": []})

    auto_res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "自动分析差旅成本",
            "provider": "mock",
        },
    )
    assert auto_res.status_code == 200
    auto_events = _parse_ndjson_events(auto_res.text)
    complete_event = next(event for event in auto_events if event["type"] == "analysis_complete")
    proposal_id = complete_event["data"]["proposal_id"]
    session_id = complete_event["data"]["session_id"]

    propose_res = client.post(
        "/api/skills/propose",
        headers=headers,
        json={
            "proposal_id": proposal_id,
            "message": "自动分析差旅成本",
            "sandbox_id": "sb_flights_overview",
        },
    )
    assert propose_res.status_code == 200
    payload = propose_res.json()
    assert payload["name"]
    assert payload["description"]
    assert isinstance(payload["knowledge"], list)
    assert payload["context_snapshot"]["source"]["session_id"] == session_id
    assert payload["context_snapshot"]["conversation_link"]["dashboard_path"] == f"/web/dashboard.html?session_id={session_id}"


def test_auto_analyze_marks_max_rounds_hit(monkeypatch):
    headers = _login_admin()

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [{"tool": "sql", "code": "SELECT * FROM tutorial_flights LIMIT 1"}],
                "conclusions": [{"text": "still working", "confidence": 0.6}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": ["execute_select_sql"],
                "explanation": "keep going",
                "final_report_outline": [],
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "Auto Report",
            "summary": "partial",
            "html_document": "<!doctype html><html><body><h1>partial</h1></body></html>",
            "chart_bindings": [],
            "legacy_markdown": "## Executive Summary\n- partial",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## Executive Summary\n- partial")

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "auto analyze cap",
            "provider": "mock",
            "max_rounds": 1,
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete_event = next(event for event in events if event["type"] == "analysis_complete")
    assert complete_event["data"]["max_rounds_hit"] is True
    assert complete_event["data"]["stop_reason"] == "max_rounds_reached"


def test_auto_analyze_injects_session_patches_and_skill_save_works(monkeypatch):
    headers = _login_admin()
    _, session_id, _ = _run_mock_iteration(headers, message="seed session")

    feedback = client.post(
        "/api/chat/feedback",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "session_id": session_id,
            "feedback": "session patch rule",
            "is_business_knowledge": False,
        },
    )
    assert feedback.status_code == 200

    captured: dict[str, list[str]] = {}

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        captured["business_knowledge"] = business_knowledge
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "done", "confidence": 0.7}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "done",
                "final_report_outline": [],
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "Auto Report",
            "summary": "session aware",
            "html_document": "<!doctype html><html><body><h1>session aware</h1></body></html>",
            "chart_bindings": [],
            "legacy_markdown": "## Executive Summary\n- session aware",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## Executive Summary\n- session aware")

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "session_id": session_id,
            "message": "auto analyze with patch",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete_event = next(event for event in events if event["type"] == "analysis_complete")
    assert "[Session Patch]: session patch rule" in captured["business_knowledge"]

    save_res = client.post(
        "/api/skills/save",
        headers=headers,
        json={"proposal_id": complete_event["data"]["proposal_id"], "name": "auto-analysis-skill"},
    )
    assert save_res.status_code == 200
    saved_skill = save_res.json()["skill"]
    assert saved_skill["name"] == "auto-analysis-skill"
    snapshot = saved_skill["layers"]["context_snapshot"]
    assert snapshot["source"]["mode"] == "auto_analysis"
    assert snapshot["source"]["session_id"] == session_id
    assert snapshot["conversation_link"]["dashboard_path"] == f"/web/dashboard.html?session_id={session_id}"
    assert snapshot["report"]["report_title"] == "Auto Report"
    assert snapshot["report"]["stop_reason"] == "model_stopped_using_tools"
    assert snapshot["report"]["rounds_completed"] >= 1
    assert snapshot["report"]["max_rounds_hit"] is False


def test_auto_analyze_no_tool_call_with_direct_report_does_not_surface_parse_error(monkeypatch):
    headers = _login_admin()

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "JSON parse failed", "confidence": 0.0}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "bad json",
                "final_report_outline": [],
                "direct_report": "## Executive Summary\n- final report",
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "finish with direct report",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    report_event = next(event for event in events if event["type"] == "report")
    assert report_event["data"]["summary"].startswith("## Executive Summary")
    loop_round = next(event for event in events if event["type"] == "loop_round")
    assert loop_round["data"]["result"]["conclusions"] == []


def test_auto_analyze_allows_empty_message_and_returns_report_url(monkeypatch):
    headers = _login_admin()

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "done", "confidence": 0.7}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "done",
                "final_report_outline": [],
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "Auto Empty",
            "summary": "empty start",
            "html_document": "<!doctype html><html><body><h1>empty start</h1></body></html>",
            "chart_bindings": [],
            "legacy_markdown": "## Executive Summary\n- empty",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## Executive Summary\n- empty")

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete = next(event for event in events if event["type"] == "analysis_complete")
    assert complete["data"]["report_url"].startswith("/web/report.html?iteration_id=")
    assert complete["data"]["report_title"] == "Auto Empty"


def test_overwrite_skill_keeps_previous_context_snapshot_in_history():
    headers = _login_admin()

    _, first_session_id, first_proposal_id = _run_mock_iteration(headers, message="first snapshot version")
    first_save_res = client.post(
        "/api/skills/save",
        headers=headers,
        json={"proposal_id": first_proposal_id, "name": "overwrite-context-skill"},
    )
    assert first_save_res.status_code == 200
    first_skill = first_save_res.json()["skill"]
    skill_id = first_skill["skill_id"]
    first_snapshot = first_skill["layers"]["context_snapshot"]
    assert first_snapshot["source"]["session_id"] == first_session_id

    _, second_session_id, second_proposal_id = _run_mock_iteration(headers, message="second snapshot version")
    overwrite_res = client.post(
        "/api/skills/save",
        headers=headers,
        json={
            "proposal_id": second_proposal_id,
            "name": "overwrite-context-skill",
            "overwrite_skill_id": skill_id,
        },
    )
    assert overwrite_res.status_code == 200
    overwritten_skill = overwrite_res.json()["skill"]
    assert overwritten_skill["version"] == 2
    assert overwritten_skill["layers"]["context_snapshot"]["source"]["session_id"] == second_session_id

    history = overwritten_skill["history"]
    assert history
    previous_snapshot = history[-1]["layers"]["context_snapshot"]
    assert previous_snapshot["source"]["session_id"] == first_session_id
    assert previous_snapshot["source"]["proposal_id"] == first_proposal_id


def test_iterate_receives_latest_auto_report_summary_in_history(monkeypatch):
    headers = _login_admin()
    captured: dict[str, list[dict]] = {}

    def fake_run_analysis_iteration_auto(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "auto done", "confidence": 0.8}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "done",
                "final_report_outline": [],
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration_auto)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "Auto Context",
            "summary": "context summary",
            "html_document": "<!doctype html><html><body><h1>context</h1></body></html>",
            "chart_bindings": [],
            "legacy_markdown": "## Executive Summary\n- context",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## Executive Summary\n- context")

    auto_res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "first auto",
            "provider": "mock",
        },
    )
    assert auto_res.status_code == 200
    auto_events = _parse_ndjson_events(auto_res.text)
    auto_complete = next(event for event in auto_events if event["type"] == "analysis_complete")

    def fake_run_analysis_iteration_manual(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        captured["iteration_history"] = iteration_history
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "manual done", "confidence": 0.7}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "manual",
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration_manual)
    iterate_res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "session_id": auto_complete["data"]["session_id"],
            "message": "continue based on report",
            "provider": "mock",
        },
    )
    assert iterate_res.status_code == 200
    history = captured["iteration_history"]
    assert any(
        str(item.get("report_title", "")) == "Auto Context"
        and str(item.get("final_report_summary", "")) == "context summary"
        for item in history
    )

def test_knowledge_assets_workbench_endpoints_cover_kb_upload_and_experience():
    headers = _login_admin()

    kb_res = client.post(
        "/api/knowledge_bases",
        headers=headers,
        json={
            "name": "Refund Policy",
            "description": "Enterprise policy",
            "sync_type": "manual",
            "content": "退款规则：48小时内允许原路退款，逾期需要人工审批。",
        },
    )
    assert kb_res.status_code == 200
    kb_id = kb_res.json()["id"]

    mount_res = client.post(
        "/api/sandboxes/sb_flights_overview/knowledge_bases",
        headers=headers,
        json={"knowledge_bases": [kb_id]},
    )
    assert mount_res.status_code == 200

    upload_res = client.post(
        "/api/data/upload",
        headers=headers,
        data={"sandbox_id": "sb_flights_overview"},
        files={"files": ("ops_guide.txt", "退款场景下，客服需要先核验支付渠道。".encode("utf-8"), "text/plain")},
    )
    assert upload_res.status_code == 200

    _, _, proposal_id = _run_mock_iteration(headers, message="save experience asset")
    skill_res = client.post(
        "/api/skills/save",
        headers=headers,
        json={"proposal_id": proposal_id, "name": "refund-experience", "knowledge": ["退款需要先核验支付渠道"]},
    )
    assert skill_res.status_code == 200

    assets_res = client.get("/api/knowledge/assets", headers=headers)
    assert assets_res.status_code == 200
    assets = assets_res.json()["assets"]

    kb_asset = next(asset for asset in assets if asset["source_type"] == "knowledge_base" and asset["source_ref"] == kb_id)
    upload_asset = next(asset for asset in assets if asset["source_type"] == "upload" and asset["source_ref"] == "ops_guide.txt")
    experience_asset = next(asset for asset in assets if asset["source_type"] == "skill" and asset["title"] == "refund-experience")

    assert kb_asset["asset_type"] == "enterprise_kb"
    assert upload_asset["asset_type"] == "uploaded_file"
    assert experience_asset["asset_type"] == "experience"
    assert kb_asset["chunk_count"] >= 1
    assert upload_asset["chunk_count"] >= 1
    assert experience_asset["chunk_count"] >= 1

    content_res = client.get(f"/api/knowledge/assets/{kb_asset['asset_id']}/content?mode=full", headers=headers)
    assert content_res.status_code == 200
    assert "48小时内允许原路退款" in content_res.json()["content"]


def test_knowledge_index_search_debug_returns_locator_and_readable_asset():
    headers = _login_admin()

    kb_res = client.post(
        "/api/knowledge_bases",
        headers=headers,
        json={
            "name": "Attribution Rulebook",
            "description": "Marketing glossary",
            "sync_type": "manual",
            "content": "活动归因规则：用户首单归因到最近一次有效点击，退款订单不计入活动转化。",
        },
    )
    assert kb_res.status_code == 200
    kb_id = kb_res.json()["id"]

    mount_res = client.post(
        "/api/sandboxes/sb_flights_overview/knowledge_bases",
        headers=headers,
        json={"knowledge_bases": [kb_id]},
    )
    assert mount_res.status_code == 200

    search_res = client.post(
        "/api/knowledge/index/search-debug",
        headers=headers,
        json={"sandbox_id": "sb_flights_overview", "query": "活动归因规则", "top_k": 3},
    )
    assert search_res.status_code == 200
    results = search_res.json()["results"]
    assert results
    first = results[0]
    assert first["asset_id"]
    assert first["full_document_locator"].startswith("asset://")

    content_res = client.get(f"/api/knowledge/assets/{first['asset_id']}/content?mode=full", headers=headers)
    assert content_res.status_code == 200
    assert "退款订单不计入活动转化" in content_res.json()["content"]


def test_pending_experience_can_be_published_from_proposal():
    headers = _login_admin()
    _, _, proposal_id = _run_mock_iteration(headers, message="summarize refund handling")

    pending_res = client.get("/api/knowledge/experiences/pending", headers=headers)
    assert pending_res.status_code == 200
    pending_items = pending_res.json()["pending_experiences"]
    pending_item = next(item for item in pending_items if item["proposal_id"] == proposal_id)
    assert pending_item["sandbox_id"] == "sb_flights_overview"

    publish_res = client.post(
        "/api/knowledge/experiences/publish-from-proposal",
        headers=headers,
        json={
            "proposal_id": proposal_id,
            "name": "refund-playbook",
            "description": "Refund handling checklist",
            "knowledge": ["退款前先校验支付渠道", "高风险场景升级主管审批"],
            "mount_sandbox_ids": ["sb_flights_overview"],
        },
    )
    assert publish_res.status_code == 200
    asset = publish_res.json()["asset"]
    assert asset["asset_type"] == "experience"
    assert any(item["sandbox_id"] == "sb_flights_overview" for item in asset["mounted_sandboxes"])

    pending_res = client.get("/api/knowledge/experiences/pending", headers=headers)
    assert pending_res.status_code == 200
    assert all(item["proposal_id"] != proposal_id for item in pending_res.json()["pending_experiences"])


def test_pending_experience_can_be_dismissed():
    headers = _login_admin()
    _, _, proposal_id = _run_mock_iteration(headers, message="dismiss this experience")

    dismiss_res = client.post(f"/api/knowledge/experiences/{proposal_id}/dismiss", headers=headers)
    assert dismiss_res.status_code == 200

    pending_res = client.get("/api/knowledge/experiences/pending", headers=headers)
    assert pending_res.status_code == 200
    assert all(item["proposal_id"] != proposal_id for item in pending_res.json()["pending_experiences"])


def test_python_runtime_can_query_index_and_read_full_asset():
    headers = _login_admin()

    kb_res = client.post(
        "/api/knowledge_bases",
        headers=headers,
        json={
            "name": "Service Playbook",
            "description": "Ops guide",
            "sync_type": "manual",
            "content": "服务手册：退款申请命中高风险标签时，需要升级到主管审批。",
        },
    )
    assert kb_res.status_code == 200
    kb_id = kb_res.json()["id"]

    mount_res = client.post(
        "/api/sandboxes/sb_flights_overview/knowledge_bases",
        headers=headers,
        json={"knowledge_bases": [kb_id]},
    )
    assert mount_res.status_code == 200

    sandbox_res = client.get("/api/sandboxes", headers=headers)
    sandbox = next(item for item in sandbox_res.json()["sandboxes"] if item["sandbox_id"] == "sb_flights_overview")
    result = main_module._execute_analysis_steps(
        result_data={
            "steps": [
                {
                    "tool": "python",
                    "code": (
                        "hits = query_knowledge_index('高风险标签', top_k=1)\n"
                        "asset = read_knowledge_asset(hits[0]['asset_id'], mode='full')\n"
                        "final_df = pd.DataFrame([{'title': asset['title'], 'content': asset['content']}])\n"
                    ),
                }
            ]
        },
        sandbox=sandbox,
        selected_tables=[],
        selected_files=[],
        sandbox_id="sb_flights_overview",
        session_id="ss_knowledge_runtime_tools",
    )

    assert not result.get("error")
    assert result["rows"]
    assert result["rows"][0]["title"] == "Service Playbook"
    assert "升级到主管审批" in result["rows"][0]["content"]
