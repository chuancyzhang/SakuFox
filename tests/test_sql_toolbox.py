from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def _login_admin() -> dict[str, str]:
    res = client.post("/api/auth/login", json={"provider": "ldap", "username": "admin"})
    assert res.status_code == 200
    token = res.json()["token"]
    return {"Authorization": f"Bearer {token}"}


def test_sql_toolbox_execute_and_save_virtual_view():
    headers = _login_admin()

    execute_res = client.post(
        "/api/sql-toolbox/execute",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "sql": "SELECT department, COUNT(*) AS cnt FROM tutorial_flights GROUP BY department ORDER BY cnt DESC LIMIT 3",
        },
    )
    assert execute_res.status_code == 200
    run = execute_res.json()["run"]
    assert run["status"] == "success"
    assert run["row_count"] > 0
    assert run["columns"][0]["name"] == "department"

    runs_res = client.get("/api/sql-toolbox/runs?sandbox_id=sb_flights_overview", headers=headers)
    assert runs_res.status_code == 200
    assert runs_res.json()["runs"]

    save_res = client.post(
        "/api/sandboxes/sb_flights_overview/virtual-views",
        headers=headers,
        json={
            "source_run_id": run["run_id"],
            "name": "flight_department_cnt",
            "description": "按部门汇总差旅次数",
            "field_descriptions": {"department": "部门名称", "cnt": "差旅次数"},
        },
    )
    assert save_res.status_code == 200
    virtual_view = save_res.json()["virtual_view"]
    assert virtual_view["name"] == "flight_department_cnt"
    assert virtual_view["source_run_id"] == run["run_id"]
    assert any(col.get("description") for col in virtual_view["columns"])

    sandboxes_res = client.get("/api/sandboxes", headers=headers)
    assert sandboxes_res.status_code == 200
    sandbox = next(item for item in sandboxes_res.json()["sandboxes"] if item["sandbox_id"] == "sb_flights_overview")
    assert any(vv["name"] == "flight_department_cnt" for vv in sandbox.get("virtual_views", []))


def test_sql_toolbox_rejects_multi_statement_sql():
    headers = _login_admin()
    res = client.post(
        "/api/sql-toolbox/execute",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "sql": "SELECT 1; SELECT 2",
        },
    )
    assert res.status_code == 400
    assert "单条" in res.json()["detail"] or "single" in res.json()["detail"].lower()
