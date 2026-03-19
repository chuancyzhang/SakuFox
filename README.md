# SakuFox 🦊: Agentic HITL Data Analytics Platform

> **Saku (樱花) for elegance, Fox (狐狸) for precision. Catching core insights from your data with ninja-like agility.**

SakuFox is an interactive data analysis platform based on Agentic autonomous agents and Human-in-the-Loop (HITL). It transforms natural language into interpretable SQL within a restricted business domain, allowing users to refine AI logic through interaction and precipitate it into reusable business skills.

![intro](images/preview.png)
![intro](images/usage.png)
![SakuFox Video](images/SakuFox_video.mp4)

---

## ✨ Key Features

### 🧠 **Human-in-the-Loop (HITL) Iteration**
Interactive analytical loop. Users can provide feedback (e.g., "Knowledge: ...") to correct AI reasoning mid-process, ensuring the final SQL and logic are perfectly aligned with business reality.

### 🛠️ **Business Skill Precipitation**
Transform one-off analyses into permanent assets. Once a path is verified, "Save as Skill" to automatically extract and store high-quality business rules, metrics, and SQL logic for zero-shot future reuse.

### 📁 **Persistent Workspaces (Sandboxes)**
Move beyond ephemeral sessions. Create, name, and manage dedicated Workspaces for different projects, each with its own DB connections and knowledge base.

### 🔗 **Unified Data Context**
*   **Multi-DB Support**: Connect to SQLite, MySQL, PostgreSQL, etc.
*   **Selective Schema**: Pick specific tables to maintain high precision and stay within context limits.
*   **Large File Analysis**: Native Pandas support for `CSV`, `Excel`, `JSON`, handling millions of rows efficiently.

### 🛡️ **Enterprise Security**
*   **Permission Whitelisting**: Data access is strictly controlled by user group permissions.
*   **Secure Execution**: Python/SQL operations run in a controlled sandbox to protect infrastructure.

---

## 🚀 Quick Start

### Prerequisites
*   Python 3.10+
*   A modern web browser (Chrome/Edge/Firefox)

### Installation

1.  **Environment Setup**
    ```bash
    # Create and activate virtual environment
    python -m venv .venv
    .\.venv\Scripts\activate  # Windows
    # source .venv/bin/activate # Mac/Linux
    
    # Install dependencies
    pip install -r requirements.txt
    ```

2.  **Configuration**
    *   Initialize `app/config.py` (copy from `app/config.example.py`).
    *   Set your LLM provider (`openai`, `anthropic`, or `mock` for testing).

3.  **Run Application**
    ```bash
    python -m uvicorn app.main:app --reload
    ```

4.  **Access Dashboard**
    *   Visit `http://localhost:8000/web/dashboard.html`.
    *   Log in (e.g., username `admin` for LDAP demo).
    *   Create a new **Workspace**, connect a database, and start asking!

---

## 🏗️ Technical Architecture

*   **Backend**: [FastAPI](https://fastapi.tiangolo.com/) + [SQLAlchemy](https://www.sqlalchemy.org/)
    *   **Agent**: Custom state-machine agent with tool-calling capabilities.
    *   **Data Tier**: [Pandas](https://pandas.pydata.org/) for high-performance file analysis.
*   **Frontend**: Vanilla JavaScript + CSS (Premium Dark/Glassmorphism aesthetic)
    *   **Charts**: [Apache ECharts](https://echarts.apache.org/)
    *   **Markdown**: [Marked.js](https://marked.js.org/)

## 📝 Core API Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/chat/iterate` | Streamed. Main analytical loop for SQL/Python generation. |
| `POST` | `/api/sandboxes` | CRUD for persistent Workspace management. |
| `POST` | `/api/data/upload` | Upload local files directly into Workspace disk storage. |
| `POST` | `/api/chat/feedback` | Persist business knowledge/rules to the active Workspace. |
| `GET`  | `/api/sandboxes/{id}/db-tables` | Fetch and select available schemas from an external DB. |

---

## 🤝 Contributing

Contributions are welcome! Please submit a Pull Request or open an issue for feature requests.

---

## 🌟 Acknowledgement

A special thanks to the [Apache Superset](https://superset.apache.org/) project. This project draws inspiration from its frontend design and utilizes its demo datasets. Superset is an outstanding open-source data visualization and exploration platform that serves as a benchmark for modern data tools.

---

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
