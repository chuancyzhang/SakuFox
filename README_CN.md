# SakuFox 🦊: Agentic HITL 交互式数据分析平台

> **Saku (樱花) for elegance, Fox (狐狸) for precision. Catching core insights from your data with ninja-like agility.**

SakuFox 是一个基于 Agentic 自主智能体与人机协同 (HITL) 的交互式数据分析平台。它能将自然语言转化为受限业务域内的可解释 SQL，允许用户通过交互修正 AI 逻辑并将其沉淀为可复用的业务技能。

![intro](images/preview.png)
![intro](images/usage.png)

---

## ✨ 核心特性

### 🧠 **人机协同 (HITL) 迭代分析**
支持交互式分析闭环。用户可以在分析过程中通过反馈（如“业务知识: ...”）直接修正 AI 的推理路径，确保生成的 SQL 和分析逻辑与业务现状完美匹配。

### 🛠️ **业务技能沉淀 (Skill Precipitation)**
将零散的分析过程转化为持久的业务资产。一旦分析路径得到验证，点击“保存为技能”即可自动提炼高质量的业务规则、指标定义和 SQL 逻辑。

### 📁 **持久化工作空间 (Workspaces)**
告别碎片化的对话记录。为不同的项目创建独立的工作空间，每个空间拥有专属的数据库连接、上传文件和业务知识库。

### 🔗 **统一数据上下文 (Unified Data Context)**
*   **多数据库支持**：一键连接 SQLite、MySQL、PostgreSQL 等主流数据库。
*   **精细化 Schema 控制**：精确挑选暴露给 AI 的表及其字段，确保持续的高精度输出。
*   **海量文件分析**：内置 Pandas 原生支持 `CSV`、`Excel`、`JSON`，可高效处理数百万行大型文件。

### 🛡️ **企业级安全保障**
*   **权限白名单**：数据访问严格受用户组权限控制。
*   **受控执行环境**：SQL 与 Python 脚本在沙箱环境中运行，确保基础设施的安全稳定。

---

## 🚀 快速开始

### 前置要求
*   Python 3.10+
*   现代 Web 浏览器 (Chrome/Edge/Firefox)

### 安装步骤

1.  **环境设置**
    ```bash
    # 创建并激活虚拟环境
    python -m venv .venv
    .\.venv\Scripts\activate  # Windows
    # source .venv/bin/activate # Mac/Linux
    
    # 安装依赖
    pip install -r requirements.txt
    ```

2.  **配置**
    *   初始化 `app/config.py` (从 `app/config.example.py` 复制)。
    *   设置您的 LLM 提供商 (`openai`, `anthropic`, 或使用 `mock` 进行测试)。

3.  **运行应用**
    ```bash
    python -m uvicorn app.main:app --reload
    ```

4.  **访问仪表盘**
    *   访问 `http://localhost:8000/web/dashboard.html`。
    *   登录 (例如使用用户名 `admin` 进行 LDAP 演示)。
    *   创建一个新 **工作空间**，连接数据库，开始提问！

---

## 🏗️ 技术架构

*   **后端**: [FastAPI](https://fastapi.tiangolo.com/) + [SQLAlchemy](https://www.sqlalchemy.org/)
    *   **Agent**: 具备工具调用能力的自定义状态机 Agent。
    *   **数据层**: 使用 [Pandas](https://pandas.pydata.org/) 进行高性能文件分析。
*   **Frontend**: 原生 JavaScript + CSS (高质感暗色/毛玻璃美学设计)
    *   **图表**: [Apache ECharts](https://echarts.apache.org/)
    *   **Markdown**: [Marked.js](https://marked.js.org/)

## 📝 核心 API 端点

| 方法 | 端点 | 描述 |
| :--- | :--- | :--- |
| `POST` | `/api/chat/iterate` | 流式输出。SQL/Python 生成的主分析循环。 |
| `POST` | `/api/sandboxes` | 持久化工作空间管理的 CRUD 操作。 |
| `POST` | `/api/data/upload` | 将本地文件直接上传到工作空间磁盘存储。 |
| `POST` | `/api/chat/feedback` | 向当前工作空间沉淀业务知识/规则。 |
| `GET`  | `/api/sandboxes/{id}/db-tables` | 获取并选择外部数据库中的可用表结构。 |

---

## 🤝 贡献

欢迎贡献代码！请随时提交 Pull Request 或通过 Issue 提出功能建议。
