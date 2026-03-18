import os
from dataclasses import dataclass
from pathlib import Path


LLM_PROVIDER = "anthropic"

OPENAI_API_KEY = ""
OPENAI_BASE_URL = ""
OPENAI_ENDPOINT = ""
OPENAI_MODEL = "gpt-4o-mini"

ANTHROPIC_API_KEY = ""
ANTHROPIC_BASE_URL = "https://api.lkeap.cloud.tencent.com/coding/anthropic"
ANTHROPIC_ENDPOINT = ""
ANTHROPIC_MODEL = "glm-5"
ANTHROPIC_VERSION = ""
MAX_SELECTED_TABLES = 5

# --- Database Configuration ---
DEFAULT_DB_TYPE = "sqlite"
DEFAULT_DB_URL = "sqlite:///./sakufox.db"

# ── 迭代式分析核心提示词（Multi-Step Agentic） ─────────────────────────
ITERATION_SYSTEM_PROMPT = (
    "【最高优先级约束】你的输出必须是且只能是一个合法的 JSON 对象，不能包含任何 markdown 格式（不能有 ```json 代码块标记），"
    "不能在 JSON 前后添加任何解释文字。整个回复从 { 开始，以 } 结束。\n"
    "【严重警告】在输出的 JSON 字符串中（例如 python_code 字段），**绝对不能出现未转义的真实换行符**，所有的换行必须严格转义写入为 `\\n` ！\n"
    "\n"
    "你是 SakuFox 🦊 — 企业级无缝数据分析 Agent。你的核心能力是灵活调度 SQL 和 Python 工具，在一个统一的分析上下文中完成任务。\n"
    "\n"
    "【统一分析管道】\n"
    "你可以输出一个 `steps` 数组，执行遵循以下“无缝”规则：\n"
    "- **Schema 感知**：系统已在提示词中为你提供了所有表的字段名和样数据（Ground Truth）。请务必精准使用这些字段名，不要臆测。\n"
    "- **自动变量注入**：每个 SQL 步骤的结果会自动绑定为变量 `df0`, `df1`, ..., `dfN`（对应 step 的索引）进入 Python 环境。变量 `df` 始终指向最近一个 SQL 结果。\n"
    "- **变量持久化**：所有步骤共享同一个变量空间。你在步骤 1 定义的变量，在后续所有步骤中均可直接使用。\n"
    "- **别名透明**：SQL 中的 `AS alias` 会被 100% 保留为 Dataframe 的列名。请务必为聚合函数（SUM, COUNT 等）指定别名。\n"
    "- **遇错即停**：如果任意步骤失败，执行会立即中断。请确保每一步的逻辑严谨。\n"
    "\n"
    "【Python 使用示例】\n"
    "- 直接使用注入的变量：`final_df = df0.merge(df1, on='id')`\n"
    "- 处理多库数据：使用多个 SQL step 获取数据，然后在 Python 中做跨库关联。\n"
    "\n"
    "\n"
    "【Python 代码规范 - 重要】\n"
    "Python 沙盒中已预先注入了以下变量，【禁止写任何 import 语句】，直接使用即可：\n"
    "- df: 最近 SQL 步骤结果的 DataFrame (若无 SQL 则为空 DataFrame) \n"
    "- step_results: 所有前序步骤的执行结果 list\n"
    "- pd / pandas: pandas 库\n"
    "- np / numpy: numpy 库\n"
    "- json / math / re: 标准库\n"
    "- datetime / date / timedelta: 日期类\n"
    "- Counter / defaultdict: collections 常用类\n"
    "- execute_select_sql(sql): 沙盒内执行 SQL，返回 list[dict]\n"
    "- uploaded_dataframes: 用户上传文件字典，key 为文件名\n"
    "- uploaded_file_paths: 用户上传文件物理路径字典，可使用 pd.read_excel(uploaded_file_paths['文件名']) 加载\n"
    "- final_df: 必须赋值，最终输出 DataFrame\n"
    "- chart_specs: 追加 ECharts option dict 以输出图表\n"
    "sklearn 预测/分群 (直接使用，无需 import)：\n"
    "  LinearRegression, LogisticRegression, Ridge, Lasso\n"
    "  RandomForestClassifier, RandomForestRegressor\n"
    "  GradientBoostingClassifier, GradientBoostingRegressor\n"
    "  KMeans, DBSCAN, AgglomerativeClustering\n"
    "  StandardScaler, MinMaxScaler, LabelEncoder, OneHotEncoder\n"
    "  train_test_split, cross_val_score\n"
    "  accuracy_score, f1_score, precision_score, recall_score\n"
    "  mean_squared_error, mean_absolute_error, r2_score\n"
    "  classification_report, confusion_matrix, PCA, Pipeline\n"
    "禁止使用 os、sys 等系统级模块 (可以使用 open 读取上传的纯文本文件)。\n"
    "\n"
    "【输出格式】\n"
    "请输出一个 JSON 对象 (不要包含 markdown 代码块标记)，包含以下字段：\n"
    '- steps: 执行步骤数组，每项为 {"tool": "sql"|"python", "code": "..."}\n'
    "- conclusions: 数组，每项包含 text (结论文本) 和 confidence (0-1 的置信度)，样本不足或分析不深时请如实标注\n"
    "- hypotheses: 3-5 个可验证的下一步分析猜想或待确认的业务假设，每项包含 id 和 text\n"
    "- action_items: 可执行的落地建议与效果预估\n"
    "- explanation: 本轮分析思路说明\n"
    "\n"
    "【核心原则】\n"
    "- 全透明：所有代码、计算过程、判断逻辑全部公开\n"
    "- 轻量高效：每轮只做当前最必要的分析，不做冗余计算\n"
    "- 业务优先：用户补充的业务知识优先级最高，分析必须贴合实际场景\n"
    "- 结论可溯源：每个结论都能追溯到对应的原始数据和计算逻辑\n"
    "- 置信度标注：每个结论标注可信程度，数据不充分时主动提示\n"
)


INSIGHT_PROMPT_METRICS = "你是企业数据分析师，输出 Markdown，说明关键指标口径与当前结论，不要输出代码。"
INSIGHT_PROMPT_ANOMALY = "你是异常归因分析师，输出 Markdown，聚焦异常与可能原因，不要输出代码。"
INSIGHT_PROMPT_ACTIONS = "你是业务负责人，输出 Markdown，给出可执行动作与优先级，不要输出代码。"


def _read_dotenv() -> dict[str, str]:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return {}
    output: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        output[key.strip()] = value.strip().strip('"').strip("'")
    return output


def _pick(*values: str, default: str = "") -> str:
    for value in values:
        if value and str(value).strip():
            return str(value).strip()
    return default


@dataclass(frozen=True)
class AppConfig:
    llm_provider: str
    openai_api_key: str
    openai_base_url: str
    openai_endpoint: str
    openai_model: str
    anthropic_api_key: str
    anthropic_base_url: str
    anthropic_endpoint: str
    anthropic_model: str
    anthropic_version: str
    max_selected_tables: int
    iteration_system_prompt: str
    insight_prompt_metrics: str
    insight_prompt_anomaly: str
    insight_prompt_actions: str
    db_type: str
    db_url: str


def load_config() -> AppConfig:
    dotenv = _read_dotenv()
    max_selected_tables_raw = _pick(str(MAX_SELECTED_TABLES), os.getenv("MAX_SELECTED_TABLES", ""), dotenv.get("MAX_SELECTED_TABLES", ""), default="5")
    try:
        max_selected_tables = max(1, int(max_selected_tables_raw))
    except ValueError:
        max_selected_tables = 5
    return AppConfig(
        llm_provider=_pick(LLM_PROVIDER, os.getenv("LLM_PROVIDER", ""), dotenv.get("LLM_PROVIDER", ""), default="mock").lower(),
        openai_api_key=_pick(OPENAI_API_KEY, os.getenv("OPENAI_API_KEY", ""), dotenv.get("OPENAI_API_KEY", "")),
        openai_base_url=_pick(OPENAI_BASE_URL, os.getenv("OPENAI_BASE_URL", ""), dotenv.get("OPENAI_BASE_URL", ""), default="https://api.openai.com").rstrip("/"),
        openai_endpoint=_pick(OPENAI_ENDPOINT, os.getenv("OPENAI_ENDPOINT", ""), dotenv.get("OPENAI_ENDPOINT", ""), default="/v1/chat/completions"),
        openai_model=_pick(OPENAI_MODEL, os.getenv("OPENAI_MODEL", ""), dotenv.get("OPENAI_MODEL", ""), default="gpt-4o-mini"),
        anthropic_api_key=_pick(ANTHROPIC_API_KEY, os.getenv("ANTHROPIC_API_KEY", ""), dotenv.get("ANTHROPIC_API_KEY", "")),
        anthropic_base_url=_pick(ANTHROPIC_BASE_URL, os.getenv("ANTHROPIC_BASE_URL", ""), dotenv.get("ANTHROPIC_BASE_URL", ""), default="https://api.anthropic.com").rstrip("/"),
        anthropic_endpoint=_pick(ANTHROPIC_ENDPOINT, os.getenv("ANTHROPIC_ENDPOINT", ""), dotenv.get("ANTHROPIC_ENDPOINT", ""), default="/v1/messages"),
        anthropic_model=_pick(ANTHROPIC_MODEL, os.getenv("ANTHROPIC_MODEL", ""), dotenv.get("ANTHROPIC_MODEL", ""), default="claude-3-5-sonnet-latest"),
        anthropic_version=_pick(ANTHROPIC_VERSION, os.getenv("ANTHROPIC_VERSION", ""), dotenv.get("ANTHROPIC_VERSION", ""), default="2023-06-01"),
        max_selected_tables=max_selected_tables,
        iteration_system_prompt=_pick(ITERATION_SYSTEM_PROMPT, os.getenv("ITERATION_SYSTEM_PROMPT", ""), dotenv.get("ITERATION_SYSTEM_PROMPT", "")),
        insight_prompt_metrics=_pick(INSIGHT_PROMPT_METRICS, os.getenv("INSIGHT_PROMPT_METRICS", ""), dotenv.get("INSIGHT_PROMPT_METRICS", "")),
        insight_prompt_anomaly=_pick(INSIGHT_PROMPT_ANOMALY, os.getenv("INSIGHT_PROMPT_ANOMALY", ""), dotenv.get("INSIGHT_PROMPT_ANOMALY", "")),
        insight_prompt_actions=_pick(INSIGHT_PROMPT_ACTIONS, os.getenv("INSIGHT_PROMPT_ACTIONS", ""), dotenv.get("INSIGHT_PROMPT_ACTIONS", "")),
        db_type=_pick(os.getenv("DB_TYPE", ""), dotenv.get("DB_TYPE", ""), DEFAULT_DB_TYPE),
        db_url=_pick(os.getenv("DB_URL", ""), dotenv.get("DB_URL", ""), DEFAULT_DB_URL),
    )
