from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    provider: str = Field(pattern="^(ldap|oauth)$")
    username: str | None = None
    oauth_token: str | None = None


class IterateRequest(BaseModel):
    """Start or continue an analysis iteration."""
    sandbox_id: str
    message: str
    session_id: str | None = None
    provider: str | None = Field(default=None, pattern="^(openai|anthropic|mock)$")
    model: str | None = None
    selected_tables: list[str] | None = None
    selected_files: list[str] | None = None
    hypothesis_id: str | None = None  # pick a hypothesis from previous iteration


class AutoAnalyzeRequest(IterateRequest):
    """Run one-click autonomous multi-round analysis until the model stops using tools."""
    max_rounds: int = Field(default=100, ge=1, le=100)
    trace_mode: str = Field(default="full", pattern="^full$")


class FeedbackRequest(BaseModel):
    """User feedback or business knowledge supplement."""
    sandbox_id: str
    session_id: str
    feedback: str
    is_business_knowledge: bool = False


class SaveSkillRequest(BaseModel):
    proposal_id: str
    name: str
    description: str | None = None
    tags: list[str] | None = None
    knowledge: list[str] | None = None  # extra business knowledge lines
    table_descriptions: list[dict] | None = None  # [{"table": ..., "description": ...}]
    overwrite_skill_id: str | None = None


class UpdateSessionRequest(BaseModel):
    title: str


class ProposeSkillRequest(BaseModel):
    proposal_id: str
    message: str
    sandbox_id: str


class UpdateSkillRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    tags: list[str] | None = None
    knowledge: list[str] | None = None
    table_descriptions: list[dict] | None = None


class CreateSandboxRequest(BaseModel):
    name: str
    allowed_groups: list[str]


class RenameSandboxRequest(BaseModel):
    name: str


class CreateKnowledgeBaseRequest(BaseModel):
    name: str
    description: str | None = None
    sync_type: str = Field(default="manual", pattern="^(manual|api)$")
    content: str | None = None
    api_url: str | None = None
    api_method: str | None = "GET"
    api_headers: dict | None = None
    api_params: dict | None = None
    api_json_path: str | None = None


class UpdateKnowledgeBaseRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    sync_type: str | None = None
    content: str | None = None
    api_url: str | None = None
    api_method: str | None = None
    api_headers: dict | None = None
    api_params: dict | None = None
    api_json_path: str | None = None


class MountKnowledgeBasesRequest(BaseModel):
    knowledge_bases: list[str]


class MountSkillsRequest(BaseModel):
    skills: list[str]
