from datetime import datetime, timezone
from sqlalchemy import Column, String, Text, JSON, DateTime, Integer, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class DBUser(Base):
    __tablename__ = "users"
    user_id = Column(String(50), primary_key=True)
    username = Column(String(50), unique=True, index=True)
    display_name = Column(String(100))
    groups = Column(JSON)  # list[str]
    provider = Column(String(20))

class DBSandbox(Base):
    __tablename__ = "sandboxes"
    sandbox_id = Column(String(50), primary_key=True)
    name = Column(String(255))
    tables = Column(JSON)  # list[str]
    allowed_groups = Column(JSON)  # list[str]
    business_knowledge = Column(JSON)  # list[dict]
    uploads = Column(JSON)  # dict[str, list[dict]]
    upload_paths = Column(JSON)  # dict[str, str]
    db_config = Column(JSON)  # dict (optional external db config)
    db_connection_id = Column(String(50), nullable=True)
    knowledge_bases = Column(JSON) # list[str], knowledge base IDs
    mounted_skills = Column(JSON)  # list[str], mounted skill IDs

class DBDatabaseConnection(Base):
    __tablename__ = "database_connections"
    connection_id = Column(String(50), primary_key=True)
    name = Column(String(255))
    db_type = Column(String(50))
    host = Column(String(255))
    port = Column(Integer, nullable=True)
    database = Column(String(500))
    username = Column(String(255))
    encrypted_password = Column(Text)
    created_at = Column(String(50))
    updated_at = Column(String(50))

class DBKnowledgeBase(Base):
    __tablename__ = "knowledge_bases"
    id = Column(String(50), primary_key=True)
    name = Column(String(255))
    description = Column(Text)
    sync_type = Column(String(50)) # 'manual', 'api'
    content = Column(Text)
    token_count = Column(Integer)
    api_url = Column(String(500))
    api_method = Column(String(10))
    api_headers = Column(JSON)
    api_params = Column(JSON)
    api_json_path = Column(String(255)) # JSON path to extract content
    created_at = Column(String(50))
    updated_at = Column(String(50))

class DBSession(Base):
    __tablename__ = "sessions"
    session_id = Column(String(50), primary_key=True)
    user_id = Column(String(50), index=True)
    title = Column(String(255))
    sandbox_id = Column(String(50))
    created_at = Column(String(50)) # ISO format
    patches = Column(JSON)  # list[str]

class DBIteration(Base):
    __tablename__ = "iterations"
    iteration_id = Column(String(50), primary_key=True)
    session_id = Column(String(50), index=True)
    user_id = Column(String(50), index=True)
    message = Column(Text)
    mode = Column(String(50), default="manual")
    steps = Column(JSON)
    conclusions = Column(JSON)
    hypotheses = Column(JSON)
    action_items = Column(JSON)
    tools_used = Column(JSON)
    result_rows = Column(JSON)
    chart_specs = Column(JSON)
    loop_rounds = Column(JSON)
    final_report_md = Column(Text)
    report_title = Column(String(255))
    final_report_html = Column(Text)
    final_report_summary = Column(Text)
    final_report_chart_bindings = Column(JSON)
    report_meta = Column(JSON)
    created_at = Column(String(50))

class DBSkill(Base):
    __tablename__ = "skills"
    skill_id = Column(String(50), primary_key=True)
    owner_id = Column(String(50), index=True)
    name = Column(String(255))
    description = Column(Text)
    tags = Column(JSON)
    layers = Column(JSON) # includes knowledge, tables, etc.
    version = Column(Integer, default=1)
    history = Column(JSON)
    created_at = Column(String(50))
    updated_at = Column(String(50))

class DBProposal(Base):
    __tablename__ = "proposals"
    proposal_id = Column(String(50), primary_key=True)
    user_id = Column(String(50), index=True)
    session_id = Column(String(50))
    sandbox_id = Column(String(50))
    message = Column(Text)
    steps = Column(JSON)
    explanation = Column(Text)
    mode = Column(String(50), default="manual")
    tables = Column(JSON)
    status = Column(String(50))
    result_rows = Column(JSON)
    chart_specs = Column(JSON)
    selected_tables = Column(JSON)
    selected_files = Column(JSON)
    session_patches = Column(JSON)
    loop_rounds = Column(JSON)
    final_report_md = Column(Text)
    report_title = Column(String(255))
    final_report_html = Column(Text)
    final_report_summary = Column(Text)
    final_report_chart_bindings = Column(JSON)
    report_meta = Column(JSON)
    created_at = Column(String(50))


class DBKnowledgeAsset(Base):
    __tablename__ = "knowledge_assets"
    asset_id = Column(String(50), primary_key=True)
    asset_type = Column(String(50), index=True)  # enterprise_kb / uploaded_file / experience
    title = Column(String(255), index=True)
    description = Column(Text)
    source_type = Column(String(50), index=True)  # knowledge_base / upload / skill
    source_ref = Column(String(100), index=True)
    source_path = Column(String(1000))
    sandbox_id = Column(String(50), index=True, nullable=True)
    owner_id = Column(String(50), index=True, nullable=True)
    permissions = Column(JSON)  # list[str]
    status = Column(String(20), default="active", index=True)
    content_type = Column(String(50))
    content_hash = Column(String(64), index=True)
    content_preview = Column(Text)
    metadata_json = Column(JSON)
    created_at = Column(String(50))
    updated_at = Column(String(50))


class DBKnowledgeChunk(Base):
    __tablename__ = "knowledge_chunks"
    chunk_id = Column(String(50), primary_key=True)
    asset_id = Column(String(50), index=True)
    chunk_index = Column(Integer, default=0)
    chunk_text = Column(Text)
    keywords = Column(JSON)
    embedding = Column(JSON)
    source_ref = Column(String(100), index=True)
    source_path = Column(String(1000))
    full_document_locator = Column(String(1000))
    content_hash = Column(String(64), index=True)
    index_version = Column(Integer, default=1)
    metadata_json = Column(JSON)
    created_at = Column(String(50))
    updated_at = Column(String(50))


class DBKnowledgeIndexJob(Base):
    __tablename__ = "knowledge_index_jobs"
    job_id = Column(String(50), primary_key=True)
    asset_id = Column(String(50), index=True, nullable=True)
    scope = Column(String(50), index=True)  # asset / sandbox / type / all
    status = Column(String(20), index=True)  # running / success / failed
    message = Column(Text)
    stats = Column(JSON)
    created_at = Column(String(50))
    updated_at = Column(String(50))


class DBExecutionRun(Base):
    __tablename__ = "execution_runs"
    run_id = Column(String(50), primary_key=True)
    status = Column(String(20), index=True)  # running / success / failed
    sandbox_id = Column(String(50), index=True)
    user_id = Column(String(50), index=True)
    sql = Column(Text)
    dependencies = Column(JSON)  # list[str], physical tables after virtual-view expansion
    row_count = Column(Integer, default=0)
    columns = Column(JSON)  # list[dict], e.g. [{"name": "...", "type": "..."}]
    error = Column(Text)
    duration_ms = Column(Integer, default=0)
    result_preview = Column(JSON)  # list[dict]
    created_at = Column(String(50))
    updated_at = Column(String(50))


class DBSandboxVirtualView(Base):
    __tablename__ = "sandbox_virtual_views"
    __table_args__ = (
        UniqueConstraint("sandbox_id", "name", name="uq_sandbox_virtual_view_name"),
    )

    view_id = Column(String(50), primary_key=True)
    sandbox_id = Column(String(50), index=True)
    name = Column(String(128), index=True)
    description = Column(Text)
    sql = Column(Text)
    columns = Column(JSON)  # list[dict], each dict may contain name/type/description
    sample_rows = Column(JSON)  # list[dict]
    source_run_id = Column(String(50), index=True)
    created_by = Column(String(50), index=True)
    created_at = Column(String(50))
    updated_at = Column(String(50))
