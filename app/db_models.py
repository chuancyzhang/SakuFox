from datetime import datetime, timezone
from sqlalchemy import Column, String, Text, JSON, DateTime, Integer, Boolean, ForeignKey
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
    knowledge_bases = Column(JSON) # list[str], knowledge base IDs

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
    steps = Column(JSON)
    conclusions = Column(JSON)
    hypotheses = Column(JSON)
    action_items = Column(JSON)
    tools_used = Column(JSON)
    result_rows = Column(JSON)
    chart_specs = Column(JSON)
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
    tables = Column(JSON)
    status = Column(String(50))
    result_rows = Column(JSON)
    chart_specs = Column(JSON)
    selected_tables = Column(JSON)
    session_patches = Column(JSON)
    created_at = Column(String(50))
