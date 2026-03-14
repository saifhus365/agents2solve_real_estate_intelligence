"""Application settings loaded from environment variables via pydantic-settings."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central configuration – every field maps to an env var."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── NVIDIA LLM ──────────────────────────────────────────────────────
    nvidia_api_key: str = ""
    nvidia_base_url: str = "https://integrate.api.nvidia.com/v1"
    nvidia_model: str = "nvidia/llama-3.1-nemotron-70b-instruct"

    # ── DLD / Dubai Pulse ───────────────────────────────────────────────
    dld_client_id: str = ""
    dld_client_secret: str = ""
    dld_auth_url: str = (
        "https://api.dubaipulse.gov.ae/oauth/client_credential/accesstoken"
        "?grant_type=client_credentials"
    )
    dld_transactions_url: str = (
        "https://api.dubaipulse.gov.ae/open/dld/dld_transactions-open-api"
    )

    # ── CSV data directory (for offline / demo mode) ────────────────────
    data_dir: str = "data"

    # ── Pinecone ────────────────────────────────────────────────────────
    pinecone_api_key: str = ""
    pinecone_env: str = "us-east-1"
    pinecone_index: str = "dubai-realestate"

    # ── Neo4j AuraDB ────────────────────────────────────────────────────
    neo4j_uri: str = "neo4j+s://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = ""

    # ── PostgreSQL ──────────────────────────────────────────────────────
    postgres_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres"

    # ── Redis ───────────────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"

    # ── W&B ─────────────────────────────────────────────────────────────
    wandb_api_key: str = ""
    wandb_project: str = "dubai-copilot-eval"

    # ── App tuning ──────────────────────────────────────────────────────
    embedding_model: str = "BAAI/bge-large-en-v1.5"
    embedding_dim: int = 1024
    reranker_model: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"
    vector_top_k: int = 15
    rerank_top_k: int = 8
    chunk_size: int = 512
    chunk_overlap: int = 64

    @property
    def asyncpg_dsn(self) -> str:
        """Return the raw asyncpg DSN (strip the +asyncpg dialect marker)."""
        return self.postgres_url.replace("postgresql+asyncpg://", "postgresql://")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Singleton accessor for the application settings."""
    return Settings()
