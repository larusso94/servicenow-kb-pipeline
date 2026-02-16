"""Configuration loader for CU2 Cybersecurity KB Sync Azure Function.

Loads environment variables for the enhanced knowledge base synchronization system.
"""
import os
from dataclasses import dataclass, field
from typing import Optional

@dataclass(frozen=True)
class Config:
    """Configuration for Cybersecurity KB sync Azure Function."""
    
    # Cosmos DB Configuration
    cosmos_endpoint: Optional[str] = field(default_factory=lambda: os.getenv("COSMOS_ENDPOINT"))
    cosmos_key: Optional[str] = field(default_factory=lambda: os.getenv("COSMOS_KEY"))
    cosmos_database: str = field(default_factory=lambda: os.getenv("COSMOS_DATABASE", "itchatbot"))
    cosmos_container_vectors: str = field(default_factory=lambda: os.getenv("COSMOS_CONTAINER_VECTORS", "vstore"))
    partition_key: str = field(default_factory=lambda: os.getenv("COSMOS_PARTITION_KEY", "/id"))

    # Azure OpenAI Configuration
    openai_api_base: Optional[str] = field(default_factory=lambda: os.getenv("OPENAI_API_BASE"))
    openai_api_key: Optional[str] = field(default_factory=lambda: os.getenv("OPENAI_API_KEY"))
    openai_api_version: str = field(default_factory=lambda: os.getenv("OPENAI_API_VERSION", "2024-02-15-preview"))
    
    # OpenAI Deployments
    openai_embed_deployment: Optional[str] = field(default_factory=lambda: os.getenv("OPENAI_EMBED_DEPLOYMENT_NAME"))
    openai_deployment_name: Optional[str] = field(default_factory=lambda: os.getenv("OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini"))
    
    # Deployment aliases for backward compatibility and specific use cases
    @property
    def summary_deployment_name(self) -> Optional[str]:
        """LLM deployment for summary generation - uses main deployment by default."""
        return os.getenv("OPENAI_SUMMARY_DEPLOYMENT_NAME") or self.openai_deployment_name
    
    @property
    def chunk_model_name(self) -> str:
        """Model name for chunking token estimation - uses main deployment by default."""
        return os.getenv("CHUNK_MODEL_NAME") or self.openai_deployment_name or "gpt-4o-mini"
    
    # Embedding Configuration
    embedding_model_name: str = field(default_factory=lambda: os.getenv("EMBED_MODEL_NAME", "text-embedding-3-large"))
    embedding_dim: int = field(default_factory=lambda: int(os.getenv("EMBED_DIM", "1536")))
    embedding_fallback_prime_base: int = field(default_factory=lambda: int(os.getenv("EMBEDDING_FALLBACK_PRIME_BASE", "17")))

    # ServiceNow Configuration
    servicenow_instance_url: Optional[str] = field(default_factory=lambda: os.getenv("SERVICENOW_INSTANCE_URL"))
    servicenow_username: Optional[str] = field(default_factory=lambda: os.getenv("SERVICENOW_USERNAME"))
    servicenow_password: Optional[str] = field(default_factory=lambda: os.getenv("SERVICENOW_PASSWORD"))
    servicenow_token: Optional[str] = field(default_factory=lambda: os.getenv("SERVICENOW_TOKEN"))
    servicenow_oauth_client_id: Optional[str] = field(default_factory=lambda: os.getenv("SERVICENOW_OAUTH_CLIENT_ID"))
    servicenow_oauth_client_secret: Optional[str] = field(default_factory=lambda: os.getenv("SERVICENOW_OAUTH_CLIENT_SECRET"))
    servicenow_remote: bool = field(default_factory=lambda: os.getenv("SERVICENOW_REMOTE", "true").lower() in ["1", "true"])
    servicenow_timeout: float = field(default_factory=lambda: float(os.getenv("SERVICENOW_TIMEOUT", "30.0")))
    
    # ServiceNow KB Configuration
    servicenow_kb_table: str = field(default_factory=lambda: os.getenv("SERVICENOW_KB_TABLE", "kb_knowledge"))
    servicenow_kb_fields: str = field(default_factory=lambda: os.getenv("SERVICENOW_KB_FIELDS", "sys_id,number,short_description,text,sys_updated_on,keywords"))
    servicenow_page_size: int = field(default_factory=lambda: int(os.getenv("SERVICENOW_PAGE_SIZE", "100")))
    servicenow_kb_url_pattern: str = field(default_factory=lambda: os.getenv("SERVICENOW_KB_URL_PATTERN", "sp/{articlename}?id=kb_article_view&sysparm_article={article_id}"))

    # KB Processing Configuration
    kb_refresh_max_docs: int = field(default_factory=lambda: int(os.getenv("KB_REFRESH_MAX_DOCS", "0")))
    upsert_batch_size: int = field(default_factory=lambda: int(os.getenv("UPSERT_BATCH_SIZE", "25")))
    enable_document_summaries: bool = field(default_factory=lambda: os.getenv("ENABLE_DOCUMENT_SUMMARIES", "true").lower() in ["1", "true"])

    # Text Chunking Configuration
    chunk_target_tokens: int = field(default_factory=lambda: int(os.getenv("CHUNK_TARGET_TOKENS", "400")))
    chunk_max_tokens: int = field(default_factory=lambda: int(os.getenv("CHUNK_MAX_TOKENS", "450")))
    chunk_overlap_tokens: int = field(default_factory=lambda: int(os.getenv("CHUNK_OVERLAP_TOKENS", "50")))
    chunk_min_chunk_tokens: int = field(default_factory=lambda: int(os.getenv("CHUNK_MIN_CHUNK_TOKENS", "60")))
    
    # Token Processing Configuration
    tiktoken_default_encoding: str = field(default_factory=lambda: os.getenv("TIKTOKEN_DEFAULT_ENCODING", "cl100k_base"))
    token_estimation_ratio: int = field(default_factory=lambda: int(os.getenv("TOKEN_ESTIMATION_RATIO", "4")))

def load_config() -> Config:
    """Load configuration from environment variables."""
    return Config()

# Global config instance
config = load_config()

__all__ = ["Config", "load_config", "config"]