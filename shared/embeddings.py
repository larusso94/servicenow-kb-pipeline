"""Embeddings client for Azure Functions (adapted from CU2).

Features:
- Azure OpenAI embeddings integration
- Batch processing with configurable sizes
- Fallback deterministic vectors for testing
- Robust error handling and retry logic
"""
import time
from typing import List, Optional
from .config import config
from .logging_client import log

try:
    from openai import AzureOpenAI
    HAS_OPENAI = True
except ImportError:
    AzureOpenAI = None
    HAS_OPENAI = False



class EmbeddingsClient:
    """Embeddings client using Azure OpenAI with fallback support."""
    
    def __init__(self, 
                 endpoint: Optional[str] = None,
                 api_key: Optional[str] = None,
                 deployment: Optional[str] = None,
                 api_version: Optional[str] = None):
        
        self.endpoint = endpoint or config.openai_api_base
        self.api_key = api_key or config.openai_api_key
        self.deployment = deployment or config.openai_embed_deployment
        self.api_version = api_version or config.openai_api_version
        self.dim = config.embedding_dim
        
        self._client = None
        
        # Try to initialize Azure OpenAI client
        if HAS_OPENAI and self.endpoint and self.api_key and self.deployment:
            try:
                self._client = AzureOpenAI(
                    api_key=self.api_key,
                    api_version=self.api_version,
                    azure_endpoint=self.endpoint,
                )
                log.info("embeddings.client.init.success", 
                        deployment=self.deployment,
                        api_version=self.api_version,
                        client_type="openai")
            except Exception as e:
                log.warning("embeddings.client.init.failed", 
                           error=str(e),
                           fallback="requests")
                self._client = None
        
        if not self._client:
            log.info("embeddings.client.init.fallback", 
                    has_endpoint=bool(self.endpoint),
                    has_api_key=bool(self.api_key),
                    has_deployment=bool(self.deployment),
                    client_type="fallback")

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a list of texts."""
        if not texts:
            return []
            
        start = time.time()
        mode = "openai" if self._client else "fallback"
        
        log.info("embeddings.batch.start", 
                count=len(texts),
                mode=mode,
                deployment=self.deployment)
        
        try:
            if self._client:
                vectors = self._embed_with_openai(texts)
            else:
                vectors = self._embed_fallback(texts)
                
            # Validate and adjust dimensions
            vectors = self._validate_dimensions(vectors)
            
            elapsed_ms = int((time.time() - start) * 1000)
            log.info("embeddings.batch.success", 
                    count=len(vectors),
                    elapsed_ms=elapsed_ms,
                    avg_dim=len(vectors[0]) if vectors else 0)
            
            return vectors
            
        except Exception as e:
            log.error("embeddings.batch.error", 
                     error=str(e),
                     error_type=type(e).__name__,
                     fallback=True)
            return self._embed_fallback(texts)

    def _embed_with_openai(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using Azure OpenAI client."""
        response = self._client.embeddings.create(
            model=self.deployment,
            input=texts
        )
        
        vectors = []
        for data_item in response.data:
            vectors.append(data_item.embedding)
            
        return vectors



    def _embed_fallback(self, texts: List[str]) -> List[List[float]]:
        """Generate deterministic fallback vectors when OpenAI is unavailable."""
        log.warning("embeddings.fallback.used", count=len(texts))
        
        vectors = []
        for i, text in enumerate(texts):
            # Create deterministic vector based on text content
            base = sum(ord(c) for c in text) or 1
            prime_base = config.embedding_fallback_prime_base
            vector = [((base * (j + i + 1)) % prime_base) / prime_base for j in range(self.dim)]
            vectors.append(vector)
            
        return vectors

    def _validate_dimensions(self, vectors: List[List[float]]) -> List[List[float]]:
        """Validate and adjust vector dimensions."""
        if not vectors:
            return vectors
            
        current_dim = len(vectors[0])
        if current_dim == self.dim:
            return vectors
            
        log.warning("embeddings.dimension.mismatch", 
                   expected=self.dim,
                   actual=current_dim,
                   adjusting=True)
        
        adjusted = []
        for vector in vectors:
            if len(vector) > self.dim:
                # Truncate
                adjusted.append(vector[:self.dim])
            elif len(vector) < self.dim:
                # Pad with zeros
                padded = vector + [0.0] * (self.dim - len(vector))
                adjusted.append(padded)
            else:
                adjusted.append(vector)
                
        return adjusted

__all__ = ["EmbeddingsClient"]
