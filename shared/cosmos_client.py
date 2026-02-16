"""Cosmos DB vector store client for Azure Functions (adapted from CU2).

Features:
- Vector storage with automatic container creation
- Connection resilience and error handling
- Batch upsert operations
- Document metadata tracking
- Key-value storage for sync timestamps
"""
from typing import Dict, Any, List, Optional
import time
from .config import config
from .logging_client import log

try:
    from azure.identity import DefaultAzureCredential
    from azure.cosmos import CosmosClient, PartitionKey
    HAS_COSMOS = True
except ImportError:
    CosmosClient = None
    PartitionKey = None 
    DefaultAzureCredential = None
    HAS_COSMOS = False

class CosmosVectorStore:
    """Vector store using Cosmos DB with connection resilience."""
    
    def __init__(self, 
                 db_name: Optional[str] = None, 
                 container_name: Optional[str] = None, 
                 partition_key: Optional[str] = None):
        
        self.endpoint = config.cosmos_endpoint
        self.key = config.cosmos_key
        self.database_name = db_name or config.cosmos_database
        self.container_name = container_name or config.cosmos_container_vectors
        self.partition_key = partition_key or config.partition_key

        # Initialize as disconnected
        self.client = None
        self.database = None
        self.container = None
        self.connected = False

        # Attempt connection without stopping runtime
        self._try_connect()

    def _try_connect(self):
        """Attempt to connect to Cosmos DB without blocking runtime."""
        # Check critical configuration
        if not self.endpoint or not self.database_name:
            log.warning("cosmos.config.incomplete",
                       has_endpoint=bool(self.endpoint),
                       has_key=bool(self.key), 
                       has_database=bool(self.database_name))
            return

        # Check SDK availability
        if not HAS_COSMOS:
            log.error("cosmos.sdk.unavailable",
                     message="azure-cosmos package not installed")
            return

        try:
            # Create client with key or managed identity
            if self.key:
                self.client = CosmosClient(self.endpoint, credential=self.key)
            else:
                self.client = CosmosClient(self.endpoint, credential=DefaultAzureCredential())

            # Create database and container if they don't exist
            self.database = self.client.create_database_if_not_exists(id=self.database_name)
            self.container = self.database.create_container_if_not_exists(
                id=self.container_name, 
                partition_key=PartitionKey(path=self.partition_key)
            )

            # Quick connectivity test
            list(self.database.list_containers())
            self.connected = True

            log.info("cosmos.connection.success",
                    endpoint=self.endpoint[:50] + "..." if len(self.endpoint) > 50 else self.endpoint,
                    database=self.database_name,
                    container=self.container_name,
                    partition_key=self.partition_key)

        except Exception as e:
            log.error("cosmos.connection.failed",
                     error=str(e)[:200],
                     error_type=type(e).__name__,
                     endpoint=self.endpoint[:50] + "..." if self.endpoint else "none")

    def is_available(self) -> bool:
        """Check if the vector store is available."""
        return self.connected and self.client is not None

    def upsert_many(self, docs: List[Dict[str, Any]]):
        """Upsert multiple documents to Cosmos DB container."""
        if not self.is_available():
            log.error("cosmos.upsert.unavailable", count=len(docs))
            return

        success_count = 0
        start_time = time.time()
        
        for doc in docs:
            try:
                self.container.upsert_item(doc)
                success_count += 1
                log.debug("cosmos.upsert.success", 
                         id=doc.get("id", "unknown"),
                         sys_id=doc.get("sys_id", "unknown"))
            except Exception as e:
                log.error("cosmos.upsert.error", 
                         id=doc.get("id", "unknown"),
                         sys_id=doc.get("sys_id", "unknown"),
                         error=str(e)[:200],
                         error_type=type(e).__name__)
        
        elapsed_ms = int((time.time() - start_time) * 1000)
        log.info("cosmos.upsert.batch_complete",
                total=len(docs),
                success=success_count,
                failed=len(docs) - success_count,
                elapsed_ms=elapsed_ms)

    def delete_doc_chunks(self, sys_id: str):
        """Delete all chunks for a specific document sys_id."""
        if not self.is_available():
            log.error("cosmos.delete.unavailable", sys_id=sys_id)
            return

        # Query for all chunks with this sys_id
        query = "SELECT c.id FROM c WHERE c.sys_id = @sys_id"
        deleted_count = 0
        
        try:
            chunk_ids = [r["id"] for r in self.container.query_items(
                query=query,
                parameters=[{"name": "@sys_id", "value": sys_id}],
                enable_cross_partition_query=True
            )]
            
            for chunk_id in chunk_ids:
                try:
                    # Use the partition key value for deletion
                    self.container.delete_item(item=chunk_id, partition_key="kb")
                    deleted_count += 1
                    log.debug("cosmos.delete.success", 
                             id=chunk_id, 
                             sys_id=sys_id)
                except Exception as e:
                    log.warning("cosmos.delete.error", 
                               id=chunk_id,
                               sys_id=sys_id,
                               error=str(e)[:200],
                               error_type=type(e).__name__)
            
            log.info("cosmos.delete.doc_complete",
                    sys_id=sys_id,
                    total_chunks=len(chunk_ids),
                    deleted=deleted_count,
                    failed=len(chunk_ids) - deleted_count)
                    
        except Exception as e:
            log.error("cosmos.delete.query_error", 
                     sys_id=sys_id,
                     error=str(e)[:200],
                     error_type=type(e).__name__)

    def get_by_id_prefix(self, sys_id: str) -> Optional[Dict[str, Any]]:
        """Get first document matching sys_id prefix (for checking updates)."""
        if not self.is_available():
            log.error("cosmos.get_prefix.unavailable", sys_id=sys_id)
            return None

        query = "SELECT TOP 1 * FROM c WHERE STARTSWITH(c.id, @prefix)"
        try:
            items = list(self.container.query_items(
                query=query,
                parameters=[{"name": "@prefix", "value": f"{sys_id}::"}],
                enable_cross_partition_query=True
            ))
            
            result = items[0] if items else None
            log.debug("cosmos.get_prefix.result", 
                     sys_id=sys_id,
                     found=bool(result))
            return result
            
        except Exception as e:
            log.error("cosmos.get_prefix.error", 
                     sys_id=sys_id,
                     error=str(e)[:200],
                     error_type=type(e).__name__)
            return None

    def get_kv(self, key: str) -> Optional[Dict[str, Any]]:
        """Get key-value pair from storage."""
        if not self.is_available():
            log.error("cosmos.get_kv.unavailable", key=key)
            return None

        query = "SELECT TOP 1 * FROM c WHERE c.id = @id"
        try:
            items = list(self.container.query_items(
                query=query,
                parameters=[{"name": "@id", "value": key}],
                enable_cross_partition_query=True
            ))
            
            result = items[0] if items else None
            log.debug("cosmos.get_kv.result", 
                     key=key,
                     found=bool(result))
            return result
            
        except Exception as e:
            log.error("cosmos.get_kv.error", 
                     key=key,
                     error=str(e)[:200],
                     error_type=type(e).__name__)
            return None

    def set_kv(self, key: str, value: Dict[str, Any]):
        """Set key-value pair in storage."""
        if not self.is_available():
            log.error("cosmos.set_kv.unavailable", key=key)
            return

        doc = {
            "id": key,
            **value
        }
        
        try:
            self.container.upsert_item(doc)
            log.debug("cosmos.set_kv.success", key=key)
        except Exception as e:
            log.error("cosmos.set_kv.error", 
                     key=key,
                     error=str(e)[:200],
                     error_type=type(e).__name__)



# Backward compatibility alias
CosmosKV = CosmosVectorStore

__all__ = ["CosmosVectorStore", "CosmosKV"]
