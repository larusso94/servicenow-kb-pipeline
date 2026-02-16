"""Cybersecurity KB Sync Timer Azure Function (enhanced v2.0).

Timer-triggered function that syncs ServiceNow cybersecurity KB articles to Cosmos DB
with embeddings for semantic search. Enhanced with improved URL handling, better error
recovery, performance optimizations, and comprehensive logging.

Features:
- Incremental sync with timestamp tracking
- Batch processing for improved performance  
- Enhanced error handling and recovery
- Proper URL construction for ServiceNow articles
- Configurable chunking and embedding parameters
- Comprehensive logging for monitoring and troubleshooting
"""
import datetime as dt
import logging
import re
from typing import Dict, List, Any, Optional

import azure.functions as func

from ..shared.config import config
from ..shared.logging_client import log
from ..shared.chunking import DocumentChunker, ChunkingConfig
from ..shared.embeddings import EmbeddingsClient
from ..shared.cosmos_client import CosmosVectorStore
from ..shared.servicenow_client import ServiceNowKBClient

def _article_id(article: Dict[str, Any]) -> str:
    """Extract article ID from ServiceNow article."""
    return article.get("sys_id", "unknown")

def _article_updated(article: Dict[str, Any]) -> str:
    """Extract update timestamp from ServiceNow article."""
    return article.get("sys_updated_on", "")

def _generate_article_slug(short_description: str) -> str:
    """Generate URL-friendly slug from article short description."""
    if not short_description:
        return "article"
    
    # Convert to lowercase and replace spaces with hyphens
    slug = short_description.lower().strip()
    
    # Remove special characters except hyphens, keep alphanumeric and spaces
    slug = re.sub(r'[^a-z0-9\s\-]', '', slug)
    
    # Replace multiple spaces/hyphens with single hyphen
    slug = re.sub(r'[\s\-]+', '-', slug)
    
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    
    # Fallback if slug becomes empty
    return slug if slug else "article"

def _compose_document(article: Dict[str, Any], chunk: str, chunk_id: int, vector: List[float]) -> Dict[str, Any]:
    """Compose Cosmos DB document from article, chunk, and vector with enhanced metadata."""
    # Construct user-accessible ServiceNow KB article URL
    instance_url = config.servicenow_instance_url.rstrip('/') if config.servicenow_instance_url else ""
    
    # Generate article slug from short_description for URL
    article_slug = _generate_article_slug(article.get('short_description', ''))
    
    # Use article number (like KB0010207) as article_id, fallback to sys_id
    article_id = article.get('number', article.get('sys_id', ''))
    
    url_pattern = config.servicenow_kb_url_pattern.format(
        articlename=article_slug,
        article_id=article_id
    )
    article_url = f"{instance_url}/{url_pattern}"
    
    # Enhanced metadata with essential fields only
    article_number = article.get("number", "").strip()
    
    document_id = f"{article_number}::{chunk_id}"
    
    return {
        "id": document_id,
        "number": article_number,
        "title": article.get("short_description", "").strip(),
        "text_chunk": chunk.strip(),
        "chunk_id": chunk_id,
        "sys_updated_on": _article_updated(article),
        "created_on": dt.datetime.utcnow().isoformat() + "Z",
        "metadata": {
            "article_url": article_url,
            "text_length": len(chunk)
        },
        "vector": vector
    }

def _needs_update(existing: Optional[Dict[str, Any]], updated_on: str) -> bool:
    """Check if article needs to be updated based on timestamp."""
    if not existing:
        return True
    return existing.get("sys_updated_on", "") != updated_on

def _html_to_markdown(html_content: str) -> str:
    """Convert HTML content to clean markdown format using html2text."""
    if not html_content or not html_content.strip():
        return ""
    
    try:
        import html2text
        
        # Configure html2text for optimal output
        h = html2text.HTML2Text()
        h.ignore_links = False
        h.ignore_images = True
        h.body_width = 0  # No line wrapping
        h.ignore_emphasis = False
        h.single_line_break = False  # Use double line breaks for paragraphs
        h.protect_links = True
        h.wrap_links = False
        
        # Convert HTML to markdown
        markdown = h.handle(html_content)
        markdown = markdown.strip()
        
        # Clean up excessive whitespace
        import re
        markdown = re.sub(r'\n\s*\n\s*\n', '\n\n', markdown)  # Multiple newlines to double
        markdown = re.sub(r'[ \t]+', ' ', markdown)  # Multiple spaces to single
        
        log.debug("html_to_markdown.success", 
                 html_length=len(html_content),
                 markdown_length=len(markdown))
        
        return markdown
        
    except Exception as e:
        log.error("html_to_markdown.error", error=str(e)[:200])
        # Return cleaned HTML as last resort
        import re
        import html
        
        # Basic cleanup: remove tags and decode entities
        text = re.sub(r'<[^>]+>', ' ', html_content)
        text = html.unescape(text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

def _generate_summary(title: str, content: str, embed_client: EmbeddingsClient) -> str:
    """Generate a summary of the article using LLM."""
    if not content or not content.strip():
        return ""
    
    try:
        # Create prompt for summarization
        summary_prompt = f"""Please provide a concise summary of the following knowledge base article in 2-3 sentences. Focus on the main purpose, key steps, and important details.

Title: {title}

Content: {content}

Summary:"""
        
        # Use OpenAI client for summary generation
        if hasattr(embed_client, '_client') and embed_client._client:
            try:
                response = embed_client._client.chat.completions.create(
                    model=config.summary_deployment_name or config.openai_deployment_name or "gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are a helpful assistant that creates concise summaries of knowledge base articles."},
                        {"role": "user", "content": summary_prompt}
                    ],
                    max_tokens=150,
                    temperature=0.3
                )
                return response.choices[0].message.content.strip()
            except Exception as e:
                log.warning("summary.generation.failed", error=str(e)[:200])
                return ""
        else:
            log.warning("summary.generation.no_client")
            return ""
    except Exception as e:
        log.warning("summary.generation.error", error=str(e)[:200])
        return ""

def _iso_utc_now() -> str:
    """Generate current UTC timestamp in ISO format."""
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def main(myTimer: func.TimerRequest) -> None:
    """Main Azure Function entry point for enhanced KB sync timer."""
    start_time = dt.datetime.utcnow()
    
    try:
        log.info("cybersecurity.kb_sync_timer.start", 
                start_time=start_time.isoformat() + "Z",
                past_due=myTimer.past_due,
                schedule_status=str(myTimer.schedule_status) if myTimer.schedule_status else None,
                service="cybersecurity_kb_sync",
                version="2.0")
        
        # Initialize lightweight components first (ServiceNow and Cosmos only)
        # Delay expensive initialization (embeddings, chunker) until needed
        sn_client = ServiceNowKBClient()
        cosmos_store = CosmosVectorStore()
        
        log.info("components.init.lightweight",
                sn_available=sn_client.is_available(),
                cosmos_available=cosmos_store.is_available())
        
        # Get last sync timestamp for incremental updates
        last_sync = cosmos_store.get_kv("__kb_last_sync_ts__")
        updated_since = last_sync.get("value") if last_sync else None
        
        log.info("cybersecurity.kb_sync_timer.incremental_check", 
                last_sync_timestamp=updated_since,
                mode="incremental" if updated_since else "full",
                knowledge_base="cybersecurity")
        
        # Fetch articles from ServiceNow
        limit = config.kb_refresh_max_docs if config.kb_refresh_max_docs > 0 else None
        
        articles = sn_client.list_kb_articles_since(updated_since, limit=limit)

        log.info("cybersecurity.kb_sync_timer.articles_fetched", 
                count=len(articles),
                updated_since=updated_since,
                limit=limit,
                knowledge_base="cybersecurity")
        
        # EARLY EXIT: No articles to process - skip expensive initialization
        if not articles:
            end_time = dt.datetime.utcnow()
            duration_seconds = (end_time - start_time).total_seconds()
            log.info("cybersecurity.kb_sync_timer.no_updates", 
                    message="No cybersecurity articles to process - exiting early",
                    duration_seconds=round(duration_seconds, 2),
                    resources_saved="embeddings_client_not_initialized,chunker_not_initialized",
                    knowledge_base="cybersecurity")
            return
        
        # Only initialize expensive components when we have articles to process
        log.info("components.init.expensive", 
                message="Initializing embeddings and chunker for article processing")
        
        embed_client = EmbeddingsClient()
        chunking_config = ChunkingConfig(
            target_tokens=config.chunk_target_tokens,
            max_tokens=config.chunk_max_tokens,
            overlap_tokens=config.chunk_overlap_tokens,
            min_chunk_tokens=config.chunk_min_chunk_tokens
        )
        chunker = DocumentChunker(chunking_config)
        
        log.info("components.init.complete",
                embeddings_ready=True,
                chunker_ready=True)
        
        # Process articles with enhanced pipeline
        total_processed = 0
        total_chunks = 0
        total_skipped = 0
        total_errors = 0
        to_upsert: List[Dict[str, Any]] = []
        
        for idx, article in enumerate(articles):
            sys_id = _article_id(article)
            if not sys_id or sys_id == "unknown":
                log.warning("kb_sync_timer.article.invalid_id", 
                           article_number=article.get("number", "unknown"),
                           available_fields=list(article.keys()))
                total_skipped += 1
                continue
            
            log.debug("kb_sync_timer.article.processing", 
                     progress=f"{idx + 1}/{len(articles)}",
                     sys_id=sys_id,
                     number=article.get("number", ""),
                     title=article.get("short_description", "")[:50])
            
            # Check if article needs update (incremental sync)
            existing = cosmos_store.get_by_id_prefix(sys_id)
            is_new_or_updated = _needs_update(existing, _article_updated(article))
            
            if not is_new_or_updated:
                log.debug("kb_sync_timer.article.skip", 
                         sys_id=sys_id,
                         reason="no_update_needed")
                total_skipped += 1
                continue
            
            # Delete existing chunks for this article
            if existing:
                cosmos_store.delete_doc_chunks(sys_id)
            
            # Get article content
            title = article.get("short_description", "")
            body_html = article.get("text", "")
            
            # Convert HTML content to markdown
            body_markdown = _html_to_markdown(body_html) if body_html else ""
            
            if not body_markdown.strip():
                log.warning("kb_sync_timer.article.empty_content", 
                           sys_id=sys_id,
                           title_length=len(title),
                           body_length=len(body_html))
                total_skipped += 1
                continue
            
            # Generate article summary using LLM only for new or updated articles
            article_summary = ""
            if is_new_or_updated:
                article_summary = _generate_summary(title, body_markdown, embed_client)
                if article_summary:
                    log.info("kb_sync_timer.summary.generated",
                            sys_id=sys_id,
                            article_number=article.get("number", ""),
                            summary_length=len(article_summary),
                            reason="new_or_updated_article")
                else:
                    log.warning("kb_sync_timer.summary.generation_failed",
                               sys_id=sys_id,
                               reason="openai_unavailable_or_error")
            
            # Extract article metadata
            article_number = article.get("number", "")
            keywords = article.get("keywords", "")
            
            # Create metadata prefix for embedding enrichment
            metadata_elements = []
            if article_number and article_number.strip():
                metadata_elements.append(f"Document: {article_number.strip()}")
            if title and title.strip():
                metadata_elements.append(f"Title: {title.strip()}")
            if keywords and keywords.strip():
                clean_keywords = keywords.strip().replace(",", ", ")
                metadata_elements.append(f"Keywords: {clean_keywords}")
            
            metadata_prefix = " | ".join(metadata_elements)
            
            # Split article into chunks
            chunks_data = chunker.chunk(body_markdown)
            
            if not chunks_data:
                log.warning("kb_sync_timer.article.no_chunks", 
                           sys_id=sys_id,
                           content_length=len(body_markdown))
                total_skipped += 1
                continue
            
            # Prepare all chunks (content + summary)
            all_chunks = []
            
            # Add regular content chunks
            for i, chunk in enumerate(chunks_data):
                all_chunks.append({
                    "text": chunk["text"],
                    "chunk_id": i,
                    "chunk_type": "content"
                })
            
            # Add summary as additional chunk
            if article_summary and article_summary.strip():
                summary_chunk_id = len(all_chunks)
                all_chunks.append({
                    "text": f"SUMMARY: {article_summary.strip()}",
                    "chunk_id": summary_chunk_id,
                    "chunk_type": "summary"
                })
            
            # Prepare enriched chunks for embedding
            chunk_texts_for_embedding = []
            for chunk_data in all_chunks:
                chunk_text = chunk_data["text"]
                
                # Append metadata to chunk for embedding
                embedding_parts = []
                if metadata_prefix:
                    embedding_parts.append(f"[{metadata_prefix}]")
                embedding_parts.append(chunk_text)
                
                enriched_chunk = "\n".join(embedding_parts)
                chunk_texts_for_embedding.append(enriched_chunk)
            
            log.debug("kb_sync_timer.embedding_preparation",
                     sys_id=sys_id,
                     metadata_elements_count=len(metadata_elements),
                     total_chunks=len(all_chunks),
                     content_chunks=len([c for c in all_chunks if c["chunk_type"] == "content"]),
                     summary_chunks=len([c for c in all_chunks if c["chunk_type"] == "summary"]),
                     has_summary=bool(article_summary and article_summary.strip()),
                     sample_embedding_text=chunk_texts_for_embedding[0][:200] + "..." if chunk_texts_for_embedding else "")
            
            if not chunk_texts_for_embedding:
                log.warning("kb_sync_timer.article.no_embedding_chunks", 
                           sys_id=sys_id,
                           content_length=len(body_markdown))
                total_skipped += 1
                continue
            
            # Generate embeddings for all chunks
            try:
                vectors = embed_client.embed_texts(chunk_texts_for_embedding)
                
                if len(vectors) != len(chunk_texts_for_embedding):
                    log.error("kb_sync_timer.article.embedding_mismatch",
                             sys_id=sys_id,
                             chunks=len(chunk_texts_for_embedding),
                             vectors=len(vectors))
                    total_errors += 1
                    continue
                
                # Create documents for vector database
                for i, vector in enumerate(vectors):
                    chunk_data = all_chunks[i]
                    clean_chunk_text = chunk_data["text"]
                    chunk_id = chunk_data["chunk_id"]
                    chunk_type = chunk_data["chunk_type"]
                    
                    doc = _compose_document(article, clean_chunk_text, chunk_id, vector)
                    
                    # Enhanced metadata with processing context
                    doc["metadata"]["metadata_fields_used"] = len(metadata_elements)
                    doc["metadata"]["has_keywords"] = bool(keywords and keywords.strip())
                    doc["metadata"]["chunk_type"] = chunk_type
                    if chunk_type == "summary":
                        doc["metadata"]["is_summary"] = True
                    
                    to_upsert.append(doc)
                
                total_chunks += len(chunk_texts_for_embedding)
                total_processed += 1
                
                log.debug("kb_sync_timer.article.processed",
                         sys_id=sys_id,
                         chunks=len(chunk_texts_for_embedding),
                         vectors=len(vectors),
                         metadata_elements=len(metadata_elements),
                         enriched_embeddings=bool(metadata_prefix))
                
                # Batch upsert when reaching configured batch size
                if len(to_upsert) >= config.upsert_batch_size:
                    cosmos_store.upsert_many(to_upsert)
                    log.info("kb_sync_timer.batch_upsert", 
                            batch_size=len(to_upsert),
                            total_processed=total_processed)
                    to_upsert = []
                    
            except Exception as e:
                log.error("kb_sync_timer.article.embedding_error",
                         sys_id=sys_id,
                         progress=f"{idx + 1}/{len(articles)}",
                         error=str(e)[:200],
                         error_type=type(e).__name__)
                total_errors += 1
                continue
        
        # Final batch upsert
        if to_upsert:
            cosmos_store.upsert_many(to_upsert)
            log.info("kb_sync_timer.final_batch_upsert", 
                    batch_size=len(to_upsert))
        
        # Update last sync timestamp
        current_timestamp = _iso_utc_now()
        cosmos_store.set_kv("__kb_last_sync_ts__", {"value": current_timestamp})
        
        # Calculate execution metrics
        end_time = dt.datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()
        
        log.info("cybersecurity.kb_sync_timer.complete",
                start_time=start_time.isoformat() + "Z",
                end_time=end_time.isoformat() + "Z", 
                duration_seconds=round(duration_seconds, 2),
                total_articles=len(articles),
                processed_articles=total_processed,
                skipped_articles=total_skipped,
                error_articles=total_errors,
                total_chunks=total_chunks,
                chunks_per_second=round(total_chunks / duration_seconds, 2) if duration_seconds > 0 else 0,
                last_sync_updated=current_timestamp,
                knowledge_base="cybersecurity",
                service="cybersecurity_kb_sync",
                version="2.0")
                
    except Exception as e:
        end_time = dt.datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()
        
        log.error("cybersecurity.kb_sync_timer.error",
                 start_time=start_time.isoformat() + "Z",
                 end_time=end_time.isoformat() + "Z",
                 duration_seconds=round(duration_seconds, 2),
                 error=str(e)[:500],
                 error_type=type(e).__name__,
                 service="cybersecurity_kb_sync",
                 version="2.0")
        raise
