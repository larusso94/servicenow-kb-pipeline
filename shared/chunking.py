"""Advanced chunking strategies for KB documents (adapted from CU2).

Goals:
- Token-aware sizing (using tiktoken) with soft limits.
- Sentence boundary preservation (simple split heuristics).
- Overlap configurable for better semantic continuity.

If tiktoken is unavailable, falls back to character/word counting.
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import List, Dict, Optional
from .config import config
from .logging_client import log

try:  
    import tiktoken  
except ImportError:  
    tiktoken = None  

@dataclass
class ChunkingConfig:
    target_tokens: int = config.chunk_target_tokens
    max_tokens: int = config.chunk_max_tokens
    overlap_tokens: int = config.chunk_overlap_tokens
    min_chunk_tokens: int = config.chunk_min_chunk_tokens
    model_name: str = config.chunk_model_name

class DocumentChunker:
    def __init__(self, chunking_config: Optional[ChunkingConfig] = None):
        self.config = chunking_config or ChunkingConfig(
            target_tokens=config.chunk_target_tokens,
            max_tokens=config.chunk_max_tokens,
            overlap_tokens=config.chunk_overlap_tokens,
            min_chunk_tokens=config.chunk_min_chunk_tokens,
            model_name=config.chunk_model_name
        )
        
        self._enc = None
        if tiktoken:
            try:
                self._enc = tiktoken.encoding_for_model(self.config.model_name)
                log.debug("chunking.tokenizer.init", model=self.config.model_name)
            except Exception as e:
                log.warning("chunking.tokenizer.fallback", error=str(e))
                try:
                    self._enc = tiktoken.get_encoding(config.tiktoken_default_encoding)
                except Exception:
                    self._enc = None

    def _count_tokens(self, text: str) -> int:
        if self._enc:
            return len(self._enc.encode(text))
        # Fallback: rough estimation
        return len(text) // config.token_estimation_ratio

    def _split_sentences(self, text: str) -> List[str]:
        # Basic sentence splitter - can be enhanced with spaCy or nltk
        parts = re.split(r'(?<=[.!?])\s+', text.strip())
        return [p.strip() for p in parts if p.strip()]

    def chunk(self, text: str) -> List[Dict]:
        sentences = self._split_sentences(text)
        cfg = self.config
        chunks: List[Dict] = []
        current: List[str] = []
        current_tokens = 0
        
        log.debug("chunking.start", 
                 text_length=len(text),
                 sentences=len(sentences),
                 target_tokens=cfg.target_tokens)
        
        for sent in sentences:
            stoks = self._count_tokens(sent)
            
            # If single sentence larger than max, hard-split by token length
            if stoks > cfg.max_tokens:
                hard_parts = self._split_hard(sent, cfg.max_tokens)
                for hp in hard_parts:
                    self._emit_chunk(chunks, hp)
                continue
                
            if current_tokens + stoks <= cfg.target_tokens:
                current.append(sent)
                current_tokens += stoks
            else:
                # finalize current
                if current:
                    self._emit_chunk(chunks, ' '.join(current))
                # start new
                current = [sent]
                current_tokens = stoks
                
        if current:
            self._emit_chunk(chunks, ' '.join(current))
            
        # Merge too-small tail chunks
        merged = self._merge_small(chunks, cfg.min_chunk_tokens)
        
        # Apply overlap by duplicating content from previous chunk
        final = self._apply_overlap(merged, cfg.overlap_tokens)
        
        log.info("chunking.complete", 
                initial_chunks=len(chunks),
                after_merge=len(merged),
                final_chunks=len(final),
                avg_tokens=sum(self._count_tokens(c["text"]) for c in final) // len(final) if final else 0)
        
        return final

    def _split_hard(self, sentence: str, max_tokens: int) -> List[str]:
        if not self._enc:
            # fallback char slicing
            size = max_tokens * config.token_estimation_ratio
            return [sentence[i:i+size] for i in range(0, len(sentence), size)]
            
        toks = self._enc.encode(sentence)
        parts = []
        for i in range(0, len(toks), max_tokens):
            sub = toks[i:i+max_tokens]
            parts.append(self._enc.decode(sub))
        return parts

    def _emit_chunk(self, out: List[Dict], text: str):
        out.append({"text": text, "chunk_index": len(out)})

    def _merge_small(self, chunks: List[Dict], min_tokens: int) -> List[Dict]:
        if not chunks:
            return []
            
        result: List[Dict] = []
        buffer: Optional[Dict] = None
        
        for c in chunks:
            toks = self._count_tokens(c["text"])
            if toks < min_tokens:
                if buffer is None:
                    buffer = c
                else:
                    buffer["text"] += " " + c["text"]
                    # check if merged now above threshold
                    if self._count_tokens(buffer["text"]) >= min_tokens:
                        result.append(buffer)
                        buffer = None
            else:
                if buffer:
                    result.append(buffer)
                    buffer = None
                result.append(c)
                
        if buffer:
            result.append(buffer)
            
        return result

    def _apply_overlap(self, chunks: List[Dict], overlap_tokens: int) -> List[Dict]:
        if overlap_tokens <= 0 or len(chunks) < 2:
            return chunks
            
        final: List[Dict] = []
        for i, c in enumerate(chunks):
            if i == 0:
                final.append({"text": c["text"], "chunk_index": i})
                continue
                
            prev = chunks[i-1]["text"]
            tail = self._tail_tokens(prev, overlap_tokens)
            combined = tail + " " + c["text"]
            final.append({"text": combined, "chunk_index": i})
            
        return final

    def _tail_tokens(self, text: str, overlap_tokens: int) -> str:
        if not self._enc:
            # crude: take last overlap_tokens*ratio chars
            approx = overlap_tokens * config.token_estimation_ratio
            return text[-approx:] if len(text) > approx else text
            
        toks = self._enc.encode(text)
        if len(toks) <= overlap_tokens:
            return text
        tail = toks[-overlap_tokens:]
        return self._enc.decode(tail)

__all__ = ["ChunkingConfig", "DocumentChunker"]
