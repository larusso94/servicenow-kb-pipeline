"""Logging client for Cybersecurity KB Azure Functions (adapted from CU2).

Integrates with Azure Functions logging system and Application Insights.
Falls back to structured JSON logging if advanced logging is not available.
Optimized for cybersecurity knowledge base operations.
"""
import logging
import os
import json
from datetime import datetime
from typing import Any, Dict

try:
    import structlog
    HAS_STRUCTLOG = True
except ImportError:
    structlog = None
    HAS_STRUCTLOG = False

def setup_function_logger():
    """Configure and return a logger optimized for Azure Functions.

    Uses the Azure Functions logging system which automatically integrates
    with Application Insights when configured.
    """
    # Get the Azure Functions logger
    func_logger = logging.getLogger("azure.functions.worker")
    
    # Set level based on environment
    log_level = os.getenv("FUNCTIONS_LOG_LEVEL", "INFO").upper()
    func_logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    if not HAS_STRUCTLOG:
        return _create_fallback_logger(func_logger)

    # Configure structlog to work with Azure Functions
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer() if os.getenv("FUNCTIONS_WORKER_RUNTIME") == "python" else structlog.processors.JSONRenderer()
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level, logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logger = structlog.get_logger()
    logger.info("cybersecurity.kb.logging.initialized", 
                level=log_level,
                structlog_available=True,
                azure_functions=True,
                service="cybersecurity_kb_sync")
    
    return logger


def _create_fallback_logger(base_logger: logging.Logger):
    """Create a fallback structured logger when structlog is not available."""
    
    class AzureFunctionsFallbackLogger:
        def __init__(self, logger: logging.Logger):
            self.logger = logger
            
        def _log(self, level: str, event: str, **fields):
            # Create structured log entry
            record = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "level": level,
                "event": event,
                **fields,
            }
            
            # Use Azure Functions logger with structured message
            log_method = getattr(self.logger, level.lower(), self.logger.info)
            log_method(json.dumps(record, ensure_ascii=False))
            
        def debug(self, event: str, **fields): self._log("DEBUG", event, **fields)
        def info(self, event: str, **fields): self._log("INFO", event, **fields)
        def warning(self, event: str, **fields): self._log("WARNING", event, **fields)
        def error(self, event: str, **fields): self._log("ERROR", event, **fields)

    logger = AzureFunctionsFallbackLogger(base_logger)
    logger.info("cybersecurity.kb.logging.initialized", 
                log_level=base_logger.level,
                structlog_available=False,
                azure_functions=True,
                service="cybersecurity_kb_sync")
    
    return logger


def get_logger():
    """Get the configured logger for Azure Functions."""
    return setup_function_logger()


# Initialize logger for Azure Functions
log = setup_function_logger()

__all__ = ["log", "get_logger", "setup_function_logger"]