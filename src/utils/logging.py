"""
Logging utilities for the ETL pipeline.
Provides structured logging with different log levels and file outputs.
"""

import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
import structlog
from .config import get_config

# --- Logger setup helpers ---
def _get_file_logger(name: str, file_path: str, level: int) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.FileHandler(file_path)
        handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

def _get_console_logger(name: str, level: int) -> logging.Logger:
    logger = logging.getLogger(name + "_console")
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

class ETLLogger:
    """Custom logger for ETL pipeline with structured logging capabilities."""
    def __init__(self, name: str = "ETLPipeline"):
        self.name = name
        self.config = get_config().get_logging_config()
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        # Set up separate loggers
        self.pipeline_logger = _get_file_logger("pipeline", self.config.get('file_path', 'logs/pipeline.log'), logging.INFO)
        self.currency_logger = _get_file_logger("currency", self.config.get('currency_file_path', 'logs/currency.log'), logging.INFO)
        self.error_logger = _get_file_logger("errors", self.config.get('error_file_path', 'logs/errors.log'), logging.ERROR)
        self.console_logger = _get_console_logger("console", logging.INFO)
        self.error_count = 0
        self.warning_count = 0
    def info(self, message: str, **kwargs: Any) -> None:
        self.pipeline_logger.info(message)
        self.console_logger.info(message)
    def warning(self, message: str, **kwargs: Any) -> None:
        self.warning_count += 1
        self.pipeline_logger.warning(message)
        self.console_logger.warning(message)
    def error(self, message: str, error: Optional[Exception] = None, **kwargs: Any) -> None:
        self.error_count += 1
        msg = message
        if error:
            msg += f" | {type(error).__name__}: {error}"
        self.error_logger.error(msg)
        self.console_logger.error(msg)
    def debug(self, message: str, **kwargs: Any) -> None:
        self.pipeline_logger.debug(message)
        self.console_logger.debug(message)
    def critical(self, message: str, error: Optional[Exception] = None, **kwargs: Any) -> None:
        self.error_count += 1
        msg = message
        if error:
            msg += f" | {type(error).__name__}: {error}"
        self.error_logger.critical(msg)
        self.console_logger.critical(msg)
    def log_pipeline_start(self, pipeline_name: str, **kwargs: Any) -> None:
        self.info(f"Pipeline started: {pipeline_name}")
    def log_pipeline_end(self, pipeline_name: str, duration_seconds: float, **kwargs: Any) -> None:
        self.info(f"Pipeline completed: {pipeline_name} in {duration_seconds:.2f} seconds")
    def log_data_validation(self, record_count: int, valid_count: int, invalid_count: int, **kwargs: Any) -> None:
        error_rate = (invalid_count / record_count * 100) if record_count > 0 else 0
        self.info(f"Data validation completed: {valid_count}/{record_count} records valid ({error_rate:.2f}% error rate)")
    def log_currency_conversion(self, from_currency: str, to_currency: str, rate: float, **kwargs: Any) -> None:
        msg = f"Currency conversion: {from_currency} -> {to_currency} (rate: {rate})"
        self.currency_logger.info(msg)
        self.console_logger.info(msg)
    def get_error_summary(self) -> Dict[str, int]:
        return {
            'error_count': self.error_count,
            'warning_count': self.warning_count
        }

# Global logger instances
_global_loggers = {}

def get_logger(name: str = "ETLPipeline") -> ETLLogger:
    global _global_loggers
    if name not in _global_loggers:
        _global_loggers[name] = ETLLogger(name)
    return _global_loggers[name]

def reset_logger() -> None:
    global _global_loggers
    _global_loggers = {} 