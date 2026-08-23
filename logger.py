# src/data_pipeline/logger.py
import logging
import sys
from config.settings import settings


def setup_logger(name: str = __name__) -> logging.Logger:
    """
    Setup logger with consistent formatting
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, settings.log_level))

    # Console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, settings.log_level))

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


# Default logger instance
logger = setup_logger("data_pipeline")
