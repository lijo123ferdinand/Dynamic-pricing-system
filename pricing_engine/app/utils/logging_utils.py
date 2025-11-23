import logging
import sys
from .time_utils import utcnow_str
from app.config import Config

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger = logging.LoggerAdapter(logger, extra={"app": "pricing_engine"})
    return logger

def log_with_ts(logger: logging.Logger, level: str, message: str, **kwargs):
    ts = utcnow_str()
    msg = f"{ts} | {message} | {kwargs}"
    getattr(logger, level.lower())(msg)
