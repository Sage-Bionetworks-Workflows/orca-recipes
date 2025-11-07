from .connector import get_connection
from .utils import write_to_snowflake, table_exists

import logging

# Configure module-level logger
logger = logging.getLogger("snowflake")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

__all__ = ["logger", "get_connection", "write_to_snowflake", "table_exists"]
