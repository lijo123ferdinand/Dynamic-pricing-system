import pymysql
from queue import Queue, Empty
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

from .config import Config
from .utils.logging_utils import get_logger

logger = get_logger(__name__)

class ConnectionPool:
    def __init__(self, minconn: int = 1, maxconn: int = 5):
        self.minconn = minconn
        self.maxconn = maxconn
        self.pool: "Queue[pymysql.connections.Connection]" = Queue(maxconn)
        for _ in range(minconn):
            self.pool.put(self._create_connection())

    def _create_connection(self) -> pymysql.connections.Connection:
        conn = pymysql.connect(
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            database=Config.DB_NAME,
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )
        return conn

    @contextmanager
    def get_connection(self):
        try:
            conn = self.pool.get_nowait()
        except Empty:
            conn = self._create_connection()
        try:
            yield conn
        finally:
            try:
                self.pool.put_nowait(conn)
            except:
                conn.close()

    def close_all(self):
        while not self.pool.empty():
            conn = self.pool.get()
            conn.close()

pool = ConnectionPool(minconn=1, maxconn=10)

def execute_query(
    sql: str,
    params: Optional[Tuple[Any, ...]] = None,
    fetch: str = "none",
) -> Optional[List[Dict[str, Any]]]:
    logger.debug(f"Executing SQL: {sql} | params={params}")
    with pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            if fetch == "one":
                return cur.fetchone()
            elif fetch == "all":
                return cur.fetchall()
    return None

def fetch_one(sql: str, params: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    return execute_query(sql, params, fetch="one")

def fetch_all(sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    result = execute_query(sql, params, fetch="all")
    return result or []
