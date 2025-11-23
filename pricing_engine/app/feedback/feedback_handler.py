from typing import Dict, Any, Optional

from app.db import execute_query, fetch_all
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)

def save_feedback(payload: Dict[str, Any]) -> None:
    sql = """
        INSERT INTO price_feedback
            (vendor_id, sku, suggested_price, action,
             custom_price, timestamp, suggestion_id)
        VALUES (%s, %s, %s, %s, %s, %s,
                (SELECT id FROM price_suggestions
                 WHERE sku = %s AND vendor_id = %s AND suggested_price = %s
                 ORDER BY created_at DESC LIMIT 1))
    """
    execute_query(
        sql,
        (
            payload["vendor_id"],
            payload["sku"],
            payload["suggested_price"],
            payload["action"],
            payload.get("custom_price"),
            payload["timestamp"],
            payload["sku"],
            payload["vendor_id"],
            payload["suggested_price"],
        ),
    )

    # Update suggestion status
    status = "PENDING"
    if payload["action"] == "accept":
        status = "ACCEPTED"
    elif payload["action"] == "reject":
        status = "REJECTED"
    elif payload["action"] == "custom_price":
        status = "CUSTOM"

    sql_status = """
        UPDATE price_suggestions
        SET status = %s
        WHERE sku = %s AND vendor_id = %s
        ORDER BY created_at DESC
        LIMIT 1
    """
    execute_query(
        sql_status,
        (status, payload["sku"], payload["vendor_id"]),
    )

def get_feedback_summary() -> Optional[Dict[str, Any]]:
    sql = """
        SELECT action, COUNT(*) AS cnt
        FROM price_feedback
        GROUP BY action
    """
    rows = fetch_all(sql)
    if not rows:
        return None
    return {r["action"]: r["cnt"] for r in rows}
