from typing import Optional, Dict, Any

import pandas as pd

from app.db import fetch_all, execute_query
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)

def get_latest_features_for_sku(sku: str, vendor_id: str) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT *
        FROM sku_features_daily
        WHERE sku = %s AND vendor_id = %s
        ORDER BY date DESC
        LIMIT 1
    """
    rows = fetch_all(sql, (sku, vendor_id))
    if not rows:
        return None
    return rows[0]

def insert_features(df: pd.DataFrame):
    if df.empty:
        return
    sql = """
        INSERT INTO sku_features_daily
            (sku, date, vendor_id, avg_daily_sales_7d, avg_daily_sales_30d,
             last_price, current_price, inventory, views_7d, views_30d,
             add_to_cart_7d, conv_rate_7d, promo_flag, ageing_days,
             restock_eta_days, cost_price, base_price, other_features_json)
        VALUES
            (%s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s,
             %s, %s, %s, %s,
             %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            avg_daily_sales_7d = VALUES(avg_daily_sales_7d),
            avg_daily_sales_30d = VALUES(avg_daily_sales_30d),
            last_price = VALUES(last_price),
            current_price = VALUES(current_price),
            inventory = VALUES(inventory),
            views_7d = VALUES(views_7d),
            views_30d = VALUES(views_30d),
            add_to_cart_7d = VALUES(add_to_cart_7d),
            conv_rate_7d = VALUES(conv_rate_7d),
            promo_flag = VALUES(promo_flag),
            ageing_days = VALUES(ageing_days),
            restock_eta_days = VALUES(restock_eta_days),
            cost_price = VALUES(cost_price),
            base_price = VALUES(base_price),
            other_features_json = VALUES(other_features_json)
    """
    for _, row in df.iterrows():
        execute_query(
            sql,
            (
                row["sku"],
                row["date"],
                row["vendor_id"],
                row["avg_daily_sales_7d"],
                row["avg_daily_sales_30d"],
                row["last_price"],
                row["current_price"],
                row["inventory"],
                row["views_7d"],
                row["views_30d"],
                row["add_to_cart_7d"],
                row["conv_rate_7d"],
                row["promo_flag"],
                row["ageing_days"],
                row["restock_eta_days"],
                row["cost_price"],
                row["base_price"],
                row.get("other_features_json"),
            ),
        )
