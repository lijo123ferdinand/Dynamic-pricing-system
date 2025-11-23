from datetime import date, timedelta

import pandas as pd

from app.db import fetch_all
from app.features.store import insert_features
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)

def _load_orders(start_date: date, end_date: date) -> pd.DataFrame:
    sql = """
        SELECT DATE(order_ts) AS date, sku, vendor_id,
               price_paid, units, promo_flag
        FROM orders
        WHERE order_ts >= %s AND order_ts < %s
    """
    rows = fetch_all(sql, (start_date, end_date))
    return pd.DataFrame(rows)

def _load_inventory(as_of_date: date) -> pd.DataFrame:
    sql = """
        SELECT s.sku, s.snapshot_date AS date, s.stock_qty AS inventory,
               s.ageing_days, s.restock_eta_date
        FROM inventory_snapshots s
        WHERE s.snapshot_date = %s
    """
    rows = fetch_all(sql, (as_of_date,))
    return pd.DataFrame(rows)

def _load_product_analytics(start_date: date, end_date: date) -> pd.DataFrame:
    sql = """
        SELECT sku, date, views, add_to_cart, conversions, conv_rate
        FROM product_analytics
        WHERE date >= %s AND date < %s
    """
    rows = fetch_all(sql, (start_date, end_date))
    return pd.DataFrame(rows)

def run_daily_feature_etl(target_date: date | None = None):
    if target_date is None:
        target_date = date.today()

    logger.info(f"Running feature ETL for date={target_date}")
    start_30d = target_date - timedelta(days=30)
    start_7d = target_date - timedelta(days=7)
    end_next = target_date + timedelta(days=1)

    orders = _load_orders(start_30d, end_next)
    inv = _load_inventory(target_date)
    pa = _load_product_analytics(start_30d, end_next)

    if orders.empty:
        logger.warning("No orders data for ETL")
    if inv.empty:
        logger.warning("No inventory data for ETL")

    # Aggregate sales
    sales_daily = (
        orders.groupby(["sku", "vendor_id", "date"])
        .agg(units=("units", "sum"), price=("price_paid", "mean"))
        .reset_index()
    )

    # Rolling windows for each sku+vendor
    sales_daily = sales_daily.sort_values(["sku", "vendor_id", "date"])
    sales_daily["avg_daily_sales_7d"] = (
        sales_daily.groupby(["sku", "vendor_id"])["units"]
        .rolling(window=7, min_periods=1)
        .mean()
        .reset_index(level=[0, 1], drop=True)
    )
    sales_daily["avg_daily_sales_30d"] = (
        sales_daily.groupby(["sku", "vendor_id"])["units"]
        .rolling(window=30, min_periods=1)
        .mean()
        .reset_index(level=[0, 1], drop=True)
    )

    # Filter to target_date
    sales_td = sales_daily[sales_daily["date"] == target_date].copy()

    # Product analytics rolling (simplified: just sum last 7/30 days)
    if not pa.empty:
        pa_7 = (
            pa[pa["date"] >= start_7d]
            .groupby("sku")
            .agg(
                views_7d=("views", "sum"),
                add_to_cart_7d=("add_to_cart", "sum"),
                conv_rate_7d=("conv_rate", "mean"),
            )
            .reset_index()
        )
        pa_30 = (
            pa[pa["date"] >= start_30d]
            .groupby("sku")
            .agg(views_30d=("views", "sum"))
            .reset_index()
        )
        pa_merged = pa_7.merge(pa_30, on="sku", how="left")
    else:
        pa_merged = pd.DataFrame(columns=["sku", "views_7d", "add_to_cart_7d", "conv_rate_7d", "views_30d"])

    # Merge with inventory
    df = sales_td.merge(inv, on=["sku", "date"], how="left")
    df = df.merge(pa_merged, on="sku", how="left")

    # Simple placeholders
    df["promo_flag"] = 0  # could be enriched from promotions_calendar
    df["restock_eta_days"] = (df["restock_eta_date"] - df["date"]).dt.days.fillna(0)
    df["cost_price"] = df["price"] * 0.7  # placeholder cost assumption
    df["base_price"] = df["price"]
    df["last_price"] = df["price"]
    df["current_price"] = df["price"]

    df["views_7d"] = df["views_7d"].fillna(0)
    df["views_30d"] = df["views_30d"].fillna(0)
    df["add_to_cart_7d"] = df["add_to_cart_7d"].fillna(0)
    df["conv_rate_7d"] = df["conv_rate_7d"].fillna(0)
    df["inventory"] = df["inventory"].fillna(0)
    df["ageing_days"] = df["ageing_days"].fillna(0)
    df["restock_eta_days"] = df["restock_eta_days"].fillna(0)

    df["other_features_json"] = None

    # Rename units columns
    df.rename(
        columns={
            "units": "avg_daily_sales_7d",  # note: ETL could be more nuanced, but keep aligned
        },
        inplace=True,
    )

    # Ensure required columns exist
    required_cols = [
        "sku",
        "date",
        "vendor_id",
        "avg_daily_sales_7d",
        "avg_daily_sales_30d",
        "last_price",
        "current_price",
        "inventory",
        "views_7d",
        "views_30d",
        "add_to_cart_7d",
        "conv_rate_7d",
        "promo_flag",
        "ageing_days",
        "restock_eta_days",
        "cost_price",
        "base_price",
        "other_features_json",
    ]
    for c in required_cols:
        if c not in df.columns:
            df[c] = 0

    features = df[required_cols].copy()
    insert_features(features)
    logger.info(f"Inserted {len(features)} feature rows for {target_date}")
