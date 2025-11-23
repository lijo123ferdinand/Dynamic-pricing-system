from datetime import date, timedelta
from typing import Dict, Any

import numpy as np
import pandas as pd

from app.db import fetch_all, execute_query
from app.models.elasticity import prepare_elasticity_data, fit_elasticity_model, save_elasticity_to_db
from app.monitoring.alerts import check_and_alert
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)

def _compute_demand_errors(target_date: date) -> Dict[str, float]:
    # Simplified: join suggestions with orders to approximate actual
    sql = """
        SELECT
            ps.sku,
            ps.vendor_id,
            ps.expected_revenue,
            ps.suggested_price,
            SUM(o.units) AS actual_units,
            SUM(o.price_paid * o.units) AS actual_revenue
        FROM price_suggestions ps
        LEFT JOIN orders o
          ON ps.sku = o.sku
         AND ps.vendor_id = o.vendor_id
         AND DATE(o.order_ts) = %s
        WHERE ps.suggestion_date = %s
        GROUP BY ps.sku, ps.vendor_id, ps.expected_revenue, ps.suggested_price
    """
    rows = fetch_all(sql, (target_date, target_date))
    if not rows:
        return {}

    df = pd.DataFrame(rows)
    df["pred_units"] = df["expected_revenue"] / df["suggested_price"]
    df["actual_units"] = df["actual_units"].fillna(0.0)

    y_true = df["actual_units"].values
    y_pred = df["pred_units"].values

    eps = 1e-6
    mape = float(np.mean(np.abs((y_true - y_pred) / (y_true + eps))))
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))
    r2 = 1.0 - float(np.sum((y_true - y_pred) ** 2)) / float(
        np.sum((y_true - np.mean(y_true)) ** 2) + eps
    )

    metrics = {"MAPE": mape, "RMSE": rmse, "R2": r2}

    sql_ins = """
        INSERT INTO monitoring_metrics
            (date, sku, vendor_id, model_type, metric_name, metric_value, created_at)
        VALUES (%s, '_global_', '_global_', 'demand', %s, %s, NOW())
    """
    for k, v in metrics.items():
        execute_query(sql_ins, (target_date, k, v))

    return metrics

def _compute_elasticity_drift(target_date: date):
    # Re-estimate elasticity on recent window and compare
    recent_start = target_date - timedelta(days=30)

    sql_skus = """
        SELECT DISTINCT sku, vendor_id
        FROM price_suggestions
        WHERE suggestion_date >= %s AND suggestion_date <= %s
    """
    skus = fetch_all(sql_skus, (recent_start, target_date))
    for r in skus:
        sku = r["sku"]
        vendor_id = r["vendor_id"]
        df = prepare_elasticity_data(sku, vendor_id)
        if df.empty:
            continue
        _, new_elasticity, metrics = fit_elasticity_model(df)
        if new_elasticity is None:
            continue

        # Get old from DB
        old_sql = """
            SELECT elasticity FROM elasticity_coeffs
            WHERE sku = %s AND vendor_id = %s
        """
        old_rows = fetch_all(old_sql, (sku, vendor_id))
        if not old_rows:
            continue
        old_el = float(old_rows[0]["elasticity"])

        drift = float(abs(new_elasticity - old_el))
        execute_query(
            """
            INSERT INTO monitoring_metrics
                (date, sku, vendor_id, model_type, metric_name, metric_value, created_at)
            VALUES (%s, %s, %s, 'elasticity', 'elasticity_drift', %s, NOW())
            """,
            (target_date, sku, vendor_id, drift),
        )

        save_elasticity_to_db(sku, vendor_id, new_elasticity, metrics)

def _compute_coverage(target_date: date):
    sql_total_skus = "SELECT COUNT(DISTINCT sku) AS cnt FROM sku_features_daily WHERE date = %s"
    total = fetch_all(sql_total_skus, (target_date,))
    total_cnt = int(total[0]["cnt"]) if total else 0

    sql_elasticity = "SELECT COUNT(DISTINCT sku) AS cnt FROM elasticity_coeffs"
    el = fetch_all(sql_elasticity)
    el_cnt = int(el[0]["cnt"]) if el else 0

    sql_suggestions = "SELECT COUNT(DISTINCT sku) AS cnt FROM price_suggestions WHERE suggestion_date = %s"
    ps = fetch_all(sql_suggestions, (target_date,))
    ps_cnt = int(ps[0]["cnt"]) if ps else 0

    def ins(name, value):
        execute_query(
            """
            INSERT INTO monitoring_metrics
                (date, sku, vendor_id, model_type, metric_name, metric_value, created_at)
            VALUES (%s, '_global_', '_global_', 'coverage', %s, %s, NOW())
            """,
            (target_date, name, value),
        )

    if total_cnt > 0:
        ins("total_skus", total_cnt)
        ins("elasticity_coverage_pct", el_cnt * 100.0 / total_cnt)
        ins("suggestion_coverage_pct", ps_cnt * 100.0 / total_cnt)

def run_daily_monitoring(target_date: date | None = None):
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    logger.info(f"Running monitoring for date={target_date}")
    metrics = _compute_demand_errors(target_date)
    check_and_alert(metrics)
    _compute_elasticity_drift(target_date)
    _compute_coverage(target_date)
    logger.info("Monitoring run completed.")
