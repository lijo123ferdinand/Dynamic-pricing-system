from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple, List

import numpy as np
import pandas as pd

from app.config import Config
from app.db import fetch_all, execute_query
from app.features.store import get_latest_features_for_sku
from app.models.elasticity import get_elasticity_for_sku
from app.models.demand_model import predict_demand
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)

@dataclass
class OptimizationResult:
    sku: str
    vendor_id: str
    current_price: float
    optimal_price: float
    expected_revenue: float
    expected_profit: float
    elasticity: float
    confidence: float
    reason: str

def _get_vendor_rules(sku: str, vendor_id: str) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT *
        FROM vendor_rules
        WHERE sku = %s AND vendor_id = %s
        LIMIT 1
    """
    rows = fetch_all(sql, (sku, vendor_id))
    return rows[0] if rows else None

def _confidence_from_metrics(elasticity_metrics: Dict[str, Any], demand_metrics: Optional[Dict[str, Any]] = None) -> float:
    r2 = elasticity_metrics.get("r2", 0.0)
    n_obs = elasticity_metrics.get("n_obs", 0)
    base_conf = min(1.0, max(0.1, r2 + min(0.5, n_obs / 1000.0)))
    return float(base_conf)

def optimize_price_for_sku(sku: str, vendor_id: str) -> Optional[OptimizationResult]:
    feat = get_latest_features_for_sku(sku, vendor_id)
    if not feat:
        logger.warning(f"No features for sku={sku}, vendor_id={vendor_id}")
        return None

    stock = int(feat.get("inventory", 0) or 0)
    if stock <= 0:
        logger.info(f"Zero stock for sku={sku}, vendor_id={vendor_id}, skipping")
        return None

    current_price = float(feat["current_price"])
    cost_price = float(feat.get("cost_price", 0) or 0)
    base_price = float(feat.get("base_price", current_price) or current_price)

    vendor_rules = _get_vendor_rules(sku, vendor_id)
    if not vendor_rules:
        logger.warning(f"No vendor rules for sku={sku}, vendor_id={vendor_id}, using defaults")
        vendor_rules = {
            "min_margin_pct": 10.0,
            "max_discount_pct": 50.0,
            "max_daily_price_move_pct": 20.0,
        }

    min_margin_pct = float(vendor_rules["min_margin_pct"]) / 100.0
    max_discount_pct = float(vendor_rules["max_discount_pct"]) / 100.0
    max_daily_move_pct = float(vendor_rules["max_daily_price_move_pct"]) / 100.0

    lower = max(
        current_price * (1 - max_daily_move_pct),
        base_price * (1 - max_discount_pct),
        cost_price / (1 - min_margin_pct) if min_margin_pct < 1 else cost_price,
    )
    upper = current_price * (1 + max_daily_move_pct)
    lower = max(lower, 0.01)

    price_grid = np.linspace(
        lower * Config.PRICE_RANGE_LOWER,
        upper * Config.PRICE_RANGE_UPPER,
        Config.PRICE_GRID_STEPS,
    )
    price_grid = np.unique(np.round(price_grid, 2))

    # Base feature vector
    feature_cols = [
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
    ]
    base_vec = {c: float(feat.get(c, 0) or 0) for c in feature_cols}

    candidate_rows: List[Dict[str, Any]] = []
    for p in price_grid:
        # Margin constraint: (p - cost_price)/p >= min_margin_pct
        if p <= cost_price:
            continue
        margin_pct = (p - cost_price) / p
        if margin_pct < min_margin_pct:
            continue

        row = base_vec.copy()
        row["current_price"] = p

        # Stock-based heuristics
        if stock < 5:
            row["promo_flag"] = 0  # avoid super low prices when almost out-of-stock
        elif stock > 100 and base_vec.get("avg_daily_sales_30d", 0) < 1:
            row["promo_flag"] = 1  # encourage clearance

        row["inventory"] = stock
        candidate_rows.append(row)

    if not candidate_rows:
        logger.warning(f"No valid candidates for sku={sku}, vendor_id={vendor_id}")
        return None

    df_candidates = pd.DataFrame(candidate_rows)
    q_pred = predict_demand(df_candidates)
    prices = df_candidates["current_price"].values
    revenue = prices * q_pred
    profit = (prices - cost_price) * q_pred

    best_idx = int(np.argmax(profit))
    optimal_price = float(prices[best_idx])
    expected_revenue = float(revenue[best_idx])
    expected_profit = float(profit[best_idx])

    elasticity, el_metrics = get_elasticity_for_sku(sku, vendor_id)
    confidence = _confidence_from_metrics(el_metrics)

    reason = "Optimized for profit given inventory and vendor rules."
    if stock > 100 and expected_profit > 0 and optimal_price < current_price:
        reason = "High stock + low demand: lowering price to stimulate sales."
    elif stock < 5 and optimal_price > current_price:
        reason = "Low stock: increasing price slightly to throttle demand."

    return OptimizationResult(
        sku=sku,
        vendor_id=vendor_id,
        current_price=current_price,
        optimal_price=optimal_price,
        expected_revenue=expected_revenue,
        expected_profit=expected_profit,
        elasticity=elasticity,
        confidence=confidence,
        reason=reason,
    )

def persist_optimization_result(result: OptimizationResult) -> int:
    sql = """
        INSERT INTO price_suggestions
            (sku, vendor_id, suggestion_date, current_price, suggested_price,
             expected_revenue, expected_profit, elasticity, confidence, reason,
             status, created_at)
        VALUES (%s, %s, CURDATE(), %s, %s, %s, %s, %s, %s, %s, 'PENDING', NOW())
    """
    execute_query(
        sql,
        (
            result.sku,
            result.vendor_id,
            result.current_price,
            result.optimal_price,
            result.expected_revenue,
            result.expected_profit,
            result.elasticity,
            result.confidence,
            result.reason,
        ),
    )
    # get last insert id
    row = fetch_all("SELECT LAST_INSERT_ID() AS id", ())
    return int(row[0]["id"]) if row else 0

def log_prediction(
    sku: str,
    vendor_id: str,
    suggestion_id: int,
    model_type: str,
    input_features: Dict[str, Any],
    output: Dict[str, Any],
):
    sql = """
        INSERT INTO prediction_logs
            (sku, vendor_id, suggestion_id, model_type,
             input_features_json, output_json, created_at)
        VALUES (%s, %s, %s, %s, JSON_OBJECT(), JSON_OBJECT(), NOW())
    """
    # MySQL JSON_OBJECT with parameters is messy, just stringify
    import json
    sql = """
        INSERT INTO prediction_logs
            (sku, vendor_id, suggestion_id, model_type,
             input_features_json, output_json, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
    """
    execute_query(
        sql,
        (
            sku,
            vendor_id,
            suggestion_id,
            model_type,
            json.dumps(input_features),
            json.dumps(output),
        ),
    )
