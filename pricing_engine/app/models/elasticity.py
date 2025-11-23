from typing import Tuple, Optional, Dict, Any

import numpy as np
import pandas as pd
import statsmodels.api as sm

from app.db import fetch_all, execute_query
from app.utils.logging_utils import get_logger
from app.config import Config

logger = get_logger(__name__)

def prepare_elasticity_data(sku: str, vendor_id: str) -> pd.DataFrame:
    sql = """
        SELECT
            DATE(order_ts) AS date,
            price_paid AS price,
            SUM(units) AS units,
            MAX(promo_flag) AS promo_flag
        FROM orders
        WHERE sku = %s AND vendor_id = %s
        GROUP BY DATE(order_ts), price_paid
        HAVING SUM(units) > 0
        ORDER BY date
    """
    rows = fetch_all(sql, (sku, vendor_id))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df

def fit_elasticity_model(df: pd.DataFrame) -> Tuple[Optional[sm.regression.linear_model.RegressionResultsWrapper],
                                                    Optional[float],
                                                    Dict[str, Any]]:
    if df.empty:
        return None, None, {"reason": "no_data"}

    # Log-log transform; add small epsilon to avoid log(0)
    eps = 1e-6
    df = df[df["price"] > 0]
    df = df[df["units"] > 0]
    if df.shape[0] < 10 or df["price"].nunique() < 3:
        return None, None, {"reason": "insufficient_variation"}

    df["log_price"] = np.log(df["price"] + eps)
    df["log_units"] = np.log(df["units"] + eps)
    df["promo_flag"] = df.get("promo_flag", 0).astype(float)

    X = df[["log_price", "promo_flag"]]
    X = sm.add_constant(X)
    y = df["log_units"]

    model = sm.OLS(y, X).fit()
    elasticity_coef = model.params.get("log_price", np.nan)

    metrics = {
        "r2": float(model.rsquared),
        "p_value_price": float(model.pvalues.get("log_price", np.nan)),
        "n_obs": int(df.shape[0]),
    }
    return model, float(elasticity_coef), metrics

def save_elasticity_to_db(
    sku: str,
    vendor_id: str,
    elasticity: float,
    metrics: Dict[str, Any],
):
    sql = """
        INSERT INTO elasticity_coeffs
          (sku, vendor_id, elasticity, r2, p_value_price, n_obs, last_trained_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
          elasticity = VALUES(elasticity),
          r2 = VALUES(r2),
          p_value_price = VALUES(p_value_price),
          n_obs = VALUES(n_obs),
          last_trained_at = NOW()
    """
    execute_query(
        sql,
        (
            sku,
            vendor_id,
            elasticity,
            metrics.get("r2"),
            metrics.get("p_value_price"),
            metrics.get("n_obs"),
        ),
    )

def get_elasticity_for_sku(sku: str, vendor_id: str) -> Tuple[float, Dict[str, Any]]:
    row = fetch_all(
        """
        SELECT elasticity, r2, p_value_price, n_obs
        FROM elasticity_coeffs
        WHERE sku = %s AND vendor_id = %s
        """,
        (sku, vendor_id),
    )
    if row:
        r = row[0]
        return float(r["elasticity"]), {
            "r2": float(r["r2"] or 0.0),
            "p_value_price": float(r["p_value_price"] or 1.0),
            "n_obs": int(r["n_obs"] or 0),
        }

    # Fallback: use default global elasticity
    logger.warning(f"No elasticity found for sku={sku}, vendor_id={vendor_id}, using default")
    return Config.DEFAULT_ELASTICITY, {"r2": 0.0, "p_value_price": 1.0, "n_obs": 0}
