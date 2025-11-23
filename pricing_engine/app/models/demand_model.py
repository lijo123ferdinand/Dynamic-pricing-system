from typing import Tuple, Dict, Any
import os

import numpy as np
import pandas as pd
from joblib import dump, load
from lightgbm import LGBMRegressor
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error, r2_score

from app.db import fetch_all, execute_query
from app.utils.logging_utils import get_logger
from app.config import Config

logger = get_logger(__name__)

_global_model: LGBMRegressor | None = None

def build_training_data() -> Tuple[pd.DataFrame, pd.Series]:
    sql = """
        SELECT *
        FROM sku_features_daily
    """
    rows = fetch_all(sql)
    if not rows:
        raise RuntimeError("No data in sku_features_daily for training")

    df = pd.DataFrame(rows)
    # target: avg_daily_sales_7d (representing demand)
    y = df["avg_daily_sales_7d"].astype(float).fillna(0.0)

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
    for c in feature_cols:
        if c not in df.columns:
            df[c] = 0.0

    X = df[feature_cols].astype(float).fillna(0.0)
    return X, y

def train_demand_model(X: pd.DataFrame, y: pd.Series) -> Tuple[LGBMRegressor, Dict[str, float]]:
    model = LGBMRegressor(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=-1,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="regression",
    )
    model.fit(X, y)

    y_pred = model.predict(X)
    mape = float(mean_absolute_percentage_error(y, y_pred))
    rmse = float(mean_squared_error(y, y_pred, squared=False))
    r2 = float(r2_score(y, y_pred))

    metrics = {"MAPE": mape, "RMSE": rmse, "R2": r2}
    logger.info(f"Demand model metrics: {metrics}")
    return model, metrics

def save_demand_model(model: LGBMRegressor):
    os.makedirs(os.path.dirname(Config.DEMAND_MODEL_PATH), exist_ok=True)
    dump(model, Config.DEMAND_MODEL_PATH)
    logger.info(f"Saved demand model to {Config.DEMAND_MODEL_PATH}")

def load_demand_model() -> LGBMRegressor:
    global _global_model
    if _global_model is None:
        logger.info("Loading demand model artifact...")
        _global_model = load(Config.DEMAND_MODEL_PATH)
    return _global_model

def predict_demand(features_df: pd.DataFrame) -> np.ndarray:
    model = load_demand_model()
    preds = model.predict(features_df)
    # Ensure non-negative
    preds = np.maximum(preds, 0.0)
    return preds

def log_demand_metrics_to_db(metrics: Dict[str, float]):
    sql = """
        INSERT INTO monitoring_metrics
            (date, sku, vendor_id, model_type, metric_name, metric_value, created_at)
        VALUES (CURDATE(), '_global_', '_global_', 'demand', %s, %s, NOW())
    """
    for name, value in metrics.items():
        execute_query(sql, (name, value))
