import pandas as pd
import numpy as np

from app.models.demand_model import train_demand_model

def test_demand_model_predict_non_negative():
    # tiny synthetic dataset
    X = pd.DataFrame(
        {
            "avg_daily_sales_30d": [10, 20, 30],
            "last_price": [100, 110, 120],
            "current_price": [105, 115, 125],
            "inventory": [50, 60, 70],
            "views_7d": [100, 150, 200],
            "views_30d": [400, 500, 600],
            "add_to_cart_7d": [10, 15, 20],
            "conv_rate_7d": [0.1, 0.12, 0.15],
            "promo_flag": [0, 1, 0],
            "ageing_days": [5, 10, 15],
            "restock_eta_days": [3, 4, 5],
            "cost_price": [60, 70, 80],
            "base_price": [100, 110, 120],
        }
    )
    y = pd.Series([10, 15, 20])

    model, metrics = train_demand_model(X, y)
    preds = model.predict(X)
    assert np.all(preds >= 0)
