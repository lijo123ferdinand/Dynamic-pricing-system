import types

from app.optimizer.price_optimizer import optimize_price_for_sku

def test_dummy_price_optimizer_constraints(monkeypatch):
    # monkeypatch feature store
    from app import optimizer

    from app.features import store as store_mod
    from app.optimizer import price_optimizer as po

    def fake_get_latest_features_for_sku(sku, vendor_id):
        return {
            "sku": sku,
            "vendor_id": vendor_id,
            "inventory": 50,
            "current_price": 100.0,
            "last_price": 100.0,
            "avg_daily_sales_30d": 5.0,
            "views_7d": 100,
            "views_30d": 400,
            "add_to_cart_7d": 20,
            "conv_rate_7d": 0.2,
            "promo_flag": 0,
            "ageing_days": 10,
            "restock_eta_days": 5,
            "cost_price": 60.0,
            "base_price": 100.0,
        }

    monkeypatch.setattr(po, "get_latest_features_for_sku", fake_get_latest_features_for_sku)

    # monkeypatch demand_model.predict_demand
    def fake_predict_demand(df):
        # demand decreases with price
        import numpy as np
        return 200 - df["current_price"].values

    monkeypatch.setattr(po, "predict_demand", fake_predict_demand)

    # monkeypatch vendor rules
    def fake_vendor_rules(sku, vendor_id):
        return {
            "min_margin_pct": 10.0,
            "max_discount_pct": 50.0,
            "max_daily_price_move_pct": 20.0,
        }

    monkeypatch.setattr(po, "_get_vendor_rules", fake_vendor_rules)

    res = optimize_price_for_sku("sku1", "v1")
    assert res is not None
    assert res.optimal_price >= 60.0  # must respect margin constraint
    assert res.expected_profit >= 0
