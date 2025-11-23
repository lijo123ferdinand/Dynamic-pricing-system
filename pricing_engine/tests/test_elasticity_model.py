import pandas as pd

from app.models.elasticity import fit_elasticity_model

def test_elasticity_sign_negative():
    # create synthetic data with negative relationship
    prices = [10, 12, 14, 16, 18, 20]
    units = [100, 80, 65, 55, 45, 40]
    df = pd.DataFrame({"price": prices, "units": units, "promo_flag": [0]*6})
    model, elasticity, metrics = fit_elasticity_model(df)
    assert elasticity is not None
    assert elasticity < 0  # demand falls with price
