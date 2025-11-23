from datetime import date

from app.db import fetch_all
from app.optimizer.price_optimizer import optimize_price_for_sku, persist_optimization_result, log_prediction
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)

def main():
    logger.info("Running batch price recommendation job")
    sql = """
        SELECT DISTINCT sku, vendor_id
        FROM sku_features_daily
        WHERE date = (SELECT MAX(date) FROM sku_features_daily)
    """
    rows = fetch_all(sql)
    for r in rows:
        sku = r["sku"]
        vendor_id = r["vendor_id"]
        result = optimize_price_for_sku(sku, vendor_id)
        if not result:
            continue
        suggestion_id = persist_optimization_result(result)
        input_features = {
            "sku": result.sku,
            "vendor_id": result.vendor_id,
            "current_price": result.current_price,
        }
        output = {
            "optimal_price": result.optimal_price,
            "expected_revenue": result.expected_revenue,
            "expected_profit": result.expected_profit,
        }
        log_prediction(
            sku=result.sku,
            vendor_id=result.vendor_id,
            suggestion_id=suggestion_id,
            model_type="optimizer",
            input_features=input_features,
            output=output,
        )
        logger.info(f"Suggested price {result.optimal_price} for sku={sku}, vendor={vendor_id}")
    logger.info("Batch pricing job completed")

if __name__ == "__main__":
    main()
