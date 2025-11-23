from datetime import date

from app.db import fetch_all
from app.models.elasticity import (
    prepare_elasticity_data,
    fit_elasticity_model,
    save_elasticity_to_db,
)
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)

def main():
    logger.info("Training elasticity models per SKU")
    sql = """
        SELECT DISTINCT sku, vendor_id
        FROM orders
    """
    rows = fetch_all(sql)
    for r in rows:
        sku = r["sku"]
        vendor_id = r["vendor_id"]
        df = prepare_elasticity_data(sku, vendor_id)
        model, elasticity, metrics = fit_elasticity_model(df)
        if elasticity is None:
            logger.info(f"Skipping sku={sku}, vendor_id={vendor_id}, reason={metrics.get('reason')}")
            continue
        save_elasticity_to_db(sku, vendor_id, elasticity, metrics)
        logger.info(f"Trained elasticity for sku={sku}, vendor_id={vendor_id}, e={elasticity:.3f}")
    logger.info("Elasticity training completed.")

if __name__ == "__main__":
    main()
