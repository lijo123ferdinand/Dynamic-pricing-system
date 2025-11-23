from app.models.demand_model import (
    build_training_data,
    train_demand_model,
    save_demand_model,
    log_demand_metrics_to_db,
)
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)

def main():
    logger.info("Building training data for demand model")
    X, y = build_training_data()
    logger.info(f"Training demand model on {len(X)} rows")
    model, metrics = train_demand_model(X, y)
    save_demand_model(model)
    log_demand_metrics_to_db(metrics)
    logger.info("Demand model training completed")

if __name__ == "__main__":
    main()
