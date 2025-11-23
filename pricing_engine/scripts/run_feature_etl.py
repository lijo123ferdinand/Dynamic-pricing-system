from datetime import date

from app.features.etl import run_daily_feature_etl
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)

def main():
    logger.info("Starting daily feature ETL script")
    run_daily_feature_etl(date.today())
    logger.info("Feature ETL completed")

if __name__ == "__main__":
    main()
