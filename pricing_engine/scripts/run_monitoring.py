from datetime import date, timedelta

from app.monitoring.monitor import run_daily_monitoring
from app.utils.logging_utils import get_logger

logger = get_logger(__name__)

def main():
    target_date = date.today() - timedelta(days=1)
    logger.info(f"Running monitoring for {target_date}")
    run_daily_monitoring(target_date)

if __name__ == "__main__":
    main()
