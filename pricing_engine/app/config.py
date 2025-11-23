import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "3306"))
    DB_USER = os.getenv("DB_USER", "pricing_user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "pricing_password")
    DB_NAME = os.getenv("DB_NAME", "pricing_db")

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    ELASTICITY_ARTIFACT_DIR = os.getenv(
        "ELASTICITY_ARTIFACT_DIR", "./models_artifacts/elasticity"
    )
    DEMAND_MODEL_PATH = os.getenv(
        "DEMAND_MODEL_PATH", "./models_artifacts/demand/demand_model.pkl"
    )

    PRICE_RANGE_LOWER = float(os.getenv("PRICE_RANGE_LOWER", "0.7"))
    PRICE_RANGE_UPPER = float(os.getenv("PRICE_RANGE_UPPER", "1.3"))
    PRICE_GRID_STEPS = int(os.getenv("PRICE_GRID_STEPS", "21"))
    DEFAULT_ELASTICITY = float(os.getenv("DEFAULT_ELASTICITY", "-1.5"))
