import os
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.getenv("SECRET_KEY", "FASTAPI_SECRET")
DEBUG = bool(int(os.getenv("IS_DEBUG", "0")))

# Database
USE_LOCAL_SQLITE_DATABASE = bool(int(os.getenv("USE_LOCAL_SQLITE_DATABASE", "0")))
if USE_LOCAL_SQLITE_DATABASE:
    DATABASE_URL = f"sqlite:///{BASE_DIR / 'flight_blender.sqlite3'}"
else:
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/flight_blender")

# Redis / Celery
if DEBUG:
    REDIS_BROKER_URL = os.getenv("REDIS_BROKER_URL", "redis://localhost:6379/")
else:
    REDIS_BROKER_URL = os.getenv("REDIS_BROKER_URL", "redis://redis:6379/")

CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_BROKER_URL = REDIS_BROKER_URL
CELERY_RESULT_BACKEND = REDIS_BROKER_URL
CELERY_TIMEZONE = "UTC"

# Custom classes
ASTM_F3623_SDSP_CUSTOM_DATA_FUSER_CLASS = os.getenv(
    "ASTM_F3623_SDSP_CUSTOM_DATA_FUSER_CLASS",
    "surveillance_monitoring_operations.utils.TrafficDataFuser",
)
CUSTOM_VOLUME_4D_GENERATION_CLASS = os.getenv("CUSTOM_VOLUME_4D_GENERATION_CLASS", "")

# Weather
WEATHER_API_BASE_URL = "https://api.open-meteo.com/v1/forecast"
