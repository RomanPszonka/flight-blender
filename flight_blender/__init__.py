from flight_blender.logging_config import *  # noqa: F401, F403

# This will make sure the app is always imported when
# the module is imported.
from flight_blender.celery import app as celery_app

__all__ = ("celery_app",)
