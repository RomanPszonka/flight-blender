import os

from celery import Celery
from dotenv import find_dotenv, load_dotenv

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)

app = Celery(
    "flight_blender",
    include=["conformance_monitoring_operations.tasks", "surveillance_monitoring_operations.tasks"],
    broker_connection_retry_on_startup=True,
)

app.conf.update(
    accept_content=["json"],
    task_serializer="json",
    result_serializer="json",
    broker_url=os.getenv("REDIS_BROKER_URL", "redis://localhost:6379/"),
    result_backend=os.getenv("REDIS_BROKER_URL", "redis://localhost:6379/"),
    timezone="UTC",
)

app.autodiscover_tasks(
    [
        "conformance_monitoring_operations",
        "surveillance_monitoring_operations",
        "flight_declaration_operations",
        "flight_feed_operations",
        "geo_fence_operations",
        "rid_operations",
    ]
)


@app.task(bind=True)
def debug_task(self):
    print(f"Request: {self.request!r}")
