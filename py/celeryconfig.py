# core/celeryconfig.py
broker_url = "amqp://guest:guest@localhost:5672//"
result_backend = "rpc://"

task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]
timezone = "UTC"
enable_utc = True

# Make sure Celery can find your tasks
imports = ("core.main.orchestration.ingestion_workflow",)

# Set the Celery app name
app_name = "r2r_app"
