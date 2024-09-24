from celery import Celery

# Initialize Celery
celery_app = Celery("r2r_tasks")

# Configure Celery
celery_app.config_from_object("celeryconfig")


# This allows you to access the Celery instance throughout your codebase
def get_celery_app():
    return celery_app
