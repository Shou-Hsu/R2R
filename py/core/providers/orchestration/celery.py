from typing import Any, Callable

from celery import Celery

from core.base import OrchestrationConfig, OrchestrationProvider


class CeleryOrchestrationProvider(OrchestrationProvider):
    def __init__(self, config: OrchestrationConfig):
        super().__init__(config)
        self.celery_app = Celery("r2r_tasks")
        self.celery_app.config_from_object("celeryconfig")

    def register_workflow(self, workflow: Any) -> None:
        # In Celery, tasks are typically defined using decorators
        # So this method might not be needed, or could be used for additional setup
        pass

    def get_worker(self, name: str, max_threads: int) -> Any:
        # Celery workers are typically started separately
        # This method could be used to configure worker settings
        return self.celery_app

    def workflow(self, *args, **kwargs) -> Callable:
        # In Celery, this would be equivalent to the @task decorator
        return self.celery_app.task(*args, **kwargs)

    def step(self, *args, **kwargs) -> Callable:
        # In Celery, steps are typically just regular functions
        # This method could be used to add additional functionality
        return lambda func: func

    def start_worker(self):
        # Celery workers are typically started using the command line
        # This method could be used to programmatically start a worker
        pass

    def set_service(self, service):
        self.service = service
        return self
