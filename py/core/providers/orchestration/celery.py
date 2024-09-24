from typing import Any, Callable

from celery import Celery

from core.base import OrchestrationConfig, OrchestrationProvider


class CeleryOrchestrationProvider(OrchestrationProvider):
    def __init__(self, config: OrchestrationConfig):
        super().__init__(config)
        self.app = Celery("r2r")  # , broker=config.broker_url)
        self.app.conf.broker_url = "amqp://guest:guest@localhost:5672//"
        self.app.conf.result_backend = "rpc://"

        self.app.conf.update(
            task_serializer="json",
            accept_content=["json"],
            result_serializer="json",
        )

    def step(self, *args, **kwargs) -> Callable:
        def decorator(func):
            return self.app.task(*args, **kwargs)(func)

        return decorator

    def register_workflow(self, workflow: Any) -> None:
        # Register the individual workflow steps as Celery tasks
        for step_name in dir(workflow):
            step = getattr(workflow, step_name)
            if callable(step) and hasattr(step, "delay"):
                self.app.task(step)

    # def register_workflow(self, workflow: Any) -> None:
    #     # Register the workflow class as a Celery task
    #     self.app.register_task(workflow)

    def get_worker(self, name: str, max_threads: int) -> Any:
        # Not needed for Celery, workers are managed separately
        pass

    def workflow(self, *args, **kwargs) -> Callable:
        # Use Celery's task decorator
        return self.app.task(*args, **kwargs)

    # def step(self, *args, **kwargs) -> Callable:
    #     # Use Celery's task decorator for steps
    #     return self.app.task(*args, **kwargs)

    # def start_worker(self):
    #     # Start the Celery worker
    #     self.app.worker_main()

    def start_worker(self):
        # Start the Celery worker programmatically
        worker = self.app.Worker(loglevel="INFO", concurrency=1)
        worker.start()
