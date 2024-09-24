import logging
from typing import Any, Callable

from celery import Celery

from core.base import OrchestrationConfig, OrchestrationProvider

logger = logging.getLogger(__name__)

celery_app = Celery("r2r_app")
celery_app.config_from_object("celeryconfig")


class CeleryOrchestrationProvider(OrchestrationProvider):
    def __init__(self, config: OrchestrationConfig):
        super().__init__(config)
        # self.app = Celery("r2r_app")
        # self.app.config_from_object('celeryconfig')
        self.app = celery_app

    def step(self, *args, **kwargs) -> Callable:
        def decorator(func):
            return self.app.task(*args, **kwargs)(func)

        return decorator

    def register_workflow(self, workflow: Any) -> None:
        # This method can be left empty as Celery auto-discovers tasks
        pass

    def workflow(self, *args, **kwargs) -> Callable:
        return self.app.task(*args, **kwargs)

    def get_worker(self, name: str, max_threads: int) -> Any:
        logger.warning(
            "Celery does not manage workers within the Python process."
        )
        return None

    def start_worker(self):
        logger.warning(
            "Celery workers are managed separately and do not need to be started from the Python process."
        )


# import logging
# from typing import Any, Callable
# from celery import Celery
# from core.base import OrchestrationConfig, OrchestrationProvider

# # Create the Celery app instance
# celery_app = Celery("r2r_app")
# celery_app.config_from_object('celeryconfig')

# logger = logging.getLogger(__name__)

# class CeleryOrchestrationProvider(OrchestrationProvider):
#     def __init__(self, config: OrchestrationConfig):
#         super().__init__(config)
#         self.app = celery_app

#     def step(self, *args, **kwargs) -> Callable:
#         def decorator(func):
#             return self.app.task(*args, **kwargs)(func)
#         return decorator

#     def register_workflow(self, workflow: Any) -> None:
#         for step_name in dir(workflow):
#             step = getattr(workflow, step_name)
#             if callable(step) and hasattr(step, "delay"):
#                 # Tasks are already registered by the @shared_task decorator
#                 pass

#     def workflow(self, *args, **kwargs) -> Callable:
#         return self.app.task(*args, **kwargs)

#     def get_worker(self, name: str, max_threads: int) -> Any:
#         # This method is not needed for Celery, as workers are managed separately
#         raise NotImplementedError("Celery does not manage workers within the python process.")

#     def start_worker(self):
#         # This method is not needed for Celery, as workers are managed separately
#         # raise NotImplementedError("Celery does not manage workers within the python process.")
#         logger.warning("Celery workers are managed separately and do not need to be started from the python process.")

# # Ensure the celery_app is available for import
# __all__ = ['celery_app', 'CeleryOrchestrationProvider']

# # from typing import Any, Callable
# # from celery import Celery
# # from core.base import OrchestrationConfig, OrchestrationProvider
# # import logging

# # logger = logging.getLogger(__name__)

# # class CeleryOrchestrationProvider(OrchestrationProvider):
# #     def __init__(self, config: OrchestrationConfig):
# #         super().__init__(config)
# #         self.app = Celery("r2r_app")
# #         self.app.config_from_object('celeryconfig')

# #     def step(self, *args, **kwargs) -> Callable:
# #         def decorator(func):
# #             return self.app.task(*args, **kwargs)(func)
# #         return decorator

# #     def register_workflow(self, workflow: Any) -> None:
# #         for step_name in dir(workflow):
# #             step = getattr(workflow, step_name)
# #             if callable(step) and hasattr(step, "delay"):
# #                 self.app.task(name=f"{workflow.__class__.__name__}.{step_name}")(step)

# #     def workflow(self, *args, **kwargs) -> Callable:
# #         return self.app.task(*args, **kwargs)

# #     def get_worker(self, name: str, max_threads: int) -> Any:
# #         # This method is not needed for Celery, as workers are managed separately
# #         raise NotImplementedError("Celery does not manage workers within the python process.")

# #     def start_worker(self):
# #         # This method is not needed for Celery, as workers are managed separately
# #         # raise NotImplementedError("Celery does not manage workers within the python process.")
# #         logger.warning("Celery workers are managed separately and do not need to be started from the python process.")
