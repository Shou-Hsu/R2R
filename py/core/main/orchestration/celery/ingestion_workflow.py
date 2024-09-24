import asyncio
import logging

from celery import Celery, chain
from celery.utils.log import get_task_logger
from hatchet_sdk import Context

from core.base import IngestionStatus, increment_version
from core.base.abstractions import DocumentInfo, R2RException

from ...services import IngestionService, IngestionServiceAdapter

logger = get_task_logger(__name__)

celery_app = Celery("r2r_tasks")
celery_app.config_from_object("celeryconfig")


class CeleryIngestFilesWorkflow:
    class IngestionTask(celery_app.Task):
        _ingestion_service = None

        def __init__(self):
            self._workflow_instance = None

        @property
        def ingestion_service(self):
            if self._workflow_instance is None:
                raise ValueError("Workflow instance not set")
            return self._workflow_instance.ingestion_service

        def __call__(self, *args, **kwargs):
            if not self._workflow_instance:
                raise ValueError("Workflow instance not set")
            return super().__call__(*args, **kwargs)

    @celery_app.task(name="ingest_file", base=IngestionTask, bind=True)
    def ingest_file(self, request):
        logger.info(
            f"Starting ingest_file workflow for document_id: {request['document_id']}"
        )
        workflow = chain(
            self.parse.s(request),
            self.chunk.s(),
            self.embed.s(),
            self.finalize.s(),
        )
        return workflow()

    @celery_app.task(base=IngestionTask, bind=True)
    async def parse(self, request):
        logger.info(f"Parsing file for document_id: {request['document_id']}")
        parsed_data = self.ingestion_service.parse_ingest_file_input(request)
        document_info = await self.ingestion_service.ingest_file_ingress(
            **parsed_data
        )
        await self.ingestion_service.update_document_status(
            document_info, status="PARSING"
        )
        extractions = await self.ingestion_service.parse_file(document_info)
        return {
            "status": "Successfully extracted data",
            "extractions": [
                extraction.to_dict() for extraction in extractions
            ],
            "document_info": document_info.to_dict(),
        }


ingest_files_workflow = CeleryIngestFilesWorkflow()

# @celery_app.task(base=IngestionTask, bind=True)
# def chunk(self, parse_result):
#     document_info = parse_result['document_info']
#     logger.info(f"Chunking document for document_id: {document_info['id']}")
#     await self.ingestion_service.update_document_status(document_info, status="CHUNKING")
#     chunks = await self.ingestion_service.chunk_document(parse_result['extractions'])
#     return {
#         "status": "Successfully chunked data",
#         "chunks": [chunk.to_dict() for chunk in chunks],
#         "document_info": document_info
#     }

# @celery_app.task(base=IngestionTask, bind=True)
# def embed(self, chunk_result):
#     document_info = chunk_result['document_info']
#     logger.info(f"Embedding document for document_id: {document_info['id']}")
#     await self.ingestion_service.update_document_status(document_info, status="EMBEDDING")
#     embeddings = await self.ingestion_service.embed_document(chunk_result['chunks'])
#     await self.ingestion_service.update_document_status(document_info, status="STORING")
#     await self.ingestion_service.store_embeddings(embeddings)
#     return {
#         "document_info": document_info
#     }

# @celery_app.task(base=IngestionTask, bind=True)
# def finalize(self, embed_result):
#     document_info = embed_result['document_info']
#     logger.info(f"Finalizing ingestion for document_id: {document_info['id']}")
#     await self.ingestion_service.finalize_ingestion(document_info)
#     await self.ingestion_service.update_document_status(document_info, status="SUCCESS")
#     return {
#         "status": "Successfully finalized ingestion",
#         "document_info": document_info
#     }

ingest_files_workflow = CeleryIngestFilesWorkflow()
