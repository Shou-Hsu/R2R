import asyncio

from celery import chain, shared_task

from core.base import IngestionStatus
from core.base.abstractions import DocumentInfo

from ..services import IngestionService, IngestionServiceAdapter

# @shared_task(bind=True)
# def workflow_task(self, method_name, *args, **kwargs):
#     workflow_logic = self.workflow_logic
#     method = getattr(workflow_logic, method_name)
#     return asyncio.run(method(*args, **kwargs))


@shared_task(bind=True)
def workflow_task(self, method_name, workflow_input, ingestion_service):
    workflow_logic = IngestFilesWorkflowLogic(ingestion_service)
    method = getattr(workflow_logic, method_name)
    return asyncio.run(method(workflow_input))


class IngestFilesWorkflowLogic:
    def __init__(self, ingestion_service: IngestionService):
        self.ingestion_service = ingestion_service

    async def parse(self, workflow_input):
        parsed_data = IngestionServiceAdapter.parse_ingest_file_input(
            workflow_input
        )
        ingestion_result = await self.ingestion_service.ingest_file_ingress(
            **parsed_data
        )
        document_info = ingestion_result["info"]

        await self.ingestion_service.update_document_status(
            document_info,
            status=IngestionStatus.PARSING,
        )

        extractions_generator = await self.ingestion_service.parse_file(
            document_info
        )
        extractions = [
            extraction async for extraction in extractions_generator
        ]
        serializable_extractions = [
            fragment.to_dict() for fragment in extractions
        ]

        return {
            "status": "Successfully extracted data",
            "extractions": serializable_extractions,
            "document_info": document_info.to_dict(),
            "workflow_input": workflow_input,
        }

    async def chunk(self, parse_result):
        document_info = DocumentInfo(**parse_result["document_info"])
        await self.ingestion_service.update_document_status(
            document_info,
            status=IngestionStatus.CHUNKING,
        )

        extractions = parse_result["extractions"]
        chunking_config = parse_result["workflow_input"].get("chunking_config")

        chunk_generator = await self.ingestion_service.chunk_document(
            extractions, chunking_config
        )
        chunks = [chunk async for chunk in chunk_generator]
        serializable_chunks = [chunk.to_dict() for chunk in chunks]

        return {
            "status": "Successfully chunked data",
            "chunks": serializable_chunks,
            "document_info": document_info.to_dict(),
            "workflow_input": parse_result["workflow_input"],
        }


# class IngestFilesWorkflow:
#     def __init__(self, ingestion_service: IngestionService):
#         self.workflow_logic = IngestFilesWorkflowLogic(ingestion_service)

#     def create_workflow(self, workflow_input):
#         return chain(
#             workflow_task.s('parse', workflow_input),
#             workflow_task.s('chunk'),
#             workflow_task.s('embed'),
#             workflow_task.s('finalize')
#         )


class IngestFilesWorkflow:
    def __init__(self, ingestion_service: IngestionService):
        self.ingestion_service = ingestion_service

    def create_workflow(self, initial_input):
        return chain(
            workflow_task.s("parse", initial_input, self.ingestion_service),
            workflow_task.s("chunk", self.ingestion_service),
            workflow_task.s("embed", self.ingestion_service),
            workflow_task.s("finalize", self.ingestion_service),
        )


# from celery import shared_task, chain
# import asyncio
# import logging

# from core.base import IngestionStatus
# from core.base.abstractions import DocumentInfo, R2RException
# from ..services import IngestionService, IngestionServiceAdapter

# logger = logging.getLogger(__name__)

# class IngestFilesWorkflow:
#     def __init__(self, ingestion_service: IngestionService):
#         self.ingestion_service = ingestion_service

#     @staticmethod
#     @shared_task(bind=True)
#     def parse(self, workflow_input):
#         return asyncio.run(self._parse(workflow_input))

#     @classmethod
#     async def _parse(self, workflow_input):
#         print(f"Parse task received workflow_input: {workflow_input}")
#         parsed_data = IngestionServiceAdapter.parse_ingest_file_input(workflow_input)
#         ingestion_result = await self.ingestion_service.ingest_file_ingress(**parsed_data)
#         document_info = ingestion_result["info"]

#         await self.ingestion_service.update_document_status(
#             document_info,
#             status=IngestionStatus.PARSING,
#         )

#         extractions_generator = await self.ingestion_service.parse_file(document_info)
#         extractions = [extraction async for extraction in extractions_generator]
#         serializable_extractions = [fragment.to_dict() for fragment in extractions]

#         return {
#             "status": "Successfully extracted data",
#             "extractions": serializable_extractions,
#             "document_info": document_info.to_dict(),
#             "workflow_input": workflow_input,
#         }

#     @shared_task(bind=True)
#     def chunk(self, parse_result):
#         return asyncio.run(self._chunk(parse_result))

#     async def _chunk(self, parse_result):
#         document_info = DocumentInfo(**parse_result["document_info"])
#         await self.ingestion_service.update_document_status(
#             document_info,
#             status=IngestionStatus.CHUNKING,
#         )

#         extractions = parse_result["extractions"]
#         chunking_config = parse_result["workflow_input"].get("chunking_config")

#         chunk_generator = await self.ingestion_service.chunk_document(extractions, chunking_config)
#         chunks = [chunk async for chunk in chunk_generator]
#         serializable_chunks = [chunk.to_dict() for chunk in chunks]

#         return {
#             "status": "Successfully chunked data",
#             "chunks": serializable_chunks,
#             "document_info": document_info.to_dict(),
#             "workflow_input": parse_result["workflow_input"],
#         }

#     @shared_task(bind=True)
#     def embed(self, chunk_result):
#         return asyncio.run(self._embed(chunk_result))

#     async def _embed(self, chunk_result):
#         document_info = DocumentInfo(**chunk_result["document_info"])
#         await self.ingestion_service.update_document_status(
#             document_info,
#             status=IngestionStatus.EMBEDDING,
#         )

#         chunks = chunk_result["chunks"]
#         embedding_generator = await self.ingestion_service.embed_document(chunks)
#         embeddings = [embedding async for embedding in embedding_generator]

#         await self.ingestion_service.update_document_status(
#             document_info,
#             status=IngestionStatus.STORING,
#         )

#         storage_generator = await self.ingestion_service.store_embeddings(embeddings)
#         async for _ in storage_generator:
#             pass

#         return {
#             "document_info": document_info.to_dict(),
#             "workflow_input": chunk_result["workflow_input"],
#         }

#     @shared_task(bind=True)
#     def finalize(self, embed_result):
#         return asyncio.run(self._finalize(embed_result))

#     async def _finalize(self, embed_result):
#         document_info = DocumentInfo(**embed_result["document_info"])
#         is_update = embed_result["workflow_input"].get("is_update", False)

#         await self.ingestion_service.finalize_ingestion(document_info, is_update=is_update)
#         await self.ingestion_service.update_document_status(
#             document_info,
#             status=IngestionStatus.SUCCESS,
#         )

#         return {
#             "status": "Successfully finalized ingestion",
#             "document_info": document_info.to_dict(),
#         }

#     def create_workflow(self):
#         return chain(
#             self.parse.s(),
#             self.chunk.s(),
#             self.embed.s(),
#             self.finalize.s()
#         )

# # from celery import shared_task, chain
# # import asyncio
# # import logging

# # from core.base import IngestionStatus
# # from core.base.abstractions import DocumentInfo, R2RException
# # from ..services import IngestionService, IngestionServiceAdapter

# # logger = logging.getLogger(__name__)

# # class IngestFilesWorkflow:
# #     def __init__(self, ingestion_service: IngestionService):
# #         self.ingestion_service = ingestion_service

# #     @shared_task(name="IngestFilesWorkflow.parse")
# #     def parse(self, workflow_input):
# #         return asyncio.run(self._parse(workflow_input))

# #     async def _parse(self, workflow_input):
# #         parsed_data = IngestionServiceAdapter.parse_ingest_file_input(workflow_input)
# #         ingestion_result = await self.ingestion_service.ingest_file_ingress(**parsed_data)
# #         document_info = ingestion_result["info"]

# #         await self.ingestion_service.update_document_status(
# #             document_info,
# #             status=IngestionStatus.PARSING,
# #         )

# #         extractions_generator = await self.ingestion_service.parse_file(document_info)
# #         extractions = [extraction async for extraction in extractions_generator]
# #         serializable_extractions = [fragment.to_dict() for fragment in extractions]

# #         return {
# #             "status": "Successfully extracted data",
# #             "extractions": serializable_extractions,
# #             "document_info": document_info.to_dict(),
# #             "workflow_input": workflow_input,
# #         }

# #     @shared_task(name="IngestFilesWorkflow.chunk")
# #     def chunk(self, parse_result):
# #         return asyncio.run(self._chunk(parse_result))

# #     async def _chunk(self, parse_result):
# #         document_info = DocumentInfo(**parse_result["document_info"])
# #         await self.ingestion_service.update_document_status(
# #             document_info,
# #             status=IngestionStatus.CHUNKING,
# #         )

# #         extractions = parse_result["extractions"]
# #         chunking_config = parse_result["workflow_input"].get("chunking_config")

# #         chunk_generator = await self.ingestion_service.chunk_document(extractions, chunking_config)
# #         chunks = [chunk async for chunk in chunk_generator]
# #         serializable_chunks = [chunk.to_dict() for chunk in chunks]

# #         return {
# #             "status": "Successfully chunked data",
# #             "chunks": serializable_chunks,
# #             "document_info": document_info.to_dict(),
# #             "workflow_input": parse_result["workflow_input"],
# #         }

# #     @shared_task(name="IngestFilesWorkflow.embed")
# #     def embed(self, chunk_result):
# #         return asyncio.run(self._embed(chunk_result))

# #     async def _embed(self, chunk_result):
# #         document_info = DocumentInfo(**chunk_result["document_info"])
# #         await self.ingestion_service.update_document_status(
# #             document_info,
# #             status=IngestionStatus.EMBEDDING,
# #         )

# #         chunks = chunk_result["chunks"]
# #         embedding_generator = await self.ingestion_service.embed_document(chunks)
# #         embeddings = [embedding async for embedding in embedding_generator]

# #         await self.ingestion_service.update_document_status(
# #             document_info,
# #             status=IngestionStatus.STORING,
# #         )

# #         storage_generator = await self.ingestion_service.store_embeddings(embeddings)
# #         async for _ in storage_generator:
# #             pass

# #         return {
# #             "document_info": document_info.to_dict(),
# #             "workflow_input": chunk_result["workflow_input"],
# #         }

# #     @shared_task(name="IngestFilesWorkflow.finalize")
# #     def finalize(self, embed_result):
# #         return asyncio.run(self._finalize(embed_result))

# #     async def _finalize(self, embed_result):
# #         document_info = DocumentInfo(**embed_result["document_info"])
# #         is_update = embed_result["workflow_input"].get("is_update", False)

# #         await self.ingestion_service.finalize_ingestion(document_info, is_update=is_update)
# #         await self.ingestion_service.update_document_status(
# #             document_info,
# #             status=IngestionStatus.SUCCESS,
# #         )

# #         return {
# #             "status": "Successfully finalized ingestion",
# #             "document_info": document_info.to_dict(),
# #         }

# #     def create_workflow(self):
# #         return chain(
# #             self.parse.s(),
# #             self.chunk.s(),
# #             self.embed.s(),
# #             self.finalize.s()
# #         )
# # # from celery import shared_task
# # # import asyncio
# # # import logging

# # # from hatchet_sdk import Context

# # # from core.base import IngestionStatus, increment_version
# # # from core.base.abstractions import DocumentInfo, R2RException

# # # from ..services import IngestionService, IngestionServiceAdapter

# # # logger = logging.getLogger(__name__)


# # # class IngestFilesWorkflow:
# # #     def __init__(self, ingestion_service: IngestionService):
# # #         self.ingestion_service = ingestion_service
# # #         self._create_ingestion_workflow()

# # #     def _create_ingestion_workflow(self):
# # #         orchestration_app = self.ingestion_service.providers.orchestration

# # #         @orchestration_app.workflow(name="ingest_files")
# # #         def ingest_files_workflow(
# # #             file_data,
# # #             document_id,
# # #             metadata,
# # #             chunking_config,
# # #             user,
# # #             size_in_bytes,
# # #             is_update,
# # #         ):
# # #             parse_result = parse.delay(
# # #                 file_data,
# # #                 document_id,
# # #                 metadata,
# # #                 chunking_config,
# # #                 user,
# # #                 size_in_bytes,
# # #                 is_update,
# # #             )
# # #             chunk_result = chunk.delay(parse_result)
# # #             embed_result = embed.delay(chunk_result)
# # #             finalize_result = finalize.delay(embed_result)
# # #             return finalize_result

# # #     @shared_task(name="IngestFilesWorkflow.parse")
# # #     async def parse(self, context: Context) -> dict:
# # #         input_data = context.workflow_input()["request"]
# # #         print("input_data = ", input_data)
# # #         parsed_data = IngestionServiceAdapter.parse_ingest_file_input(
# # #             input_data
# # #         )

# # #         ingestion_result = (
# # #             await self.ingestion_service.ingest_file_ingress(**parsed_data)
# # #         )

# # #         document_info = ingestion_result["info"]

# # #         await self.ingestion_service.update_document_status(
# # #             document_info,
# # #             status=IngestionStatus.PARSING,
# # #         )

# # #         extractions_generator = await self.ingestion_service.parse_file(
# # #             document_info
# # #         )

# # #         extractions = []
# # #         async for extraction in extractions_generator:
# # #             extractions.append(extraction)

# # #         serializable_extractions = [
# # #             fragment.to_dict() for fragment in extractions
# # #         ]

# # #         return {
# # #             "status": "Successfully extracted data",
# # #             "extractions": serializable_extractions,
# # #             "document_info": document_info.to_dict(),
# # #         }

# # #     @shared_task(name="IngestFilesWorkflow.chunk")
# # #     async def chunk(self, context: Context) -> dict:
# # #         document_info_dict = context.step_output("parse")["document_info"]
# # #         document_info = DocumentInfo(**document_info_dict)

# # #         await self.ingestion_service.update_document_status(
# # #             document_info,
# # #             status=IngestionStatus.CHUNKING,
# # #         )

# # #         extractions = context.step_output("parse")["extractions"]
# # #         chunking_config = context.workflow_input()["request"].get(
# # #             "chunking_config"
# # #         )

# # #         chunk_generator = await self.ingestion_service.chunk_document(
# # #             extractions,
# # #             chunking_config,
# # #         )

# # #         chunks = []
# # #         async for chunk in chunk_generator:
# # #             chunks.append(chunk)

# # #         serializable_chunks = [chunk.to_dict() for chunk in chunks]

# # #         return {
# # #             "status": "Successfully chunked data",
# # #             "chunks": serializable_chunks,
# # #             "document_info": document_info.to_dict(),
# # #         }

# # #     @shared_task(name="IngestFilesWorkflow.embed")
# # #     async def embed(self, context: Context) -> dict:
# # #         document_info_dict = context.step_output("chunk")["document_info"]
# # #         document_info = DocumentInfo(**document_info_dict)

# # #         await self.ingestion_service.update_document_status(
# # #             document_info,
# # #             status=IngestionStatus.EMBEDDING,
# # #         )

# # #         chunks = context.step_output("chunk")["chunks"]

# # #         embedding_generator = await self.ingestion_service.embed_document(
# # #             chunks
# # #         )

# # #         embeddings = []
# # #         async for embedding in embedding_generator:
# # #             embeddings.append(embedding)

# # #         await self.ingestion_service.update_document_status(
# # #             document_info,
# # #             status=IngestionStatus.STORING,
# # #         )

# # #         storage_generator = await self.ingestion_service.store_embeddings(  # type: ignore
# # #             embeddings
# # #         )

# # #         async for _ in storage_generator:
# # #             pass

# # #         return {
# # #             "document_info": document_info.to_dict(),
# # #         }

# # #     @shared_task(name="IngestFilesWorkflow.finalize")
# # #     async def finalize(self, context: Context) -> dict:
# # #         document_info_dict = context.step_output("embed")["document_info"]
# # #         document_info = DocumentInfo(**document_info_dict)

# # #         is_update = context.workflow_input()["request"].get("is_update")

# # #         await self.ingestion_service.finalize_ingestion(
# # #             document_info, is_update=is_update
# # #         )

# # #         await self.ingestion_service.update_document_status(
# # #             document_info,
# # #             status=IngestionStatus.SUCCESS,
# # #         )

# # #         return {
# # #             "status": "Successfully finalized ingestion",
# # #             "document_info": document_info.to_dict(),
# # #         }

# # #     def create_workflow(self):
# # #         return (self.parse.s() | self.chunk.s() | self.embed.s() | self.finalize.s())

# # #         # @r2r_hatchet.on_failure_step()
# # #         # async def on_failure(self, context: Context) -> None:
# # #         #     request = context.workflow_input().get("request", {})
# # #         #     document_id = request.get("document_id")

# # #         #     if not document_id:
# # #         #         logger.error(
# # #         #             "No document id was found in workflow input to mark a failure."
# # #         #         )
# # #         #         return

# # #         #     try:
# # #         #         documents_overview = await self.ingestion_service.providers.database.relational.get_documents_overview(
# # #         #             filter_document_ids=[document_id]
# # #         #         )

# # #         #         if not documents_overview:
# # #         #             logger.error(
# # #         #                 f"Document with id {document_id} not found in database to mark failure."
# # #         #             )
# # #         #             return

# # #         #         document_info = documents_overview[0]

# # #         #         # Update the document status to FAILURE
# # #         #         await self.ingestion_service.update_document_status(
# # #         #             document_info,
# # #         #             status=IngestionStatus.FAILURE,
# # #         #         )

# # #         #     except Exception as e:
# # #         #         logger.error(
# # #         #             f"Failed to update document status for {document_id}: {e}"
# # #         #         )


# # # # from celery import shared_task
# # # # import asyncio
# # # # import logging

# # # # from hatchet_sdk import Context

# # # # from core.base import IngestionStatus, increment_version
# # # # from core.base.abstractions import DocumentInfo, R2RException

# # # # from ..services import IngestionService, IngestionServiceAdapter

# # # # logger = logging.getLogger(__name__)


# # # # class IngestFilesWorkflow:
# # # #     def __init__(self, ingestion_service: IngestionService):
# # # #         self.ingestion_service = ingestion_service
# # # #         self._create_ingestion_workflow()

# # # #     def _create_ingestion_workflow(self):
# # # #         orchestration_app = self.ingestion_service.providers.orchestration

# # # #         @orchestration_app.workflow(name="ingest_files")
# # # #         def ingest_files_workflow(
# # # #             file_data,
# # # #             document_id,
# # # #             metadata,
# # # #             chunking_config,
# # # #             user,
# # # #             size_in_bytes,
# # # #             is_update,
# # # #         ):
# # # #             parse_result = parse.delay(
# # # #                 file_data,
# # # #                 document_id,
# # # #                 metadata,
# # # #                 chunking_config,
# # # #                 user,
# # # #                 size_in_bytes,
# # # #                 is_update,
# # # #             )
# # # #             chunk_result = chunk.delay(parse_result)
# # # #             embed_result = embed.delay(chunk_result)
# # # #             finalize_result = finalize.delay(embed_result)
# # # #             return finalize_result

# # # #         @orchestration_app.step()
# # # #         async def parse(self, context: Context) -> dict:
# # # #             input_data = context.workflow_input()["request"]
# # # #             print("input_data = ", input_data)
# # # #             parsed_data = IngestionServiceAdapter.parse_ingest_file_input(
# # # #                 input_data
# # # #             )

# # # #             ingestion_result = (
# # # #                 await self.ingestion_service.ingest_file_ingress(**parsed_data)
# # # #             )

# # # #             document_info = ingestion_result["info"]

# # # #             await self.ingestion_service.update_document_status(
# # # #                 document_info,
# # # #                 status=IngestionStatus.PARSING,
# # # #             )

# # # #             extractions_generator = await self.ingestion_service.parse_file(
# # # #                 document_info
# # # #             )

# # # #             extractions = []
# # # #             async for extraction in extractions_generator:
# # # #                 extractions.append(extraction)

# # # #             serializable_extractions = [
# # # #                 fragment.to_dict() for fragment in extractions
# # # #             ]

# # # #             return {
# # # #                 "status": "Successfully extracted data",
# # # #                 "extractions": serializable_extractions,
# # # #                 "document_info": document_info.to_dict(),
# # # #             }

# # # #         @orchestration_app.step()
# # # #         async def chunk(self, context: Context) -> dict:
# # # #             document_info_dict = context.step_output("parse")["document_info"]
# # # #             document_info = DocumentInfo(**document_info_dict)

# # # #             await self.ingestion_service.update_document_status(
# # # #                 document_info,
# # # #                 status=IngestionStatus.CHUNKING,
# # # #             )

# # # #             extractions = context.step_output("parse")["extractions"]
# # # #             chunking_config = context.workflow_input()["request"].get(
# # # #                 "chunking_config"
# # # #             )

# # # #             chunk_generator = await self.ingestion_service.chunk_document(
# # # #                 extractions,
# # # #                 chunking_config,
# # # #             )

# # # #             chunks = []
# # # #             async for chunk in chunk_generator:
# # # #                 chunks.append(chunk)

# # # #             serializable_chunks = [chunk.to_dict() for chunk in chunks]

# # # #             return {
# # # #                 "status": "Successfully chunked data",
# # # #                 "chunks": serializable_chunks,
# # # #                 "document_info": document_info.to_dict(),
# # # #             }

# # # #         @orchestration_app.step()
# # # #         async def embed(self, context: Context) -> dict:
# # # #             document_info_dict = context.step_output("chunk")["document_info"]
# # # #             document_info = DocumentInfo(**document_info_dict)

# # # #             await self.ingestion_service.update_document_status(
# # # #                 document_info,
# # # #                 status=IngestionStatus.EMBEDDING,
# # # #             )

# # # #             chunks = context.step_output("chunk")["chunks"]

# # # #             embedding_generator = await self.ingestion_service.embed_document(
# # # #                 chunks
# # # #             )

# # # #             embeddings = []
# # # #             async for embedding in embedding_generator:
# # # #                 embeddings.append(embedding)

# # # #             await self.ingestion_service.update_document_status(
# # # #                 document_info,
# # # #                 status=IngestionStatus.STORING,
# # # #             )

# # # #             storage_generator = await self.ingestion_service.store_embeddings(  # type: ignore
# # # #                 embeddings
# # # #             )

# # # #             async for _ in storage_generator:
# # # #                 pass

# # # #             return {
# # # #                 "document_info": document_info.to_dict(),
# # # #             }

# # # #         @orchestration_app.step()
# # # #         async def finalize(self, context: Context) -> dict:
# # # #             document_info_dict = context.step_output("embed")["document_info"]
# # # #             document_info = DocumentInfo(**document_info_dict)

# # # #             is_update = context.workflow_input()["request"].get("is_update")

# # # #             await self.ingestion_service.finalize_ingestion(
# # # #                 document_info, is_update=is_update
# # # #             )

# # # #             await self.ingestion_service.update_document_status(
# # # #                 document_info,
# # # #                 status=IngestionStatus.SUCCESS,
# # # #             )

# # # #             return {
# # # #                 "status": "Successfully finalized ingestion",
# # # #                 "document_info": document_info.to_dict(),
# # # #             }

# # # #         # @r2r_hatchet.on_failure_step()
# # # #         # async def on_failure(self, context: Context) -> None:
# # # #         #     request = context.workflow_input().get("request", {})
# # # #         #     document_id = request.get("document_id")

# # # #         #     if not document_id:
# # # #         #         logger.error(
# # # #         #             "No document id was found in workflow input to mark a failure."
# # # #         #         )
# # # #         #         return

# # # #         #     try:
# # # #         #         documents_overview = await self.ingestion_service.providers.database.relational.get_documents_overview(
# # # #         #             filter_document_ids=[document_id]
# # # #         #         )

# # # #         #         if not documents_overview:
# # # #         #             logger.error(
# # # #         #                 f"Document with id {document_id} not found in database to mark failure."
# # # #         #             )
# # # #         #             return

# # # #         #         document_info = documents_overview[0]

# # # #         #         # Update the document status to FAILURE
# # # #         #         await self.ingestion_service.update_document_status(
# # # #         #             document_info,
# # # #         #             status=IngestionStatus.FAILURE,
# # # #         #         )

# # # #         #     except Exception as e:
# # # #         #         logger.error(
# # # #         #             f"Failed to update document status for {document_id}: {e}"
# # # #         #         )
