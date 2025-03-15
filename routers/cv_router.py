import logging
from fastapi import APIRouter, UploadFile, File, FastAPI
import asyncio
from Service.CVProcessor.CVProcessor import CVProcessor
from Service.Kafka.KafkaClient import KafkaClient
from Service.Proccess_CV.ProccessService import upload_cv_service


app = FastAPI()
router = APIRouter()
queue = asyncio.Queue()
# @router.post("/upload-cv/")
# async def upload_cv(file: UploadFile = File(...)):
#     return await upload_cv_service(file)


# @router.post("/upload-cv/")
# async def upload_cv(files: list[UploadFile] = File(...)):
#     """Nhận nhiều file, xử lý và gửi lên Kafka trước khi phản hồi"""
#
#     results = []  # Danh sách chứa kết quả xử lý của từng file
#
#     for file in files:
#         file_content = await file.read()
#         filename = file.filename
#
#         # Gọi trực tiếp hàm xử lý file và đợi kết quả trước khi tiếp tục
#         result = await upload_cv_service(file_content, filename)
#         results.append(result)
#
#     return {"message": "Xử lý hoàn tất", "results": results}

logging.basicConfig(level=logging.DEBUG)
kafka_client = KafkaClient("localhost:29092")
processor = CVProcessor(kafka_client)

@router.post("/upload-cv/")
async def upload_cv(files: list[UploadFile] = File(...)):
    """Nhận nhiều file, đưa vào hàng đợi để xử lý tuần tự."""
    for file in files:
        file_content = await file.read()
        filename = file.filename
        await processor.add_task(file_content, filename)
    return {"message": "Các file đã được thêm vào hàng đợi để xử lý tuần tự."}
#
# app.include_router(router)
