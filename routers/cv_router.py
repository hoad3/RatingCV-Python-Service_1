import logging
from fastapi import APIRouter, UploadFile, File, FastAPI
import asyncio
from Service.CVProcessor.CVProcessor import CVProcessor
from Service.Kafka.KafkaClient import KafkaClient



app = FastAPI()
router = APIRouter()
queue = asyncio.Queue()

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
