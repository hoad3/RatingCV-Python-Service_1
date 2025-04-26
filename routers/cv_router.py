import logging

from fastapi import APIRouter, UploadFile, File, FastAPI, HTTPException
import asyncio

from Service.AHP.CadidateAHP import CadidateAHP
from Service.CVProcessor.CVProcessor import CVProcessor
from Service.Kafka.KafkaClient import KafkaClient
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from typing import List
from Service.AHP.CalculatorSum import AHPCalculator


app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = APIRouter()
queue = asyncio.Queue()

logging.basicConfig(level=logging.DEBUG)
kafka_client = KafkaClient("localhost:29092")
processor = CVProcessor(kafka_client)

class CriteriaMatrix(BaseModel):
    matrix: List[List[float]] = Field(
        ...,
        description="Ma trận so sánh cặp các tiêu chí"
    )

    @validator('matrix')
    def validate_matrix(cls, v):
        if not v:
            raise ValueError('Ma trận không được để trống')
        if not all(len(row) == len(v) for row in v):
            raise ValueError('Ma trận phải là ma trận vuông')
        if not all(v[i][i] == 1 for i in range(len(v))):
            raise ValueError('Các phần tử trên đường chéo chính phải bằng 1')
        if not all(abs(v[i][j] * v[j][i] - 1) < 1e-10 for i in range(len(v)) for j in range(len(v))):
            raise ValueError('Ma trận phải thỏa mãn tính chất đối xứng nghịch đảo')
        return v

@router.post("/upload-cv/")
async def upload_cv(files: list[UploadFile] = File(...)):
    """Nhận nhiều file, đưa vào hàng đợi để xử lý tuần tự."""
    for file in files:
        file_content = await file.read()
        filename = file.filename
        await processor.add_task(file_content, filename)
    return {"message": "Các file đã được thêm vào hàng đợi để xử lý tuần tự."}

@router.post("/calculate-ahp")
async def calculate_ahp(criteria_matrix: CriteriaMatrix):

    try:
        matrix_data = [list(row) for row in criteria_matrix.matrix]
        
        calculator = AHPCalculator(matrix_data)
        
        metrics = calculator.calculate_all_metrics()
        
        return metrics
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logging.error(f"Lỗi khi tính toán AHP: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Lỗi khi tính toán AHP: {str(e)}"
        )

class MatrixInput(BaseModel):
    matrix: List[List[float]]
    ri_value: float
@router.post("/calculate_ahp_cadidate")
async def calculate_ahp_cadidate(input_data: MatrixInput):
    try:
        ahp = CadidateAHP(input_data.matrix, input_data.ri_value)

        if not ahp.validate_matrix():
            raise HTTPException(status_code=400, detail="Ma trận không hợp lệ. Đảm bảo tính đối xứng nghịch đảo và đường chéo bằng 1.")

        result = ahp.calculate_CadidateAHP()
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#
# app.include_router(router)
