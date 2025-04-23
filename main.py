import os

from fastapi import FastAPI
import textract
from pdfminer.high_level import extract_text
from starlette.middleware.cors import CORSMiddleware

from routers.cv_router import router
from routers import cv_router


app = FastAPI()
origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:8000",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router, prefix="/cv", tags=["CV Processing"])

@app.get("/")
async def root():
    return {"message": "LM Studio API is running!"}


