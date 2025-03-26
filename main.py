import os

from fastapi import FastAPI
import textract
from pdfminer.high_level import extract_text
from routers.cv_router import router
from routers import cv_router


app = FastAPI()

app.include_router(router, prefix="/cv", tags=["CV Processing"])

@app.get("/")
async def root():
    return {"message": "LM Studio API is running!"}


