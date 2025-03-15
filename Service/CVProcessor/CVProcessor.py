import asyncio
import base64
import io
import json
import logging

import docx
import ollama

from Service.CVProcessor.CVProcessorInfo import CVDetailExtractor
from Service.Kafka import KafkaClient
from Service.Xu_ly.Xu_ly_file import convert_pdf_to_txt


class CVAnalyzer:
    @staticmethod
    def extract_text(file_content: bytes, filename: str) -> str:
        if filename.endswith(".pdf"):
            return convert_pdf_to_txt(file_content)
        elif filename.endswith(".docx"):
            doc = docx.Document(io.BytesIO(file_content))
            return "\n".join([p.text for p in doc.paragraphs]).strip()
        else:
            return file_content.decode("utf-8", errors="ignore").strip()

    @staticmethod
    def analyze(content: str, filename: str) -> dict:
        prompt = f"""
        Hãy phân tích CV sau và trả về đúng định dạng JSON với các trường sau:
        {{
            "name": "Họ và tên đầy đủ sẽ nằm trên cùng một dòng, Chữ cái đầu của mỗi từ viết hoa, không được phép để trống.",   
            "phone": "Số điện thoại 10 hoặc 11 số",
            "email": "Email",
            "address": "Địa chỉ",
            "github": "Link GitHub (nếu có)"
        }}

        CV:
        {content}

        Lưu ý:
            - Chỉ trả về JSON thuần túy, không có văn bản bổ sung.
            - Không được thiếu bất kỳ trường nào.
        """
        try:
            response = ollama.chat(model="llama3.2:latest", messages=[{"role": "user", "content": prompt}])
            content_text = response.get("message", {}).get("content", "").strip()
            parsed_data = json.loads(content_text) if content_text.startswith("{") else {}
            parsed_data["ten_cv"] = filename
            return parsed_data
        except Exception as e:
            return {"error": str(e)}

class CVProcessor:
    def __init__(self, kafka_client: KafkaClient):
        self.kafka_client = kafka_client
        self.queue = asyncio.Queue()
        self.processing = False
        asyncio.create_task(self.worker())

    async def worker(self):
        while True:
            file_content, filename = await self.queue.get()
            try:
                await self.process_cv(file_content, filename)
            except Exception as e:
                logging.error(f"Lỗi khi xử lý file {filename}: {e}")
            finally:
                self.queue.task_done()


    async def add_task(self, file_content: bytes, filename: str):
        await self.queue.put((file_content, filename))

    async def process_cv(self, file_content: bytes, filename: str):
        text_content = CVAnalyzer.extract_text(file_content, filename)
        if not text_content:
            logging.error(f"Không thể trích xuất nội dung từ {filename}")
            return {"error": f"Không thể trích xuất nội dung từ {filename}"}

        extracted_data = CVAnalyzer.analyze(text_content, filename)
        self.kafka_client.send("cv-data", extracted_data)

        detailed_info = CVDetailExtractor.extract_details(text_content)
        self.kafka_client.send("info-ungvien", detailed_info)

        encoded_file = base64.b64encode(file_content).decode("utf-8")
        file_message = {"filename": filename, "file_content": encoded_file}
        self.kafka_client.send("cv-file", file_message)
        logging.info(f"File {filename} đã được xử lý và gửi lên Kafka")
        return {"filename": filename, "status": "processed"}