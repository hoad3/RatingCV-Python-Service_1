import asyncio
import base64
import io
import json
import logging

import docx
import ollama
from kafka import KafkaProducer, KafkaConsumer
from Service.CVProcessor import extract_name
from Service.CVProcessor.CVProcessorInfo import CVDetailExtractor
from Service.Kafka import KafkaClient
from Service.Xu_ly.Xu_ly_file import convert_pdf_to_txt


logging.basicConfig(level=logging.DEBUG)

KAFKA_BROKER = "localhost:29092"
TOPIC_CV_DATA = "cv-data"
TOPIC_CV_FILE = "cv-file"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    max_request_size=10485760,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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
            Vui lòng phân tích CV sau và trích xuất dữ liệu dưới dạng JSON với cấu trúc sau:  
            CV:
            {content}
            {{
                "name": "Họ và tên đầy đủ của ứng viên.",
                "phone": "Số điện thoại 10 hoặc 11 chữ số.",
                "email": "Địa chỉ email.",
                "address": "Địa chỉ nhà hoặc nơi làm việc.",
                "github": "Đường dẫn GitHub cá nhân nếu có, nếu không có thì trả về 'N/A'.",
                "hoc_van": "Tên trường học (bắt đầu bằng 'Đại học', 'Cao đẳng' hoặc chứa 'University').",
                "projects": [
                    {{
                        "ten_du_an": "Tên đầy đủ của dự án (GIỮ NGUYÊN 100%, KHÔNG SỬA KÝ TỰ, VIẾT ĐÚNG NHƯ TRONG VĂN BẢN ĐƯA RA). Nếu có lỗi sai, trả về lỗi 'Tên bị thay đổi'.",
                        "mo_ta": "Mô tả về dự án.",
                        "ngay_bat_dau": "Ngày dự án bắt đầu (định dạng MM/YYYY hoặc DD/MM/YYYY).",
                        "ngay_ket_thuc": "Ngày dự án kết thúc thường nằm tiếp nối phía sau ngày bắt đầu",
                        "team_size": "Số lượng người tham gia (nếu có 'dự án cá nhân' thì team_size = 1).",
                        "role": "Vai trò trong dự án.",
                        "github_du_an": "Link GitHub chính xác của dự án, lấy toàn bộ nội dung trên dòng chứa link GitHub gần nhất có liên quan đến dự án. Nếu không tìm thấy, trả về 'N/A' nhưng không tự động thêm giá trị khác."
                    }}
                ]
            
            }}
            
        """

        try:
            response = ollama.chat(model="gemma3:4b", messages=[{"role": "user", "content": prompt}])
            content_text = response.get("message", {}).get("content", "").strip()
            if content_text.startswith("```json"):
                content_text = content_text[7:].strip()  # Loại bỏ ```json
            if content_text.endswith("```"):
                content_text = content_text[:-3].strip()
            parsed_data = json.loads(content_text) if content_text.startswith("{") else {}
            parsed_data["ten_cv"] = filename
            # Tách riêng hoc_van
            hoc_van = parsed_data.pop("hoc_van", "không có thông tin")
            return parsed_data, hoc_van
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
                logging.error(f"Lỗi khi xử lý file")
            finally:
                self.queue.task_done()


    async def add_task(self, file_content: bytes, filename: str):
        await self.queue.put((file_content, filename))

    async def process_cv(self, file_content: bytes, filename: str):
        text_content = CVAnalyzer.extract_text(file_content, filename)
        if not text_content:
            logging.error(f"Không thể trích xuất nội dung từ {filename}")
            return {"error": f"Không thể trích xuất nội dung từ {filename}"}

        # extracted_data = CVAnalyzer.analyze(text_content, filename)
        # Nhận về tuple (extracted_data, hoc_van)
        extracted_data, hoc_van = CVAnalyzer.analyze(text_content, filename)
        self.kafka_client.send("cv-data", extracted_data)

        # 🛠 Lấy số điện thoại từ kết quả JSON
        phone_number = extracted_data.get("phone", "N/A")  # Mặc định "N/A" nếu không có

        # detailed_info = CVDetailExtractor.extract_details(text_content, phone_number)
        # self.kafka_client.send("info-ungvien", detailed_info)
        # Gửi thông tin chi tiết kèm hoc_van
        detailed_info = CVDetailExtractor.extract_details(text_content, phone_number, hoc_van)
        self.kafka_client.send("info-ungvien", detailed_info)

        encoded_file = base64.b64encode(file_content).decode("utf-8")
        file_message = {"filename": filename, "file_content": encoded_file}
        self.kafka_client.send("cv-file", file_message)
        logging.info(f"File {filename} đã được xử lý và gửi lên Kafka")
        return {"filename": filename, "status": "processed"}