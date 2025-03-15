import base64
import io
import json
import ollama
import docx
from fastapi import UploadFile, File
from kafka import KafkaProducer, KafkaConsumer
from kafka.coordinator import consumer
import asyncio

from Service.Xu_ly.Xu_ly_file import convert_pdf_to_txt
import logging

logging.basicConfig(level=logging.DEBUG)

KAFKA_BROKER = "localhost:29092"
TOPIC_CV_DATA = "cv-data"
TOPIC_CV_FILE = "cv-file"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    max_request_size=10485760,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# # **Hàng đợi xử lý file**
queue = asyncio.Queue()

async def worker():
    """Worker xử lý các file trong hàng đợi tuần tự"""
    while True:
        file_content, filename = await queue.get()
        try:
            await upload_cv_service(file_content, filename)
        except Exception as e:
            logging.error(f"Lỗi khi xử lý file {filename}: {e}")
        finally:
            queue.task_done()

# **Khởi động worker trong nền**
async def start_worker():
    asyncio.create_task(worker())

asyncio.create_task(start_worker())


def extract_text_from_file(file_content: bytes, filename: str) -> str:

    text = ""

    if filename.endswith(".pdf"):
        return convert_pdf_to_txt(file_content)
    elif filename.endswith(".docx"):
        doc = docx.Document(io.BytesIO(file_content))
        return "\n".join([p.text for p in doc.paragraphs]).strip()
    else:
        return file_content.decode("utf-8", errors="ignore").strip()

def analyze_cv(content: str, filename: str):
    prompt = f"""
    Hãy phân tích CV sau và trả về đúng định dạng JSON với các trường sau:
    {{
        "name": "Họ và tên đầy đủ phải nằm trên cùng một dòng, Chữ cái đầu của mỗi từ phải viết hoa, giữ nguyên dấu tiếng Việt (nếu có).",   
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

        # Kiểm tra response có dữ liệu không
        if not response or "message" not in response or "content" not in response["message"]:
            return {"error": "Ollama response does not contain expected content"}

        content_text = response["message"]["content"].strip()

        # Kiểm tra dữ liệu có đúng JSON không
        if not content_text.startswith("{") and not content_text.startswith("["):
            return {"error": "Invalid JSON format from Ollama"}

        # Parse JSON từ Ollama
        parsed_data = json.loads(content_text)

        # **Thêm 'ten_cv' vào dữ liệu**
        parsed_data["ten_cv"] = filename

        return parsed_data

    except json.JSONDecodeError:
        return {"error": "Invalid JSON response from Ollama"}

    except Exception as e:
        return {"error": str(e)}

async def upload_cv_service(file_content: bytes, filename: str):
    """Nhận file CV, chuyển thành TXT để phân tích và lưu file gốc lên Kafka"""
    text_content = extract_text_from_file(file_content, filename)
    if not text_content:
        logging.error(f"Không thể trích xuất nội dung từ {filename}")
        return

    # Phân tích nội dung CV
    extracted_data = analyze_cv(text_content, filename)

    # Gửi dữ liệu phân tích lên Kafka
    future = producer.send(TOPIC_CV_DATA, extracted_data)
    result = future.get(timeout=20)  # Đợi phản hồi

    # Mã hóa file và gửi lên Kafka
    encoded_file = base64.b64encode(file_content).decode("utf-8")
    file_message = {"filename": filename, "file_content": encoded_file}
    future_file = producer.send(TOPIC_CV_FILE, file_message)
    future_file.get(timeout=20)

    logging.info(f"File {filename} đã được xử lý và gửi lên Kafka")


