import io
import os
import re
from pdfminer.high_level import extract_text

def remove_icons(text):
    """
    Loại bỏ các ký tự icon đặc biệt trong văn bản.
    """
    return re.sub(r'[\uf000-\uf8ff]', '', text)  # Loại bỏ các ký tự Unicode Private Use Area (PUA)

def convert_pdf_to_txt(file_content: bytes) -> str:
    """
    Chuyển đổi nội dung file PDF (bytes) sang văn bản và loại bỏ icon.
    :param file_content: Nội dung file PDF dưới dạng bytes
    :return: Chuỗi văn bản đã xử lý
    """

    text = extract_text(io.BytesIO(file_content))


    cleaned_text = remove_icons(text)


    return cleaned_text.strip()

