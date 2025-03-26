import pdfplumber
import re
import io

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
    text = ""
    with pdfplumber.open(io.BytesIO(file_content)) as pdf:
        for page in pdf.pages:
            text += page.extract_text() + "\n" if page.extract_text() else ""

    cleaned_text = remove_icons(text)
    return cleaned_text.strip()
