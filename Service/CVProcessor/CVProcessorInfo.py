import json
import re

import ollama
from underthesea import word_tokenize, ner
class CVDetailExtractor:

    PROGRAMMING_LANGUAGES = {"Python", "Java", "JavaScript", "C", "C++", "C#", "Go", "Rust",
                              "Swift", "Kotlin", "Ruby", "TypeScript", "Dart", "PHP", "R",
                              "Scala", "Perl", "Haskell", "Lua", "Shell", "Elixir", "Clojure",
                              "MATLAB", "Objective-C", "F#", "Visual Basic", "COBOL", "Fortran"}

    FRAMEWORKS = {"Flutter", "React", "Vue", "Angular", "Django", "Spring", "FastAPI", "NestJS",
                  "ExpressJS", "Laravel", "ASP.NET Core", "ASP.NET", "State managemant: Bloc", "Nuxt", "Next.js",
                  "Resful API: Https Method"}

    DATABASES = {"MySQL", "PostgreSQL", "Microsoft SQL Server", "MongoDB", "SQLite", "Oracle", "Firebase", "MariaDB", "Redis",
                 "DynamoDB", "Elasticsearch", "SqlServer"}

    @staticmethod
    def extract_certificate(text):
        """Trích xuất chứng chỉ từ CV bằng regex"""
        CERTIFICATES = {"Kỹ sư", "Thạc sĩ", "Tiến sĩ"}

        found_certificates = set()
        for cert in CERTIFICATES:
            pattern = rf"\b{re.escape(cert)}\b"  # Đảm bảo khớp nguyên từ
            if re.search(pattern, text, re.IGNORECASE):
                found_certificates.add(cert)

        return list(found_certificates) if found_certificates else ["N/A"]
    @staticmethod
    def extract_github_projects(text):
        """Đếm số lượng dự án dựa theo số lần xuất hiện link github"""
        return len(re.findall(r"https://github\.com/[^\s]+", text))

    @staticmethod
    def extract_keywords(text, keyword_set):
        """Tìm các từ khóa (ngôn ngữ lập trình, framework, database) trong văn bản"""
        found_keywords = set()

        sorted_keywords = sorted(keyword_set, key=len, reverse=True)

        for keyword in sorted_keywords:
            if keyword == "C":
                pattern = r"\bC\b(?!#|\+\+)"
            else:
                pattern = rf"(?<!\w){re.escape(keyword)}(?!\w)"

            if re.search(pattern, text, re.IGNORECASE):
                found_keywords.add(keyword)

        return list(found_keywords)
    @staticmethod
    def extract_details(content: str) -> dict:
        programming_languages = CVDetailExtractor.extract_keywords(content, CVDetailExtractor.PROGRAMMING_LANGUAGES)
        frameworks = CVDetailExtractor.extract_keywords(content, CVDetailExtractor.FRAMEWORKS)
        databases = CVDetailExtractor.extract_keywords(content, CVDetailExtractor.DATABASES)
        num_projects = CVDetailExtractor.extract_github_projects(content)
        certificates = CVDetailExtractor.extract_certificate(content)
        prompt = f"""
           Bạn là một chuyên gia phân tích CV. Hãy trích xuất thông tin sau từ văn bản CV và trả về đúng định dạng JSON với các trường sau:
           {{
               "hoc_van": "Tên trường học, đã tốt nghiệp hay chưa, GPA bao nhiêu (nếu không có thì ghi "N/A").
               "phone": "Số điện thoại chỉ được phép 10 hoặc 11 số lấy đúng như trong văn bản không được phép để trống phần này",
           }}

           CV:
           {content}

           Lưu ý:
           - Trả về kết quả dưới dạng JSON **thuần túy**, không thêm bất kỳ mô tả nào. Chỉ trả về JSON.
           - Không được thiếu bất kỳ trường nào.
           """

        try:
            response = ollama.chat(model="llama3.2:latest", messages=[{"role": "user", "content": prompt}])
            content_text = response.get("message", {}).get("content", "").strip()
            parsed_data = json.loads(content_text) if content_text.startswith("{") else {}

            # Ghép kết quả regex vào JSON trả về
            parsed_data["chung_chi"] = certificates[0]
            parsed_data["cong_nghe"] = programming_languages
            parsed_data["framework"] = frameworks
            parsed_data["data_base"] = databases
            parsed_data["kinh_nghiem"] = f"{num_projects} dự án"

            return parsed_data

        except Exception as e:
            return {"error": str(e)}
