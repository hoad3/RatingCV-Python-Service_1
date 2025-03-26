class extract_name:
    @staticmethod
    def extract_name_from_cv(text):
        # Chuẩn hóa text (xoá khoảng trắng dư thừa)
        lines = [line.strip() for line in text.split("\n") if line.strip()]

        # 1️⃣ Ưu tiên tìm trong phần "Thông tin liên hệ"
        for i, line in enumerate(lines):
            if "Thông Tin Liên Hệ" in line or "Contact" in line:
                if i + 1 < len(lines):  # Dòng sau "Thông Tin Liên Hệ" có thể chứa tên
                    return extract_name.standardize_name(lines[i + 1])

        # 2️⃣ Nếu không có, lấy dòng có nhiều chữ in hoa, nhưng không phải tiêu đề khác
        uppercase_lines = [line for line in lines if line.isupper() and len(line.split()) > 1]
        if uppercase_lines:
            return extract_name.standardize_name(uppercase_lines[0])

        # 3️⃣ Nếu vẫn không có, lấy dòng đầu tiên có từ 2-4 từ và chữ cái đầu viết hoa
        for line in lines:
            words = line.split()
            if 2 <= len(words) <= 4 and all(word[0].isupper() for word in words if word.isalpha()):
                return extract_name.standardize_name(line)

        return "No name found"

    @staticmethod
    def standardize_name(name):
        """Viết hoa chữ cái đầu của từng từ"""
        return " ".join(word.capitalize() for word in name.split())
