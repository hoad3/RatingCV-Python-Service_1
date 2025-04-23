import re
from typing import List

import requests


class GitHubExtractor:
    GITHUB_BASE = "https://github.com/"

    @staticmethod
    def extract_github_links(content: str):
        """
        Trích xuất và kiểm tra link GitHub, thử từng chữ nếu repo không hợp lệ.
        """
        # ✅ Giữ nguyên dấu "-" khi nối lại link bị xuống dòng
        content = re.sub(r'(https?:\/\/github\.com\/[^\s]+)-\s+([^\s]+)', r'\1-\2', content)
        content = re.sub(r'(https?:\/\/github\.com\/[^\s]+)\s+([^\s]+)', r'\1\2', content)

        # 🔍 Tìm các link GitHub hợp lệ
        pattern = r'https?:\/\/github\.com\/([a-zA-Z0-9-]+)\/([a-zA-Z0-9-_.]+)'
        matches = re.findall(pattern, content)

        github_links = [f"https://github.com/{user}/{repo}" for user, repo in matches]
        unique_links = list(dict.fromkeys(github_links))

        # ✅ Kiểm tra repo bằng cách xóa từng chữ cái nếu không hợp lệ
        valid_links = [GitHubExtractor.verify_github_link(link) for link in unique_links]
        return [link for link in valid_links if link]

    @staticmethod
    def verify_github_link(base_link: str) -> str:
        """
        Kiểm tra xem link GitHub có hợp lệ không, thử từng chữ nếu repo không hợp lệ.
        """
        match = re.match(r'https://github.com/([a-zA-Z0-9-]+)/([a-zA-Z0-9-_.]+)', base_link)
        if not match:
            return ""

        user, repo = match.groups()

        if not repo:
            return ""


        for i in range(len(repo), 0, -1):
            candidate_repo = repo[:i]
            full_link = f"https://github.com/{user}/{candidate_repo}"
            if GitHubExtractor.is_valid_github_url(full_link):
                return full_link

        return ""

    @staticmethod
    def is_valid_github_url(url: str) -> bool:
        """
        Gửi request HTTP kiểm tra xem link GitHub có hợp lệ không.
        """
        try:
            response = requests.get(url, timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            return False