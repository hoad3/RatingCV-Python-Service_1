import numpy as np
from typing import Dict, List, Tuple

class AHPCalculator:
    def __init__(self, criteria_matrix: List[List[float]]):
        """
        Khởi tạo calculator với ma trận tiêu chí
        
        Args:
            criteria_matrix: Ma trận so sánh cặp các tiêu chí
        """
        self.criteria_matrix = np.array(criteria_matrix, dtype=float)
        self.n = len(criteria_matrix)
        
    def calculate_all_metrics(self) -> Dict:
        """
        Tính toán tất cả các chỉ số AHP
        
        Returns:
            Dict: Dictionary chứa tất cả các chỉ số và bảng tính toán:
                - original_matrix: Ma trận tiêu chí gốc
                - normalized_matrix: Ma trận đã chuẩn hóa
                - weight_matrix: Ma trận trọng số
                - weighted_sum: Vector tổng có trọng số
                - lambda_max: Giá trị riêng lớn nhất
                - CI: Chỉ số nhất quán
                - CR: Tỷ lệ nhất quán
                - is_consistent: Trạng thái nhất quán của ma trận
                - sum_row: Hàng tổng của ma trận chuẩn hóa
                - vector_table: Bảng vector
                - cr_table: Bảng CR
        """
        try:
            # Tính tổng các cột
            column_sums = np.sum(self.criteria_matrix, axis=0)
            
            # Chuẩn hóa ma trận
            normalized_matrix = self.criteria_matrix / column_sums
            
            # Tính trọng số (vector)
            weights = np.mean(normalized_matrix, axis=1)

            weighted_matrix = self.criteria_matrix * weights[np.newaxis, :]  # broadcasting
            # Tính vector weighted sum
            weighted_sum = np.dot(self.criteria_matrix, weights)
            
            # Tính lambda max
            lambda_max = float(np.mean(weighted_sum / weights))
            
            # Tính CI
            CI = float((lambda_max - self.n) / (self.n - 1))
            
            # Bảng giá trị RI (Random Index) cho n = 4
            RI = 0.89
            
            # Tính CR
            CR = float(CI / RI)
            
            # Kiểm tra tính nhất quán
            is_consistent = bool(CR < 0.1)
            
            # Tính hàng tổng của ma trận chuẩn hóa
            sum_row = np.sum(normalized_matrix, axis=1)
            
            # Tạo ma trận trọng số
            weight_matrix = np.outer(weights, weights)
            
            # Tạo bảng vector
            vector_table = np.column_stack((weights, weighted_sum, weighted_sum/weights))
            
            # Tạo bảng CR
            cr_table = {
                "lambda_max": lambda_max,
                "CI": CI,
                "RI": RI,
                "CR": CR,
                "is_consistent": is_consistent
            }
            
            return {
                "original_matrix": self.criteria_matrix.tolist(),
                "normalized_matrix": normalized_matrix.tolist(),
                "weighted_matrix": weighted_matrix.tolist(),
                "weight_matrix": weight_matrix.tolist(),
                "weights": weights.tolist(),
                "weighted_sum": weighted_sum.tolist(),
                "lambda_max": lambda_max,
                "CI": CI,
                "CR": CR,
                "is_consistent": is_consistent,
                "column_sums": column_sums.tolist(),
                "sum_row": sum_row.tolist(),
                "vector_table": vector_table.tolist(),
                "cr_table": cr_table
            }
        except Exception as e:
            raise ValueError(f"Lỗi khi tính toán AHP: {str(e)}")
    
    def validate_matrix(self) -> bool:
        """
        Kiểm tra tính hợp lệ của ma trận tiêu chí
        
        Returns:
            bool: True nếu ma trận hợp lệ, False nếu không
        """
        try:
            # Kiểm tra kích thước ma trận
            if self.criteria_matrix.shape[0] != self.criteria_matrix.shape[1]:
                return False
            
            # Kiểm tra các giá trị đối xứng
            for i in range(self.n):
                for j in range(self.n):
                    if abs(self.criteria_matrix[i][j] * self.criteria_matrix[j][i] - 1) > 1e-10:
                        return False
            
            # Kiểm tra các giá trị trên đường chéo
            for i in range(self.n):
                if abs(self.criteria_matrix[i][i] - 1) > 1e-10:
                    return False
            
            return True
        except Exception:
            return False

