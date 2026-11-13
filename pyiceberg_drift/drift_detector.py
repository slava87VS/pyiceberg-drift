import pandas as pd
from scipy.stats import ks_2samp


class DriftDetector:
    def __init__(self, df_prev: pd.DataFrame, df_curr: pd.DataFrame):
        self.df_prev = df_prev
        self.df_curr = df_curr

    def analyze_numeric_drift(self, col_name: str, threshold: float = 0.05):
        """Считает KS-тест для числовых колонок. Если p-value < threshold, дрейф значителен."""
        if col_name not in self.df_prev.columns or col_name not in self.df_curr.columns:
            return {"error": f"Колонка {col_name} не найдена"}

        prev_data = self.df_prev[col_name].dropna()
        curr_data = self.df_curr[col_name].dropna()

        if len(prev_data) == 0 or len(curr_data) == 0:
            return {"error": "Недостаточно данных для анализа"}

        # KS-тест (Kolmogorov-Smirnov)
        stat, p_value = ks_2samp(prev_data, curr_data)

        is_drift = bool(p_value < threshold)

        return {
            "column": col_name,
            "type": "numeric",
            "mean_prev": float(prev_data.mean()),
            "mean_curr": float(curr_data.mean()),
            "ks_stat": float(stat),
            "p_value": float(p_value),
            "is_drift": is_drift
        }