import pandas as pd
from scipy.stats import ks_2samp, chi2_contingency


class DriftDetector:
    def __init__(self, df_prev: pd.DataFrame, df_curr: pd.DataFrame):
        self.df_prev = df_prev
        self.df_curr = df_curr

    def analyze_numeric_drift(self, col_name: str, threshold: float = 0.05):
        """Считает KS-тест для числовых колонок."""
        if col_name not in self.df_prev.columns or col_name not in self.df_curr.columns:
            return {"error": f"Колонка {col_name} не найдена"}

        prev_data = self.df_prev[col_name].dropna()
        curr_data = self.df_curr[col_name].dropna()

        if len(prev_data) == 0 or len(curr_data) == 0:
            return {"error": "Недостаточно данных для анализа"}

        stat, p_value = ks_2samp(prev_data, curr_data)
        is_drift = bool(p_value < threshold)

        return {
            "column": col_name,
            "type": "numeric",
            "mean_prev": float(prev_data.mean()),
            "mean_curr": float(curr_data.mean()),
            "stat": float(stat),
            "p_value": float(p_value),
            "is_drift": is_drift
        }

    def analyze_categorical_drift(self, col_name: str, threshold: float = 0.05):
        """Считает Chi-square тест для категориальных (строковых) колонок."""
        if col_name not in self.df_prev.columns or col_name not in self.df_curr.columns:
            return {"error": f"Колонка {col_name} не найдена"}

        # Считаем частоты (value_counts)
        prev_counts = self.df_prev[col_name].value_counts()
        curr_counts = self.df_curr[col_name].value_counts()

        # Объединяем в одну таблицу (contingency table)
        contingency_table = pd.concat([prev_counts, curr_counts], axis=1).fillna(0)

        # Если в таблице меньше 2 строк или 2 столбцов, тест не имеет смысла
        if contingency_table.shape[0] < 2 or contingency_table.shape[1] < 2:
            return {"error": "Недостаточно уникальных значений для Chi-square теста"}

        # Запускаем тест
        stat, p_value, _, _ = chi2_contingency(contingency_table)
        is_drift = bool(p_value < threshold)

        return {
            "column": col_name,
            "type": "categorical",
            "stat": float(stat),
            "p_value": float(p_value),
            "is_drift": is_drift
        }