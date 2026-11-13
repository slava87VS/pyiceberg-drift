from .drift_detector import DriftDetector
from pyiceberg.catalog import load_catalog


def check_table_drift(
        catalog_name: str,
        table_name: str,
        column_name: str,
        threshold: float = 0.05,
        **catalog_kwargs
):
    """
    Основная функция библиотеки для проверки дрейфа.

    :param catalog_name: Имя каталога (например, 'default')
    :param table_name: Имя таблицы (например, 'my_table' или 'default.my_table')
    :param column_name: Колонка для проверки
    :param threshold: Порог p-value
    :param catalog_kwargs: Дополнительные параметры для подключения (uri, warehouse и т.д.)
    """
    # 1. Подключаемся к каталогу
    catalog = load_catalog(catalog_name, **catalog_kwargs)

    # Если в имени таблицы нет точки, добавляем неймспейс по умолчанию
    if "." not in table_name:
        table_name = f"default.{table_name}"

    # 2. Загружаем таблицу
    table = catalog.load_table(table_name)
    snapshots = table.metadata.snapshots

    if len(snapshots) < 2:
        raise ValueError("В таблице меньше двух снапшотов. Нечего сравнивать.")

    # Берем самый первый и самый последний снапшот (как вы и просили)
    first_snapshot = snapshots[0]
    last_snapshot = snapshots[-1]

    # 3. Читаем данные
    df_first = table.scan(snapshot_id=first_snapshot.snapshot_id).to_pandas()
    df_last = table.scan(snapshot_id=last_snapshot.snapshot_id).to_pandas()

    # 4. Запускаем детектор
    detector = DriftDetector(df_first, df_last)
    return detector.analyze_numeric_drift(column_name, threshold)