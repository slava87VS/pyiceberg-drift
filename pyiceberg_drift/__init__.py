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
    Проверка дрейфа с использованием PyIceberg (локально или через REST-каталог).
    Сравнивает первый и последний снапшоты.

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

    # Берем самый первый и самый последний снапшот
    first_snapshot = snapshots[0]
    last_snapshot = snapshots[-1]

    # 3. Читаем данные
    df_first = table.scan(snapshot_id=first_snapshot.snapshot_id).to_pandas()
    df_last = table.scan(snapshot_id=last_snapshot.snapshot_id).to_pandas()

    # 4. Запускаем детектор
    detector = DriftDetector(df_first, df_last)
    return detector.analyze_numeric_drift(column_name, threshold)


def check_table_drift_spark(
        spark,
        table_name: str,
        column_name: str,
        threshold: float = 0.05,
        sample_limit: int = 100000
):
    """
    Проверка дрейфа для реальной таблицы Iceberg через существующую SparkSession.
    Использует сэмплирование (limit), чтобы не перегружать память драйвера (безопасный toPandas).

    :param spark: Существующая SparkSession
    :param table_name: Полное имя таблицы в Spark (например, 'my_catalog.db.my_table')
    :param column_name: Колонка для проверки
    :param threshold: Порог p-value
    :param sample_limit: Максимальное количество строк для выгрузки в Pandas
    """
    # 1. Получаем метаданные о снапшотах через Spark SQL
    snapshots_df = spark.sql(f"SELECT snapshot_id, committed_at FROM {table_name}.snapshots ORDER BY committed_at")
    snapshots = snapshots_df.collect()

    if len(snapshots) < 2:
        raise ValueError("В таблице меньше двух снапшотов. Нечего сравнивать.")

    first_snapshot_id = snapshots[0]['snapshot_id']
    last_snapshot_id = snapshots[-1]['snapshot_id']

    # 2. Читаем данные через time travel (опция snapshot-id)
    df_first = spark.read \
        .format("iceberg") \
        .option("snapshot-id", first_snapshot_id) \
        .load(table_name)

    df_last = spark.read \
        .format("iceberg") \
        .option("snapshot-id", last_snapshot_id) \
        .load(table_name)

    # 3. Безопасная конвертация в Pandas (с сэмплированием)
    print(f"Извлечение до {sample_limit} строк для анализа...")
    pdf_first = df_first.select(column_name).limit(sample_limit).toPandas()
    pdf_last = df_last.select(column_name).limit(sample_limit).toPandas()

    # 4. Запускаем ваш DriftDetector
    detector = DriftDetector(pdf_first, pdf_last)
    return detector.analyze_numeric_drift(column_name, threshold)