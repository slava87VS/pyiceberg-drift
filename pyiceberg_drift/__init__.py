from rich.console import Console
from rich.table import Table

from .drift_detector import DriftDetector
from pyiceberg.catalog import load_catalog
import datetime


def list_snapshots(spark, table_name: str):
    """
    Выводит список всех снапшотов таблицы Iceberg в виде красивой таблицы.
    """
    snapshots_df = spark.sql(
        f"SELECT snapshot_id, committed_at, operation FROM {table_name}.snapshots ORDER BY committed_at ASC")
    snapshots = snapshots_df.collect()

    console = Console()
    table = Table(title=f"История снапшотов: {table_name}")

    table.add_column("Snapshot ID", style="cyan", no_wrap=True)
    table.add_column("Дата создания", style="magenta")
    table.add_column("Операция", style="green")

    for row in snapshots:
        dt = row['committed_at']
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S") if hasattr(dt, 'strftime') else str(dt)
        table.add_row(str(row['snapshot_id']), dt_str, row['operation'])

    console.print("\n")
    console.print(table)
    console.print("\n")
    return snapshots


def check_table_drift(
        catalog_name: str,
        table_name: str,
        column_name: str,
        threshold: float = 0.05,
        snapshot_id_from: int = None,
        snapshot_id_to: int = None,
        **catalog_kwargs
):
    """
    Проверка дрейфа с использованием PyIceberg.
    """
    catalog = load_catalog(catalog_name, **catalog_kwargs)
    if "." not in table_name:
        table_name = f"default.{table_name}"

    table = catalog.load_table(table_name)
    snapshots = table.metadata.snapshots

    if len(snapshots) < 2:
        raise ValueError("В таблице меньше двух снапшотов.")

    # Логика выбора снапшотов
    if snapshot_id_from and snapshot_id_to:
        first_snapshot = next((s for s in snapshots if s.snapshot_id == snapshot_id_from), None)
        last_snapshot = next((s for s in snapshots if s.snapshot_id == snapshot_id_to), None)
        if not first_snapshot or not last_snapshot:
            raise ValueError("Указанные ID снапшотов не найдены в таблице.")
    else:
        # По умолчанию: первый и последний
        first_snapshot = snapshots[0]
        last_snapshot = snapshots[-1]

    df_first = table.scan(snapshot_id=first_snapshot.snapshot_id).to_pandas()
    df_last = table.scan(snapshot_id=last_snapshot.snapshot_id).to_pandas()

    detector = DriftDetector(df_first, df_last)
    return detector.analyze_numeric_drift(column_name, threshold)


# ... (начало файла без изменений, импорты) ...

def check_table_drift_spark(
        spark,
        table_name: str,
        column_name: str,
        threshold: float = 0.05,
        sample_limit: int = 100000,
        snapshot_id_from: int = None,
        snapshot_id_to: int = None
):
    """
    Проверка дрейфа для таблицы Iceberg через Spark.
    Автоматически определяет тип колонки (числовая или категориальная).
    """
    snapshots_df = spark.sql(f"SELECT snapshot_id, committed_at FROM {table_name}.snapshots ORDER BY committed_at ASC")
    snapshots = snapshots_df.collect()

    if len(snapshots) < 2:
        raise ValueError("В таблице меньше двух снапшотов.")

    if snapshot_id_from is not None and snapshot_id_to is not None:
        first_snapshot_id = int(snapshot_id_from)
        last_snapshot_id = int(snapshot_id_to)
        existing_ids = [row['snapshot_id'] for row in snapshots]
        if first_snapshot_id not in existing_ids or last_snapshot_id not in existing_ids:
            raise ValueError(f"Указанные ID снапшотов не найдены.")
    else:
        first_snapshot_id = snapshots[0]['snapshot_id']
        last_snapshot_id = snapshots[-1]['snapshot_id']

    df_first = spark.read.format("iceberg").option("snapshot-id", first_snapshot_id).load(table_name)
    df_last = spark.read.format("iceberg").option("snapshot-id", last_snapshot_id).load(table_name)

    # --- НОВАЯ ЛОГИКА: Определение типа колонки ---
    # Получаем тип данных из схемы Spark
    col_type = dict(df_first.dtypes)[column_name]
    is_numeric = col_type in ('int', 'bigint', 'double', 'float', 'decimal')

    print(f"Извлечение до {sample_limit} строк для анализа (Тип колонки: {col_type})...")
    pdf_first = df_first.select(column_name).limit(sample_limit).toPandas()
    pdf_last = df_last.select(column_name).limit(sample_limit).toPandas()

    detector = DriftDetector(pdf_first, pdf_last)

    # Выбираем метод в зависимости от типа
    if is_numeric:
        return detector.analyze_numeric_drift(column_name, threshold)
    else:
        return detector.analyze_categorical_drift(column_name, threshold)


def generate_html_report(result: dict, filename: str = "drift_report.html"):
    """
    Генерирует HTML-отчет (поддерживает и числа, и строки).
    """
    # Если передан список (результаты по всем колонкам)
    if isinstance(result, list):
        rows = ""
        for r in result:
            status_color = "#f8d7da" if r.get('is_drift') else "#d4edda"
            status_text = "🚨 Дрейф" if r.get('is_drift') else "✅ Стабильно"

            # Для чисел показываем среднее, для строк — просто статистику
            extra = f"Среднее: {r.get('mean_prev', 0):.2f} -> {r.get('mean_curr', 0):.2f}" if r.get(
                'type') == 'numeric' else ""

            rows += f"""
            <tr>
                <td><strong>{r['column']}</strong></td>
                <td>{r['type']}</td>
                <td>{r['p_value']:.4f}</td>
                <td>{extra}</td>
                <td style="background-color: {status_color}; font-weight: bold;">{status_text}</td>
            </tr>
            """

        html_content = f"""
        <!DOCTYPE html>
        <html lang="ru">
        <head>
            <meta charset="UTF-8">
            <title>Отчет о дрейфе данных</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f4f4f9; }}
                table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #f8f9fa; }}
                h1 {{ color: #333; }}
            </style>
        </head>
        <body>
            <h1>Сводный отчет о дрейфе данных</h1>
            <table>
                <thead>
                    <tr>
                        <th>Колонка</th>
                        <th>Тип</th>
                        <th>p-value</th>
                        <th>Детали</th>
                        <th>Статус</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        </body>
        </html>
        """
    else:
        # Одиночный результат (как было раньше)
        html_content = f"""
        <!DOCTYPE html>
        <html lang="ru">
        <head>
            <meta charset="UTF-8">
            <title>Отчет о дрейфе</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f4f4f9; }}
                .card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); max-width: 600px; }}
                .alert {{ padding: 15px; border-radius: 5px; font-weight: bold; text-align: center; margin-top: 20px; }}
                .alert-danger {{ background-color: #f8d7da; color: #721c24; }}
                .alert-success {{ background-color: #d4edda; color: #155724; }}
            </style>
        </head>
        <body>
            <div class="card">
                <h1>Отчет о дрейфе данных</h1>
                <p><strong>Колонка:</strong> {result.get('column')}</p>
                <p><strong>Тип:</strong> {result.get('type')}</p>
                <p><strong>p-value:</strong> {result.get('p_value', 0):.4f}</p>
                <div class="alert {'alert-danger' if result.get('is_drift') else 'alert-success'}">
                    {'🚨 ОБНАРУЖЕН ДРЕЙФ!' if result.get('is_drift') else '✅ Дрейф не обнаружен.'}
                </div>
            </div>
        </body>
        </html>
        """

    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"\nHTML-отчет сохранен: {filename}")


def analyze_table_drift(
        spark,
        table_name: str,
        threshold: float = 0.05,
        sample_limit: int = 100000,
        snapshot_id_from: int = None,
        snapshot_id_to: int = None
):
    """
    Анализирует дрейф по всем колонкам таблицы.
    Возвращает список результатов.
    """
    # Получаем схему таблицы
    df_sample = spark.read.format("iceberg").load(table_name).limit(1)
    columns = df_sample.columns

    results = []
    print(f"\n--- Анализ дрейфа по {len(columns)} колонкам ---")

    for col in columns:
        try:
            # Вызываем нашу функцию для каждой колонки
            res = check_table_drift_spark(
                spark=spark,
                table_name=table_name,
                column_name=col,
                threshold=threshold,
                sample_limit=sample_limit,
                snapshot_id_from=snapshot_id_from,
                snapshot_id_to=snapshot_id_to
            )
            # Добавляем в результат
            if "error" not in res:
                results.append(res)
                status = "🚨 Дрейф" if res['is_drift'] else "✅ Стабильно"
                print(f"  {col} ({res['type']}): {status} (p-value: {res['p_value']:.4f})")
        except Exception as e:
            print(f"  {col}: Ошибка - {e}")

    return results


def check_null_drift_zero_scan(
        spark,
        table_name: str,
        column_name: str,
        threshold: float = 0.05,
        snapshot_id_from: int = None,
        snapshot_id_to: int = None
):
    """
    Zero-Scan проверка дрейфа null_count между двумя снапшотами.
    Не читает данные, только метаданные файлов (манифесты).
    """
    # 1. Получаем список снапшотов
    snapshots_df = spark.sql(f"SELECT snapshot_id, committed_at FROM {table_name}.snapshots ORDER BY committed_at ASC")
    snapshots = snapshots_df.collect()

    if len(snapshots) < 2:
        raise ValueError("В таблице меньше двух снапшотов.")

    # Выбор снапшотов
    if snapshot_id_from is not None and snapshot_id_to is not None:
        first_id = int(snapshot_id_from)
        last_id = int(snapshot_id_to)
    else:
        first_id = snapshots[0]['snapshot_id']
        last_id = snapshots[-1]['snapshot_id']

    # 2. Функция для получения суммарного null_count для колонки в снапшоте
    def get_null_count(snapshot_id):
        # Читаем .files для нужного снапшота
        files_df = spark.read.format("iceberg") \
            .option("snapshot-id", snapshot_id) \
            .load(f"{table_name}.files")

        files_df.createOrReplaceTempView("current_files")

        # Определяем числовой ID колонки (порядковый номер + 1)
        table_schema = spark.table(table_name).schema
        field_id = None
        for idx, field in enumerate(table_schema.fields):
            if field.name == column_name:
                # Пытаемся взять из метаданных, иначе используем индекс+1
                field_id = field.metadata.get("iceberg.field.id", idx + 1)
                break

        if field_id is None:
            raise ValueError(f"Колонка {column_name} не найдена")

        # Суммируем null_value_counts по всем файлам
        # Используем числовой ID как ключ
        null_count_df = spark.sql(f"""
            SELECT SUM(null_value_counts[{field_id}]) as total_nulls
            FROM current_files
            WHERE null_value_counts[{field_id}] IS NOT NULL
        """)

        result = null_count_df.collect()[0]['total_nulls']
        return result if result is not None else 0

    # 3. Получаем null_count для обоих снапшотов
    nulls_first = get_null_count(first_id)
    nulls_last = get_null_count(last_id)

    # 4. Простая логика дрейфа: если количество null выросло более чем на threshold (в процентах)
    # или изменилось существенно, считаем это дрейфом
    if nulls_first == 0 and nulls_last > 0:
        is_drift = True
        change_pct = float('inf')
    elif nulls_first > 0:
        change_pct = abs(nulls_last - nulls_first) / nulls_first
        is_drift = change_pct > threshold
    else:
        change_pct = 0.0
        is_drift = False

    return {
        "column": column_name,
        "type": "null_count_zero_scan",
        "nulls_prev": int(nulls_first),
        "nulls_curr": int(nulls_last),
        "change_pct": float(change_pct) if change_pct != float('inf') else "inf",
        "is_drift": is_drift,
        "p_value": None,  # Для zero-scan нет p-value, есть только изменение
        "stat": None
    }