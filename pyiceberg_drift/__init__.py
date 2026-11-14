from rich.console import Console
from rich.table import Table

from .drift_detector import DriftDetector
from pyiceberg.catalog import load_catalog
import datetime

def get_snapshot_summary(spark, table_name: str):
    """Собирает базовую статистику по последнему снапшоту."""
    # Убрали MIN(committed_at), так как в .files этого поля нет
    summary = spark.sql(f"""
        SELECT 
            COUNT(*) as file_count,
            SUM(file_size_in_bytes) as total_bytes,
            SUM(record_count) as total_rows
        FROM {table_name}.files
    """).collect()[0]

    # Если размер меньше 1 МБ, показываем в КБ
    total_bytes = summary['total_bytes'] if summary['total_bytes'] else 0
    if total_bytes > 1024 * 1024:
        size_str = f"{round(total_bytes / (1024 * 1024), 2)} MB"
    else:
        size_str = f"{round(total_bytes / 1024, 2)} KB"

    return {
        "file_count": summary['file_count'],
        "size_str": size_str,
        "total_rows": summary['total_rows'] if summary['total_rows'] else 0
    }

def generate_dashboard(results: list, snapshot_summary: dict = None, filename: str = "drift_dashboard.html"):
    rows = ""
    for r in results:
        status_color = "#f8d7da" if r.get('is_drift') else "#d4edda"
        status_text = "🚨 Дрейф" if r.get('is_drift') else "✅ Стабильно"

        # Формируем детали
        details = ""
        if r.get('type') == 'numeric':
            details = f"Среднее: {r.get('mean_prev', 0):.2f} -> {r.get('mean_curr', 0):.2f}"
        elif r.get('type') == 'categorical':
            details = "Распределение изменилось" if r.get('is_drift') else "Без изменений"
        elif r.get('type') == 'null_count_zero_scan':
            details = f"NULL: {r.get('nulls_prev')} -> {r.get('nulls_curr')}"
        elif r.get('type') == 'bounds_zero_scan':
            details = f"Min: {r.get('min_prev')} -> {r.get('min_curr')}, Max: {r.get('max_prev')} -> {r.get('max_curr')}"

        p_val = f"{r.get('p_value', 0):.4f}" if r.get('p_value') is not None else "—"

        rows += f"""
        <tr>
            <td><strong>{r['column']}</strong></td>
            <td>{r['type']}</td>
            <td>{p_val}</td>
            <td>{details}</td>
            <td style="background-color: {status_color}; font-weight: bold; text-align: center;">{status_text}</td>
        </tr>
        """

    summary_html = ""
    if snapshot_summary:
        summary_html = f"""
        <div class="summary-cards">
            <div class="card">
                <div class="card-value">{snapshot_summary['file_count']}</div>
                <div class="card-label">Файлов</div>
            </div>
            <div class="card">
                <div class="card-value">{snapshot_summary['size_str']}</div>
                <div class="card-label">Общий размер</div>
            </div>
            <div class="card">
                <div class="card-value">{snapshot_summary['total_rows']:,}</div>
                <div class="card-label">Строк</div>
            </div>
        </div>
        """

    html_content = f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <title>Drift Dashboard</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; background-color: #f0f2f5; }}
            h1 {{ color: #1a1a2e; margin-bottom: 10px; }}
            .subtitle {{ color: #666; margin-bottom: 30px; }}
            .summary-cards {{ display: flex; gap: 20px; margin-bottom: 30px; }}
            .card {{ background: white; padding: 20px 30px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); text-align: center; flex: 1; max-width: 200px; }}
            .card-value {{ font-size: 28px; font-weight: bold; color: #16213e; }}
            .card-label {{ font-size: 13px; color: #888; margin-top: 5px; text-transform: uppercase; letter-spacing: 0.5px; }}
            table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
            th {{ background-color: #16213e; color: white; padding: 14px; text-align: left; font-weight: 500; }}
            td {{ padding: 14px; border-bottom: 1px solid #eee; vertical-align: top; }}
            tr:last-child td {{ border-bottom: none; }}
            tr:hover {{ background-color: #f8f9fa; }}
        </style>
    </head>
    <body>
        <h1>🔍 Drift Detection Dashboard</h1>
        <div class="subtitle">Результаты zero-scan и сканирующих проверок</div>

        {summary_html}

        <table>
            <thead>
                <tr>
                    <th>Колонка</th>
                    <th>Тип проверки</th>
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

    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"\nДашборд сохранен: {filename}")

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


def check_bounds_drift_zero_scan(spark, table_name: str, column_name: str, threshold_pct: float = 0.5, **kwargs):
    snapshots_df = spark.sql(f"SELECT snapshot_id FROM {table_name}.snapshots ORDER BY committed_at ASC")
    snapshots = snapshots_df.collect()
    first_id, last_id = snapshots[0]['snapshot_id'], snapshots[-1]['snapshot_id']

    def get_bounds(snapshot_id):
        files_df = spark.read.format("iceberg").option("snapshot-id", snapshot_id).load(f"{table_name}.files")
        files_df.createOrReplaceTempView("current_files_bounds")

        # Получаем ID колонки
        table_schema = spark.table(table_name).schema
        field_id = next((idx + 1 for idx, field in enumerate(table_schema.fields) if field.name == column_name), None)

        if field_id is None:
            raise ValueError(f"Колонка {column_name} не найдена")

        # Берем HEX-представление границ
        res = spark.sql(f"""
            SELECT 
                MIN(HEX(lower_bounds[{field_id}])) as min_hex,
                MAX(HEX(upper_bounds[{field_id}])) as max_hex
            FROM current_files_bounds
        """).collect()[0]

        return res['min_hex'], res['max_hex']

    min_prev, max_prev = get_bounds(first_id)
    min_curr, max_curr = get_bounds(last_id)

    is_drift = (min_prev != min_curr) or (max_prev != max_curr)

    return {
        "column": column_name,
        "type": "bounds_zero_scan",
        "min_prev": min_prev, "min_curr": min_curr,
        "max_prev": max_prev, "max_curr": max_curr,
        "is_drift": is_drift,
        "details": "Границы изменились (HEX)" if is_drift else "Без изменений"
    }