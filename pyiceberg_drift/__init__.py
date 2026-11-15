import datetime
from rich.console import Console
from rich.table import Table
from .drift_detector import DriftDetector
from pyiceberg.catalog import load_catalog


# ==========================================
# БЛОК 1: ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И СТАТИСТИКА
# ==========================================

def list_snapshots(spark, table_name: str):
    """
    Выводит историю снапшотов в виде красивой таблицы.
    """
    console = Console()
    snapshots_df = spark.sql(
        f"SELECT snapshot_id, committed_at, operation, summary FROM {table_name}.snapshots ORDER BY committed_at ASC")
    snapshots = snapshots_df.collect()

    table = Table(title=f"📸 История снапшотов: {table_name}")
    table.add_column("Snapshot ID", style="cyan", no_wrap=True)
    table.add_column("Дата", style="magenta")
    table.add_column("Операция", style="green")
    table.add_column("Добавлено строк", style="yellow")

    for row in snapshots:
        dt = row['committed_at']
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S") if hasattr(dt, 'strftime') else str(dt)

        # Достаем количество добавленных строк из summary (если есть)
        added_rows = row['summary'].get('added-records', '—')

        table.add_row(str(row['snapshot_id']), dt_str, row['operation'], str(added_rows))

    console.print("\n")
    console.print(table)
    console.print("\n")
    return snapshots

def get_table_statistics(spark, table_name: str):
    """
    Расширенная статистика: файлы, размер, строки, разбивка по размерам.
    """
    try:
        stats = spark.sql(f"""
            SELECT 
                COUNT(*) as total_files,
                SUM(file_size_in_bytes) as total_bytes,
                SUM(record_count) as total_rows,
                SUM(CASE WHEN file_size_in_bytes < 1048576 THEN 1 ELSE 0 END) as files_under_1mb,
                SUM(CASE WHEN file_size_in_bytes >= 1048576 AND file_size_in_bytes < 134217728 THEN 1 ELSE 0 END) as files_1_to_128mb,
                SUM(CASE WHEN file_size_in_bytes >= 134217728 THEN 1 ELSE 0 END) as files_over_128mb
            FROM {table_name}.files
        """).collect()[0]

        total_bytes = stats['total_bytes'] if stats['total_bytes'] else 0
        size_str = f"{round(total_bytes / (1024 * 1024), 2)} MB" if total_bytes > 1024 * 1024 else f"{round(total_bytes / 1024, 2)} KB"

        return {
            "total_files": stats['total_files'],
            "size_str": size_str,
            "total_rows": stats['total_rows'] if stats['total_rows'] else 0,
            "file_distribution": {
                "under_1mb": stats['files_under_1mb'] or 0,
                "1_to_128mb": stats['files_1_to_128mb'] or 0,
                "over_128mb": stats['files_over_128mb'] or 0
            }
        }
    except Exception as e:
        return {"total_files": 0, "size_str": "0 KB", "total_rows": 0, "file_distribution": {}, "error": str(e)}


def get_column_null_stats(spark, table_name: str, column_name: str):
    """
    Достает статистику по NULL для конкретной колонки из метаданных.
    """
    try:
        table_schema = spark.table(table_name).schema
        # Находим ID колонки (порядковый номер)
        field_id = next((idx + 1 for idx, field in enumerate(table_schema.fields) if field.name == column_name), None)

        if field_id is None:
            return None

        null_stats = spark.sql(f"""
            SELECT SUM(null_value_counts[{field_id}]) as total_nulls
            FROM {table_name}.files
        """).collect()[0]

        return null_stats['total_nulls'] if null_stats['total_nulls'] else 0
    except Exception:
        return 0


# ==========================================
# БЛОК 2: АУДИТ ЗДОРОВЬЯ ТАБЛИЦЫ
# ==========================================

def audit_table_health(spark, table_name: str):
    """
    Полный аудит метаданных Iceberg: файлы, снапшоты, манифесты, партиции.
    Возвращает словарь с результатами и списком проблем.
    """
    console = Console()
    console.print(f"\n[bold cyan]🏥 Аудит здоровья таблицы: {table_name}[/bold cyan]\n")

    issues = []
    summary_data = {}

    # 1. Аудит файлов (Small Files)
    try:
        file_stats = spark.sql(f"""
            SELECT 
                COUNT(*) as total_files,
                SUM(CASE WHEN file_size_in_bytes < 134217728 THEN 1 ELSE 0 END) as small_files,
                ROUND(AVG(file_size_in_bytes) / 1024 / 1024, 2) as avg_size_mb
            FROM {table_name}.files
        """).collect()[0]

        total = file_stats['total_files']
        small = file_stats['small_files'] or 0
        avg_size = file_stats['avg_size_mb'] or 0

        summary_data['files'] = {'total': total, 'small': small, 'avg_mb': avg_size}

        table_files = Table(title="📁 Файлы данных")
        table_files.add_column("Метрика", style="cyan")
        table_files.add_column("Значение", style="magenta")
        table_files.add_row("Всего файлов", str(total))
        table_files.add_row("Мелких (<128MB)", str(small))
        table_files.add_row("Средний размер (MB)", str(avg_size))
        console.print(table_files)

        if total > 0 and (small / total) > 0.5:
            issues.append("⚠️ Много мелких файлов. Рекомендуется `rewrite_data_files`.")
    except Exception as e:
        console.print(f"[red]Ошибка аудита файлов: {e}[/red]")

    # 2. Аудит снапшотов
    try:
        snap_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}.snapshots").collect()[0]['cnt']
        summary_data['snapshots'] = snap_count

        table_snaps = Table(title="📸 Снапшоты")
        table_snaps.add_column("Метрика", style="cyan")
        table_snaps.add_column("Значение", style="magenta")
        table_snaps.add_row("Всего снапшотов", str(snap_count))
        console.print(table_snaps)

        if snap_count > 100:
            issues.append("⚠️ Слишком много снапшотов. Рекомендуется `expire_snapshots`.")
    except Exception as e:
        console.print(f"[red]Ошибка аудита снапшотов: {e}[/red]")

    # 3. Аудит манифестов
    try:
        manifest_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}.manifests").collect()[0]['cnt']
        summary_data['manifests'] = manifest_count

        table_manifests = Table(title="📜 Манифесты")
        table_manifests.add_column("Метрика", style="cyan")
        table_manifests.add_column("Значение", style="magenta")
        table_manifests.add_row("Всего манифестов", str(manifest_count))
        console.print(table_manifests)

        if manifest_count > 50:
            issues.append("⚠️ Слишком много манифестов. Рекомендуется `rewrite_manifests`.")
    except Exception as e:
        console.print(f"[red]Ошибка аудита манифестов: {e}[/red]")

    # Итоговый вердикт
    console.print("\n[bold]Вердикт:[/bold]")
    if issues:
        for issue in issues:
            console.print(f"[yellow]{issue}[/yellow]")
    else:
        console.print("[green]✅ Таблица здорова. Проблем с метаданными не обнаружено.[/green]")

    return {"issues": issues, "summary": summary_data}


# ==========================================
# БЛОК 3: ГЕНЕРАЦИЯ HTML-ОТЧЕТА
# ==========================================
def generate_full_report(table_name, stats: dict, snapshots: list, audit_result: dict, null_stats: dict = None,
                         filename: str = "report.html"):
    """
    Генерирует детальный HTML-отчет со всей статистикой, историей и аудитом.
    """
    # 1. Блок карточек с общей статистикой
    cards_html = f"""
    <div class="summary-cards">
        <div class="card">
            <div class="card-value">{stats.get('total_files', 0)}</div>
            <div class="card-label">Файлов</div>
        </div>
        <div class="card">
            <div class="card-value">{stats.get('size_str', '0')}</div>
            <div class="card-label">Размер</div>
        </div>
        <div class="card">
            <div class="card-value">{stats.get('total_rows', 0):,}</div>
            <div class="card-label">Строк</div>
        </div>
        <div class="card">
            <div class="card-value">{audit_result['summary'].get('snapshots', 0)}</div>
            <div class="card-label">Снапшотов</div>
        </div>
    </div>
    """

    # 2. Блок проблем
    if audit_result['issues']:
        issues_html = "<ul class='issues-list'>"
        for issue in audit_result['issues']:
            issues_html += f"<li class='issue-item'>{issue}</li>"
        issues_html += "</ul>"
    else:
        issues_html = "<p class='success'>✅ Таблица здорова. Проблем не обнаружено.</p>"

    # 3. Детализация по файлам (распределение размеров)
    dist = stats.get('file_distribution', {})
    files_dist_html = f"""
    <table>
        <thead><tr><th>Категория размера</th><th>Количество файлов</th><th>Статус</th></tr></thead>
        <tbody>
            <tr><td>Меньше 1 МБ</td><td>{dist.get('under_1mb', 0)}</td><td>{'⚠️ Много мелких' if dist.get('under_1mb', 0) > 10 else '✅ ОК'}</td></tr>
            <tr><td>1 МБ - 128 МБ</td><td>{dist.get('1_to_128mb', 0)}</td><td>✅ ОК</td></tr>
            <tr><td>Больше 128 МБ</td><td>{dist.get('over_128mb', 0)}</td><td>✅ Оптимально</td></tr>
        </tbody>
    </table>
    """

    # 4. История снапшотов
    snaps_rows = ""
    for s in snapshots:
        dt_str = s['committed_at'].strftime("%Y-%m-%d %H:%M:%S") if hasattr(s['committed_at'], 'strftime') else str(
            s['committed_at'])
        added = s['summary'].get('added-records', '—')
        snaps_rows += f"""
        <tr>
            <td style="font-family: monospace;">{s['snapshot_id']}</td>
            <td>{dt_str}</td>
            <td>{s['operation']}</td>
            <td>{added}</td>
        </tr>
        """

    # 5. Статистика по колонкам
    cols_html = ""
    if null_stats:
        cols_rows = ""
        for col, null_count in null_stats.items():
            cols_rows += f"<tr><td>{col}</td><td>{null_count}</td></tr>"
        cols_html = f"""
        <h2>📊 Статистика по колонкам (NULL)</h2>
        <table>
            <thead><tr><th>Колонка</th><th>Количество NULL</th></tr></thead>
            <tbody>{cols_rows}</tbody>
        </table>
        """

    html_content = f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <title>Iceberg Table Report</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; background-color: #f0f2f5; color: #333; }}
            h1 {{ color: #1a1a2e; margin-bottom: 5px; }}
            h2 {{ color: #16213e; margin-top: 40px; border-bottom: 2px solid #ddd; padding-bottom: 10px; }}
            .subtitle {{ color: #666; margin-bottom: 30px; }}
            .summary-cards {{ display: flex; gap: 20px; margin-bottom: 30px; }}
            .card {{ background: white; padding: 20px 30px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); text-align: center; flex: 1; max-width: 200px; }}
            .card-value {{ font-size: 28px; font-weight: bold; color: #16213e; }}
            .card-label {{ font-size: 13px; color: #888; margin-top: 5px; text-transform: uppercase; letter-spacing: 0.5px; }}
            .issues-list {{ list-style: none; padding: 0; }}
            .issue-item {{ background: #fff3cd; color: #856404; padding: 15px; border-radius: 8px; margin-bottom: 10px; border-left: 5px solid #ffeeba; }}
            .success {{ background: #d4edda; color: #155724; padding: 15px; border-radius: 8px; border-left: 5px solid #c3e6cb; }}
            table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin-top: 15px; }}
            th {{ background-color: #16213e; color: white; padding: 14px; text-align: left; font-weight: 500; }}
            td {{ padding: 14px; border-bottom: 1px solid #eee; }}
            tr:hover {{ background-color: #f8f9fa; }}
        </style>
    </head>
    <body>
        <h1>🧊 Iceberg Table Report</h1>
        <div class="subtitle">Таблица: <strong>{table_name}</strong></div>

        {cards_html}

        <h2>🏥 Аудит метаданных</h2>
        {issues_html}

        <h2>📁 Распределение файлов по размеру</h2>
        {files_dist_html}

        {cols_html}

        <h2>📸 История снапшотов</h2>
        <table>
            <thead>
                <tr>
                    <th>Snapshot ID</th>
                    <th>Дата</th>
                    <th>Операция</th>
                    <th>Добавлено строк</th>
                </tr>
            </thead>
            <tbody>
                {snaps_rows}
            </tbody>
        </table>
    </body>
    </html>
    """

    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"\n📄 Полный отчет сохранен в файл: {filename}")