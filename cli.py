import click
import json
from pyiceberg.catalog.sql import SqlCatalog
from drift_detector import DriftDetector


# Это декоратор, который превращает функцию в CLI-команду
@click.command()
@click.option('--table', required=True, help='Имя таблицы (например, default.my_table)')
@click.option('--col', required=True, help='Имя колонки для проверки (например, value)')
@click.option('--threshold', default=0.05, help='Порог p-value для обнаружения дрейфа (по умолчанию 0.05)')
@click.option('--warehouse', default='./warehouse', help='Путь к локальному каталогу')
def check_drift(table, col, threshold, warehouse):
    """Утилита для проверки дрейфа данных между двумя последними снапшотами Iceberg."""

    click.echo(f"Подключение к каталогу: {warehouse}...")

    # Настраиваем каталог
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse}/catalog.db",
            "warehouse": f"file://{warehouse}",
        }
    )

    try:
        # Загружаем таблицу
        iceberg_table = catalog.load_table(table)
    except Exception as e:
        click.echo(f"❌ Ошибка: Не удалось загрузить таблицу {table}. {e}", err=True)
        return

    snapshots = iceberg_table.metadata.snapshots
    if len(snapshots) < 2:
        click.echo("❌ Ошибка: В таблице меньше двух снапшотов. Нечего сравнивать.", err=True)
        return

    click.echo("Чтение снапшотов...")
    snap_current = snapshots[-1]
    snap_previous = snapshots[-2]

    df_prev = iceberg_table.scan(snapshot_id=snap_previous.snapshot_id).to_pandas()
    df_curr = iceberg_table.scan(snapshot_id=snap_current.snapshot_id).to_pandas()

    click.echo(f"Анализ колонки '{col}' (порог: {threshold})...\n")

    detector = DriftDetector(df_prev, df_curr)
    result = detector.analyze_numeric_drift(col, threshold=threshold)

    if "error" in result:
        click.echo(f"❌ {result['error']}", err=True)
        return

    # Выводим красивый JSON
    click.echo(json.dumps(result, indent=4, ensure_ascii=False))

    if result.get("is_drift"):
        click.echo("\n🚨 Обнаружен статистически значимый дрейф данных!")
    else:
        click.echo("\n✅ Дрейф не обнаружен, данные стабильны.")


if __name__ == '__main__':
    check_drift()