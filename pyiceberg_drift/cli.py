import click
import json
from pyiceberg_drift import check_table_drift, list_snapshots


@click.command()
@click.option('--table', required=True, help='Имя таблицы')
@click.option('--col', help='Имя колонки для анализа (не нужно, если используется --list)')
@click.option('--threshold', default=0.05, help='Порог p-value')
@click.option('--warehouse', default='./warehouse', help='Путь к локальному каталогу')
@click.option('--list', 'list_snaps', is_flag=True, help='Только вывести список снапшотов')
@click.option('--from-id', type=int, help='ID первого снапшота для сравнения')
@click.option('--to-id', type=int, help='ID второго снапшота для сравнения')
def check_drift(table, col, threshold, warehouse, list_snaps, from_id, to_id):
    """Утилита для проверки дрейфа данных в Iceberg."""

    # В CLI мы используем PyIceberg (локально)
    catalog_kwargs = {
        "uri": f"sqlite:///{warehouse}/catalog.db",
        "warehouse": f"file://{warehouse}"
    }

    if list_snaps:
        # Для вывода списка нам нужен Spark, либо можно реализовать через PyIceberg
        # Для простоты в CLI мы выводим через PyIceberg
        from pyiceberg.catalog import load_catalog
        catalog = load_catalog("default", **catalog_kwargs)
        iceberg_table = catalog.load_table(table if "." in table else f"default.{table}")

        snapshots = iceberg_table.metadata.snapshots
        click.echo(f"\nИстория снапшотов для {table}:")
        click.echo(f"{'Snapshot ID':<22} | {'Дата создания'}")
        click.echo("-" * 50)
        for snap in snapshots:
            click.echo(f"{snap.snapshot_id:<22} | {snap.timestamp_ms}")
        return

    if not col:
        click.echo("❌ Ошибка: Параметр --col обязателен, если не используется --list", err=True)
        return

    try:
        result = check_table_drift(
            catalog_name="default",
            table_name=table,
            column_name=col,
            threshold=threshold,
            snapshot_id_from=from_id,
            snapshot_id_to=to_id,
            **catalog_kwargs
        )

        click.echo(json.dumps(result, indent=4, ensure_ascii=False))

        if result.get("is_drift"):
            click.echo("\n🚨 Обнаружен статистически значимый дрейф данных!")
        else:
            click.echo("\n✅ Дрейф не обнаружен, данные стабильны.")

    except Exception as e:
        click.echo(f"❌ Ошибка: {e}", err=True)


if __name__ == '__main__':
    check_drift()