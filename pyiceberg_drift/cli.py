import click
import json
from .__init__ import check_table_drift

@click.command()
@click.option('--table', required=True, help='Имя таблицы (например, my_table)')
@click.option('--col', required=True, help='Имя колонки')
@click.option('--threshold', default=0.05, help='Порог p-value')
@click.option('--warehouse', default='./warehouse', help='Путь к локальному каталогу')
def check_drift(table, col, threshold, warehouse):
    """Утилита для проверки дрейфа данных между первым и последним снапшотом."""

    click.echo(f"Подключение к каталогу: {warehouse}...")

    try:
        # Вызываем функцию из нашей библиотеки, передавая параметры
        result = check_table_drift(
            catalog_name="default",
            table_name=table,
            column_name=col,
            threshold=threshold,
            uri=f"sqlite:///{warehouse}/catalog.db",
            warehouse=f"file://{warehouse}"
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