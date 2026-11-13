import os
import shutil
import json
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg_drift.drift_detector import DriftDetector


def create_dummy_table():
    """Создает тестовую таблицу с двумя снапшотами и возвращает объект table."""
    warehouse_path = "./warehouse"

    # Безопасное удаление (если папка существует)
    if os.path.exists(warehouse_path):
        try:
            shutil.rmtree(warehouse_path)
        except PermissionError:
            print("Внимание: файл catalog.db занят. Возможно, запущено другое окно.")
            raise

    os.makedirs(warehouse_path, exist_ok=True)

    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/catalog.db",
            "warehouse": f"file://{os.path.abspath(warehouse_path)}",
        }
    )

    catalog.create_namespace("default")
    schema = pa.schema([("id", pa.int32()), ("value", pa.float64())])
    table = catalog.create_table("default.my_table", schema=schema)

    # Снапшот 1
    data1 = pa.table({"id": [1, 2, 3, 4, 5], "value": [10.0, 20.0, 30.0, 40.0, 50.0]}, schema=schema)
    table.append(data1)

    # Снапшот 2
    data2 = pa.table({"id": [6, 7, 8, 9, 10], "value": [100.0, 200.0, 300.0, 400.0, 500.0]}, schema=schema)
    table.append(data2)

    return table


def main():
    print("Шаг 1: Создаем тестовую таблицу Iceberg...")
    table = create_dummy_table()

    print("\nШаг 2: Извлекаем данные из снапшотов...")
    snapshots = table.metadata.snapshots
    snap_current = snapshots[-1]
    snap_previous = snapshots[-2]

    df_prev = table.scan(snapshot_id=snap_previous.snapshot_id).to_pandas()
    df_curr = table.scan(snapshot_id=snap_current.snapshot_id).to_pandas()

    print("\nШаг 3: Анализируем дрейф...")
    detector = DriftDetector(df_prev, df_curr)
    result = detector.analyze_numeric_drift("value", threshold=0.05)

    # Красивый вывод JSON
    print(json.dumps(result, indent=4, ensure_ascii=False))

    if result.get("is_drift"):
        print("\n🚨 Обнаружен статистически значимый дрейф данных!")
    else:
        print("\n✅ Дрейф не обнаружен, данные стабильны.")


if __name__ == "__main__":
    main()