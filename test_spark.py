from pyspark.sql import SparkSession
from pyiceberg_drift import check_table_drift_spark
import os
import shutil

import os
# Укажите ваш точный путь к Java 17
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot"

from pyspark.sql import SparkSession
from pyiceberg_drift import check_table_drift_spark
import shutil
# ... остальной код

import os
os.environ.pop("SPARK_HOME", None)  # Игнорировать системный SPARK_HOME

def run_spark_test():
    print("--- Инициализация Spark ---")
    # Очищаем папку от прошлых запусков
    warehouse_path = "./spark_warehouse"
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)

    # Создаем Spark сессию с локальным Iceberg каталогом (Hadoop)
    spark = SparkSession.builder \
        .appName("DriftTest") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", os.path.abspath(warehouse_path)) \
        .getOrCreate()

    # Отключаем лишние логи, чтобы видеть только результат
    spark.sparkContext.setLogLevel("ERROR")

    print("--- Создание тестовой таблицы Iceberg ---")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql("DROP TABLE IF EXISTS local.db.test_table")

    # Создаем таблицу
    spark.sql("""
        CREATE TABLE local.db.test_table (
            id INT, 
            value DOUBLE
        ) USING iceberg
    """)

    # Снапшот 1: значения 10-50
    spark.sql("""
        INSERT INTO local.db.test_table VALUES 
        (1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0)
    """)

    # Снапшот 2: значения 100-500 (имитируем дрейф)
    spark.sql("""
        INSERT OVERWRITE local.db.test_table VALUES 
        (6, 100.0), (7, 200.0), (8, 300.0), (9, 400.0), (10, 500.0)
    """)

    print("--- Запуск функции check_table_drift_spark ---")

    # ВЫЗЫВАЕМ НАШУ БИБЛИОТЕКУ
    try:
        result = check_table_drift_spark(
            spark=spark,
            table_name="local.db.test_table",
            column_name="value",
            threshold=0.05,
            sample_limit=1000
        )

        print("\nРезультат работы библиотеки:")
        print(f"p-value: {result['p_value']:.4f}")
        print(f"Обнаружен дрейф: {result['is_drift']}")

        if result['is_drift']:
            print("✅ ТЕСТ ПРОЙДЕН: Библиотека успешно нашла дрейф в Spark!")
        else:
            print("❌ ТЕСТ НЕ ПРОЙДЕН: Библиотека не увидела изменений.")

    except Exception as e:
        print(f"Ошибка при вызове функции: {e}")

    spark.stop()


if __name__ == "__main__":
    run_spark_test()