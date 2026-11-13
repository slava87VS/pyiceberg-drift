from pyiceberg_drift import check_table_drift


def my_data_pipeline():
    print("--- Запуск пайплайна ---")

    try:
        # Вызываем функцию из нашей библиотеки
        result = check_table_drift(
            catalog_name="default",
            table_name="my_table",  # Передаем без неймспейса, библиотека сама добавит "default."
            column_name="value",
            threshold=0.05,
            uri="sqlite:///./warehouse/catalog.db",
            warehouse="file://./warehouse"
        )

        print(f"Результат KS-теста: p-value = {result['p_value']:.4f}")

        if result["is_drift"]:
            print("🚨 Алерт! Данные изменились, останавливаем пайплайн.")
        else:
            print("✅ Всё ок, данные стабильны.")

    except Exception as e:
        print(f"❌ Ошибка выполнения: {e}")


if __name__ == "__main__":
    my_data_pipeline()