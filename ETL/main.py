from __future__ import annotations

import logging
from datetime import datetime

from db_layer import get_connection, upsert_products
from scraper import fetch_products


def setup_logging() -> None:
    """
    Простая настройка логирования на stdout.

    При желании сюда можно прикрутить лог-файл или JSON-формат,
    но для тестового достаточно консоли.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main() -> None:
    """
    Точка входа: запускает весь ETL-процесс.

    Шаги:
      1. Собираем данные с витрины (EXTRACT + TRANSFORM).
      2. Подключаемся к SQLite и создаём таблицу при необходимости.
      3. Загружаем данные в таблицу, обрабатывая дубликаты (LOAD).
      4. Логируем старт/финиш, кол-во собранных и загруженных записей.
    """
    setup_logging()
    logger = logging.getLogger("etl")

    start_ts = datetime.now()
    logger.info("Старт ETL-пайплайна.")

    query = "кроссовки мужские"
    limit = 1000

    try:
        # EXTRACT + TRANSFORM
        logger.info("Шаг 1: парсинг витрины для запроса %r (limit=%d).", query, limit)
        products = fetch_products(query, limit=limit)
        logger.info("Шаг 1 завершён: собрано %d валидных записей.", len(products))

        # Если совсем пусто — всё равно продолжаем, чтобы лог был полным
        # (но можно и сделать ранний return).
        # LOAD
        logger.info("Шаг 2: подключение к базе и загрузка данных.")
        conn = get_connection()

        try:
            inserted = upsert_products(conn, products)
            logger.info(
                "Шаг 2 завершён: в базу добавлено %d новых записей (всего собрано: %d).",
                inserted,
                len(products),
            )
        finally:
            conn.close()
            logger.info("Соединение с базой закрыто.")

    except Exception as exc:
        # Любая неожиданная ошибка считается критической.
        logger.exception("Критическая ошибка ETL-процесса: %s", exc)

    finally:
        end_ts = datetime.now()
        duration = (end_ts - start_ts).total_seconds()
        logger.info(
            "ETL завершён. Начало: %s, конец: %s, длительность: %.1f сек.",
            start_ts.strftime("%d-%m-%Y %H:%M:%S"),
            end_ts.strftime("%d-%m-%Y %H:%M:%S"),
            duration,
        )


if __name__ == "__main__":
    main()
