from __future__ import annotations

import logging
import sqlite3
from typing import Iterable

from scraper import Product

logger = logging.getLogger(__name__)

DB_PATH = "products.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS products (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL,
    price         REAL    NOT NULL,
    url           TEXT    NOT NULL,
    rating        REAL,
    reviews_count INTEGER,
    scraped_at    TEXT    NOT NULL,  -- DD-MM-YYYY hh:mm:ss
    UNIQUE (url, scraped_at)
);
"""


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """
    Открывает соединение с SQLite и гарантирует наличие таблицы products.

    Если файл базы отсутствует — SQLite создаст его автоматически.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute(SCHEMA_SQL)
    conn.commit()
    logger.info("Подключились к базе %s и убедились, что таблица products существует.", db_path)
    return conn


def upsert_products(conn: sqlite3.Connection, products: Iterable[Product]) -> int:
    """
    Сохраняет список товаров в таблицу products.

    Использует INSERT OR IGNORE + UNIQUE(url, scraped_at),
    чтобы не вставлять дубликаты с одинаковым timestamp.
    Возвращает количество реально добавленных строк.
    """
    products = list(products)  # может прийти генератор — нам нужно два прохода
    if not products:
        logger.info("Список продуктов пуст, в базу ничего не пишем.")
        return 0

    cursor = conn.cursor()
    before_count = cursor.execute("SELECT COUNT(*) FROM products").fetchone()[0]

    payload = [
        (p.name, p.price, p.url, p.rating, p.reviews_count, p.scraped_at)
        for p in products
    ]

    cursor.executemany(
        """
        INSERT OR IGNORE INTO products
            (name, price, url, rating, reviews_count, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?);
        """,
        payload,
    )
    conn.commit()

    after_count = cursor.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    inserted = after_count - before_count

    logger.info(
        "В таблицу products передано %d записей, фактически вставлено (без дублей): %d.",
        len(products),
        inserted,
    )
    return inserted
