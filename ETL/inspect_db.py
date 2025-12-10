from __future__ import annotations

import sqlite3
from pathlib import Path


DB_PATH = "products.db"


def _print_row(row: tuple) -> None:
    """
    Чисто утилитарная функция — красиво печатаем запись из БД.
    Ожидаем порядок колонок:
    id, name, price, url, rating, reviews_count, scraped_at
    """
    (
        _id,
        name,
        price,
        url,
        rating,
        reviews_count,
        scraped_at,
    ) = row

    print(f"[{_id}] {name}")
    print(f"  Цена:         {price}")
    print(f"  Рейтинг:      {rating}")
    print(f"  Отзывов:      {reviews_count}")
    print(f"  Ссылка:       {url}")
    print(f"  Собрано в:    {scraped_at}")
    print("-" * 80)


def main() -> None:
    """
    Небольшой helper-скрипт, чтобы глазами убедиться,
    что ETL сделал то, что должен.

    Делает три вещи:
      * проверяет, что база существует;
      * выводит количество строк в таблице products;
      * показывает несколько первых записей.
    """
    db_file = Path(DB_PATH)
    if not db_file.exists():
        print(f"Файл базы данных не найден: {DB_PATH}")
        print('Сначала запусти:  python main.py')
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    total = cur.execute("SELECT COUNT(*) FROM products;").fetchone()[0]
    print(f"Всего записей в products: {total}\n")

    print("Первые 5 записей:")
    rows = cur.execute(
        """
        SELECT id, name, price, url, rating, reviews_count, scraped_at
        FROM products
        ORDER BY id
        LIMIT 5;
        """
    ).fetchall()

    for row in rows:
        _print_row(row)

    conn.close()


if __name__ == "__main__":
    main()
