# ETL-пайплайн для Яндекс.Маркета (пример: "кроссовки мужские")

Небольшой ETL-проект, собранный под тестовое задание.
Цель — показать полноценный цикл: сбор → преобразование → загрузка в БД → логирование.

---

## Что делает проект

### **1) Сбор данных (Extract + Transform)**
Скрипт открывает Яндекс.Маркет через Playwright, вводит поисковый запрос
**"кроссовки мужские"**, затем скроллит бесконечную ленту, пока не будут собраны **1000 карточек**.

Собираются поля:
- название товара
- цена (float)
- ссылка
- рейтинг
- количество отзывов
- дата и время сбора (DD-MM-YYYY hh:mm:ss)

Ошибочные карточки пропускаются, информация уходит в лог.

---

### **2) Загрузка данных в SQLite**
Таблица создаётся автоматически:

```sql
CREATE TABLE IF NOT EXISTS products (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL,
    price         REAL    NOT NULL,
    url           TEXT    NOT NULL,
    rating        REAL,
    reviews_count INTEGER,
    scraped_at    TEXT    NOT NULL,
    UNIQUE (url, scraped_at)
);
```

`INSERT OR IGNORE` предотвращает дублирование данных.

---

### **3) Логирование и мониторинг**
Лог фиксирует:
- начало и конец ETL,
- длительность выполнения,
- количество собранных карточек,
- количество реально вставленных строк,
- ошибки парсинга,
- критические ошибки.

---

## Структура проекта
```
/
│── main.py          # Точка входа, orchestration ETL
│── scraper.py       # Парсинг и Playwright
│── db_layer.py      # Работа с SQLite
│── inspect_db.py    # Просмотр содержимого таблицы
│── products.db      # Создаётся автоматически
│── README.md
```

---

## Как запустить

### 1. Установить зависимости
```bash
pip install playwright
playwright install
playwright install chromium
```

### 2. Запуск ETL
```bash
python main.py
```

### 3. Посмотреть содержимое БД
```bash
python inspect_db.py
```

---

## Технологии
- Python 3.10+
- Playwright
- SQLite
- Logging
- Типизация и структурированный код

---

