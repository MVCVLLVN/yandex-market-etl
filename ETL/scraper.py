from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://market.yandex.ru/"
SEARCH_URL = "https://market.yandex.ru/"
SEARCH_INPUT_SELECTOR = "#header-search"
CARD_SELECTOR = 'div[data-zone-name="productSnippet"]'


@dataclass
class Product:
    """
    Модель одного товара, который мы забираем с витрины.

    scraped_at — момент, когда карточка была распаршена
    (по ТЗ: дата и время в формате DD-MM-YYYY hh:mm:ss).
    """
    name: str
    price: float
    url: str
    rating: float | None
    reviews_count: int | None
    scraped_at: str


def _clean_text(text: str) -> str:
    """Поджимаем лишние пробелы, переводы строк и прочий шум."""
    return " ".join((text or "").split())


def _parse_price(raw: str) -> float:
    """
    Преобразуем строку цены в число.

    Пример:
      '1 768 ₽'  -> 1768.0
      '2 999₽'   -> 2999.0
    """
    if not raw:
        return 0.0
    digits = "".join(ch for ch in raw if ch.isdigit())
    return float(digits) if digits else 0.0


def _parse_rating(raw: str) -> float | None:
    """
    Достаём рейтинг.
    Если не нашли, считаем, что рейтинга нет.
    """
    if not raw:
        return None
    match = re.search(r"\d+[.,]\d+", raw)
    if not match:
        return None
    return float(match.group(0).replace(",", "."))


def _parse_reviews_count(raw: str) -> int | None:
    """
    Парсим количество оценок из строки:

      'Оценок: (12) · 28 купили'
      'Оценок: (2.7K) · 10.1K купили'

    K -> тысячи (2.7K = 2700).
    """
    if not raw:
        return None

    match = re.search(r"\(([^)]+)\)", raw)
    if not match:
        return None

    inner = match.group(1).strip()

    # Формат 2.7K
    if inner.lower().endswith("k"):
        try:
            number = float(inner[:-1].replace(",", ".").strip())
            return int(round(number * 1000))
        except ValueError:
            return None

    digits = "".join(ch for ch in inner if ch.isdigit())
    return int(digits) if digits else None


def _scroll_until_enough_cards(page, *, target: int, max_scrolls: int = 80, pause_ms: int = 1000) -> None:
    """
    Крутим бесконечную ленту, пока:
      * не насобираем хотя бы target карточек,
      * не сделаем max_scrolls шагов,
      * или карточки перестанут подгружаться.
    """
    last_count = 0
    same_count_times = 0

    for i in range(max_scrolls):
        cards = page.query_selector_all(CARD_SELECTOR)
        current_count = len(cards)
        logger.info("Scroll #%d: карточек в DOM = %d", i, current_count)

        if current_count >= target:
            logger.info("Достигли целевого количества карточек: %d", target)
            return

        if current_count == 0:
            logger.warning("На странице не найдено ни одной карточки — возможно, сменился layout.")
            return

        if current_count == last_count:
            same_count_times += 1
        else:
            same_count_times = 0
            last_count = current_count

        if same_count_times >= 3:
            logger.info("Карточки перестали прибавляться, прекращаем скролл.")
            return

        cards[-1].scroll_into_view_if_needed()
        page.wait_for_timeout(pause_ms)


def _extract_from_dom(page, *, limit: int) -> List[Product]:
    """
    Извлекаем данные из DOM и превращаем их в список Product.

    Здесь стараемся быть максимально терпимыми к кривым данным:
    если не хватает рейтинга/отзывов — просто сохраняем None.
    """
    cards = page.query_selector_all(CARD_SELECTOR)
    logger.info("Всего карточек в DOM: %d", len(cards))

    products: List[Product] = []
    errors = 0

    for idx, card in enumerate(cards[:limit], start=1):
        try:
            scraped_at = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

            link_el = card.query_selector('a[data-auto="snippet-link"]')
            href = link_el.get_attribute("href") if link_el else None
            url = BASE_URL.rstrip("/") + (href or "")

            title_el = card.query_selector('p[data-auto="snippet-title"]')
            name = _clean_text(title_el.inner_text()) if title_el else ""

            if not name:
                title_block = card.query_selector('[data-zone-name="title"]')
                if title_block:
                    name = _clean_text(title_block.inner_text())

            price_el = card.query_selector('span[data-auto="snippet-price-current"]')
            price_raw = price_el.inner_text() if price_el else ""
            price = _parse_price(price_raw)

            rating_el = card.query_selector('[data-zone-name="rating"] [data-auto="reviews"]')
            rating_raw = rating_el.inner_text() if rating_el else ""
            rating = _parse_rating(rating_raw)
            reviews = _parse_reviews_count(rating_raw)

            if not url or not name:
                logger.debug(
                    "Карточка #%d пропущена: пустые name/url (name=%r, url=%r)",
                    idx,
                    name,
                    url,
                )
                continue

            products.append(
                Product(
                    name=name,
                    price=price,
                    url=url,
                    rating=rating,
                    reviews_count=reviews,
                    scraped_at=scraped_at,
                )
            )
        except Exception as exc:  # намеренно широкий catch: это граница внешних данных
            errors += 1
            logger.warning("Не удалось распарсить карточку #%d: %s", idx, exc, exc_info=True)

    logger.info("Парсинг DOM завершён: ok=%d, ошибок=%d", len(products), errors)
    return products


def fetch_products(query: str, *, limit: int = 1000) -> List[Product]:
    """
    Высокоуровневая функция: открывает браузер, выполняет поиск,
    скроллит ленту и возвращает список Product.

    В случае серьёзных проблем (сайт недоступен, селекторы перестали работать)
    возвращает пустой список, не кидая исключения наверх.
    """
    logger.info("Запускаю Playwright для запроса %r (limit=%d)", query, limit)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False, slow_mo=80)
            context = browser.new_context()
            page = context.new_page()

            try:
                page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=60_000)
                page.wait_for_selector(SEARCH_INPUT_SELECTOR, timeout=10_000)
            except PlaywrightTimeoutError:
                logger.error("Страница поиска не загрузилась или селектор поиска не найден.")
                return []

            page.fill(SEARCH_INPUT_SELECTOR, query)
            page.press(SEARCH_INPUT_SELECTOR, "Enter")

            try:
                page.wait_for_selector(CARD_SELECTOR, timeout=20_000)
            except PlaywrightTimeoutError:
                logger.error("Не дождались появления карточек. Возможно, изменился layout.")
                return []

            _scroll_until_enough_cards(page, target=limit)
            products = _extract_from_dom(page, limit=limit)

            # чуть подержим окно открытым — удобно при отладке
            time.sleep(1)
            return products

    except Exception as exc:
        logger.error("Критическая ошибка Playwright: %s", exc, exc_info=True)
        return []
