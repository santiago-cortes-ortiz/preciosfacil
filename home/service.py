from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
import random
import re
from urllib.parse import quote_plus
import logging
import time
import json
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

PROBABILIDAD_DNT_ACTIVADO = 0.6  # 60% de probabilidad de que DNT sea '1'


def process_search_mercadolibre(search_query: str, max_retries: int = 3, max_items: int = 20):
    if not search_query:
        return {"results": []}

    base_url = "https://listado.mercadolibre.com.co"
    formatted_query = slugify_query(search_query)
    full_url = f"{base_url}/{formatted_query}"
    logger.info("ML: buscando term='%s' url='%s'", search_query, full_url)

    session = create_http_session(max_retries=max_retries)

    headers = get_realistic_headers()
    session.headers.update(headers)
    logger.debug("ML: headers UA='%s' Referer='%s'", headers.get('User-Agent'), headers.get('Referer'))
    warm_up_ml_session(session)

    # Sin uso de API: nos quedamos solo con HTML

    # 1) Scraper básico con headers mínimos (UA genérico) y vista previa de HTML
    basic = basic_ml_scraper(formatted_query, max_items=max_items)
    results_html = basic.get('results', [])

    # 2) Eliminado fallback ?q=: usamos únicamente slug por consistencia

    logger.info("ML HTML: resultados=%d", len(results_html))
    combined = deduplicate_items(results_html, max_items)
    logger.info("ML combinado: total=%d", len(combined))

    return {
        "results": combined,
        "source": "mercadolibre",
        "source_label": "Mercado Libre",
        "query": search_query,
        "url": full_url,
    }

def get_realistic_headers():
    user_agents = [
        # Lista de User-Agents
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (Windows NT 10.0; rv:124.0) Gecko/20100101 Firefox/124.0'
    ]

    referers =  [
        "https://www.mercadolibre.com.co/",
        "https://www.google.com/",
        "https://www.mercadolibre.com.co/ofertas",
        "https://www.mercadolibre.com.co/historial"
    ]
    
    hdrs = {
        'User-Agent': random.choice(user_agents),
        'Accept-Language': 'es-CO,es;q=0.9,en-US;q=0.8',
        'Referer': random.choice(referers),
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'DNT': '1' if random.random() < PROBABILIDAD_DNT_ACTIVADO else '0',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        # Hints de cliente similares a Chrome
        'sec-ch-ua': '"Chromium";v="125", "Not.A/Brand";v="24", "Google Chrome";v="125"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    return hdrs


def warm_up_ml_session(session: requests.Session) -> None:
    try:
        base = "https://www.mercadolibre.com.co/"
        r1 = session.get(base, timeout=10)
        logger.debug("Warm-up 1: %s %s", r1.status_code, base)
        time.sleep(random.uniform(0.2, 0.6))
        offers = "https://www.mercadolibre.com.co/ofertas"
        r2 = session.get(offers, timeout=10)
        logger.debug("Warm-up 2: %s %s", r2.status_code, offers)
    except Exception:
        logger.debug("Warm-up: ignorado por excepción", exc_info=True)


def slugify_query(query: str) -> str:
    # Normaliza a minúsculas, reemplaza espacios por guiones y elimina caracteres no alfanuméricos ni guiones
    q = (query or "").strip().lower()
    q = re.sub(r"\s+", "-", q)
    q = re.sub(r"[^a-z0-9\-]", "", q)
    q = re.sub(r"-+", "-", q).strip('-')
    return q or ""


def basic_ml_scraper(search_slug: str, max_items: int = 5) -> dict:
    url = f"https://listado.mercadolibre.com.co/{search_slug}"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Scraper/1.0)"}
    logger.info("BASIC ML: url='%s'", url)
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
    except Exception as exc:
        logger.exception("BASIC ML: error al solicitar la página")
        return {"results": [], "url": url, "preview": "", "error": str(exc)}

    html = response.text or ""
    preview = html[:1000]
    print(preview)
    soup = BeautifulSoup(html, 'html.parser')

    anchors = _select_basic_title_anchors(soup)
    items = _collect_items_from_anchors(anchors, max_items)

    logger.info("BASIC ML: items=%d", len(items))
    return {"results": items, "url": url, "preview": preview}


def _select_basic_title_anchors(soup: BeautifulSoup) -> List:
    anchors = soup.select('h3.poly-component__title-wrapper > a.poly-component__title')
    if not anchors:
        anchors = soup.select('a.ui-search-link')
    return anchors


def _collect_items_from_anchors(anchors: List, max_items: int) -> List[Dict]:
    items: List[Dict] = []
    for a in anchors:
        item = _anchor_to_item(a)
        if item:
            items.append(item)
        if len(items) >= max_items:
            break
    return items


def _anchor_to_item(a) -> Optional[Dict]:
    try:
        link = a.get('href')
        title = a.get_text(strip=True)
        container = a.find_parent(['div','li'])
        price_text = _extract_price_text(container)
        thumb = _extract_thumbnail(container)
        price_cop = extract_price_cop(price_text) if price_text else None
        if title and link and price_cop is not None:
            return {
                'title': title,
                'link': link,
                'price_cop': price_cop,
                'price_str': format_price_cop(price_cop),
                'thumbnail': thumb,
            }
        return None
    except Exception:
        return None


def _extract_price_text(container) -> Optional[str]:
    if not container:
        return None
    frac = container.select_one('.andes-money-amount__fraction')
    cents = container.select_one('.andes-money-amount__cents')
    price_text: Optional[str] = None
    if frac:
        price_text = frac.get_text(strip=True)
        if cents:
            price_text = f"{price_text},{cents.get_text(strip=True)}"
    return price_text


def _extract_thumbnail(container) -> Optional[str]:
    if not container:
        return None
    img = container.select_one('img')
    if not img:
        return None
    return img.get('data-src') or img.get('src')


def create_http_session(max_retries: int = 3) -> requests.Session:
    try:
        import cloudscraper  # type: ignore
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True,
            },
            delay=random.uniform(0.3, 0.8),
        )
        # cloudscraper ya hereda de requests.Session
        return scraper
    except Exception:
        session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=max_retries,
            pool_connections=10,
            pool_maxsize=30,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({"Accept-Encoding": "gzip"})
        return session

def parse_mercadolibre_results(soup: BeautifulSoup, max_items: int = 20):
    items = []

    # Detección simple de anti-bot/captcha
    page_text = soup.get_text(" ", strip=True).lower()
    if any(keyword in page_text for keyword in ["no eres un robot", "verifica que no eres", "captcha", "robot check"]):
        logger.warning("ML HTML: posible captcha / anti-bot detectado")
        return []

    # Selección robusta de items (prioriza layout poly dentro de li.ui-search-layout__item)
    candidates: list = []
    # 1) Estructura que compartiste: li -> .poly-card
    sel_poly_in_li = soup.select("li.ui-search-layout__item .poly-card")
    candidates.extend(sel_poly_in_li)
    # 2) Fallbacks
    sel_ui_results = soup.select(".ui-search-result, .ui-search-layout--stack .ui-search-layout__item")
    sel_poly_divs = soup.select("div.poly-card.poly-card--list, div.poly-card.poly-card--large, div.poly-card, div.poly-card--mobile")
    sel_li_items = soup.select("li.ui-search-layout__item")
    candidates.extend(sel_ui_results)
    candidates.extend(sel_poly_divs)
    candidates.extend(sel_li_items)

    logger.info(
        "ML selectores: poly_in_li=%d ui_results=%d poly_divs=%d li_items=%d total_candidates=%d",
        len(sel_poly_in_li), len(sel_ui_results), len(sel_poly_divs), len(sel_li_items), len(candidates)
    )

    for node in candidates:
        # Link
        link_tag = (
            node.select_one("h3.poly-component__title-wrapper > a.poly-component__title")
            or node.select_one("a.poly-component__title")
            or node.select_one("a.ui-search-link")
            or node.select_one("a.shops__items-group-details")
            or node.select_one("a")
        )
        link = link_tag.get("href") if link_tag else None

        # Título
        title_tag = (
            node.select_one("h3.poly-component__title-wrapper > a.poly-component__title")
            or node.select_one(".poly-component__title")
            or node.select_one("h2.ui-search-item__title")
            or node.select_one(".shops__item-title")
            or node.select_one("h2")
        )
        title = title_tag.get_text(strip=True) if title_tag else None

        # Precio (texto y normalizado)
        price_fraction = (
            node.select_one(".poly-component__price .poly-price__current .andes-money-amount__fraction")
            or node.select_one(".poly-price__current .andes-money-amount__fraction")
            or node.select_one(".poly-component__price .andes-money-amount__fraction")
            or node.select_one(".andes-money-amount__fraction")
        )
        price_cents = node.select_one(".andes-money-amount__cents")
        price_text = None
        if price_fraction:
            price_text = price_fraction.get_text(strip=True)
            if price_cents:
                price_text = f"{price_text},{price_cents.get_text(strip=True)}"

        # Fallback de precio
        if not price_text:
            price_any = node.select_one(".price-tag-fraction, .price-tag-cents, .ui-search-price__second-line")
            if price_any:
                price_text = price_any.get_text(" ", strip=True)

        price_cop = extract_price_cop(price_text) if price_text else None

        # Imagen
        img_tag = (
            node.select_one("figure.poly-component__image-wrapper img.poly-component__picture")
            or node.select_one(".poly-card__portada img")
            or node.select_one("img")
        )
        thumbnail = None
        if img_tag:
            thumbnail = img_tag.get("data-src") or img_tag.get("src")

        if title and link and price_cop is not None:
            items.append(
                {
                    "title": title,
                    "link": link,
                    "price_cop": price_cop,
                    "price_str": format_price_cop(price_cop),
                    "thumbnail": thumbnail,
                }
            )
        else:
            logger.debug(
                "ML item descartado: have_title=%s have_link=%s have_price=%s",
                bool(title), bool(link), price_cop is not None,
            )

        if len(items) >= max_items:
            break

    logger.info("ML parse: items_validos=%d", len(items))
    if items:
        return items

    # Fallback: buscar por anclas de título y reconstruir contenedor
    anchors = soup.select('a.poly-component__title')
    logger.info("ML fallback anchors poly-component__title=%d", len(anchors))
    for a in anchors:
        try:
            link = a.get('href')
            title = a.get_text(strip=True)
            container = a.find_parent(lambda tag: tag.name in ['div','li'] and any(
                ('class' in tag.attrs and isinstance(tag.attrs['class'], list) and (
                    any('poly-card' in cls for cls in tag.attrs['class']) or any('ui-search' in cls for cls in tag.attrs['class'])
                ))
            ))
            price_fraction = None
            price_cents = None
            thumbnail = None
            if container:
                price_fraction = (
                    container.select_one('.poly-component__price .poly-price__current .andes-money-amount__fraction') or
                    container.select_one('.poly-price__current .andes-money-amount__fraction') or
                    container.select_one('.andes-money-amount__fraction')
                )
                price_cents = container.select_one('.andes-money-amount__cents')
                img_tag = (
                    container.select_one('figure.poly-component__image-wrapper img.poly-component__picture') or
                    container.select_one('.poly-card__portada img') or
                    container.select_one('img')
                )
                if img_tag:
                    thumbnail = img_tag.get('data-src') or img_tag.get('src')

            price_text = None
            if price_fraction:
                price_text = price_fraction.get_text(strip=True)
                if price_cents:
                    price_text = f"{price_text},{price_cents.get_text(strip=True)}"
            if not price_text:
                near_price = a.find_next(string=lambda s: s and s.strip().startswith('$'))
                if near_price:
                    price_text = near_price.strip()

            price_cop = extract_price_cop(price_text) if price_text else None
            if title and link and price_cop is not None:
                items.append({
                    'title': title,
                    'link': link,
                    'price_cop': price_cop,
                    'price_str': format_price_cop(price_cop),
                    'thumbnail': thumbnail,
                })
            if len(items) >= max_items:
                break
        except Exception:
            logger.debug('ML fallback anchor parse error', exc_info=True)

    logger.info("ML fallback parse: items_validos=%d", len(items))
    if items:
        return items

    # Fallback JSON-LD
    items = parse_json_ld(soup, max_items=max_items)
    logger.info("ML json-ld parse: items_validos=%d", len(items))
    if items:
        return items

    # Fallback genérico: buscar anchors a dominios de ML y extraer precios cercanos por regex
    items = parse_generic_by_regex(soup, max_items=max_items)
    logger.info("ML regex parse: items_validos=%d", len(items))
    return items


def log_marker_counts(html_text: str) -> None:
    markers = [
        'poly-card', 'poly-component__title', 'ui-search-layout__item',
        'andes-money-amount__fraction', 'ui-search-result', 'poly-price__current'
    ]
    counts = {m: html_text.count(m) for m in markers}
    logger.info("ML markers: %s", counts)


def parse_generic_by_regex(soup: BeautifulSoup, max_items: int = 20) -> list[dict]:
    results: list[dict] = []
    seen_links: set[str] = set()
    # Anchors que parecen tarjetas de producto
    anchors = soup.select('a[href*="mercadolibre.com.co/"]')
    for a in anchors:
        href = a.get('href') or ''
        text = a.get_text(strip=True)
        if not href or len(text) < 10:
            continue
        if any(x in href for x in ['/p/', '/MCO', '/MLA', '/MCO-']) and 'mercadolibre.com.co' in href:
            if href in seen_links:
                continue
            seen_links.add(href)
            # Buscar precio cercano en el árbol
            container = a.find_parent(['div','li'])
            price_text = None
            if container:
                price_node = (
                    container.select_one('.andes-money-amount__fraction') or
                    container.select_one('.price-tag-fraction')
                )
                if price_node:
                    price_text = price_node.get_text(strip=True)
            if not price_text:
                # Buscar texto de precio en hermanos/cercanos
                neighbor_texts = ' '.join(s.get_text(' ', strip=True) for s in a.parent.find_all(limit=5)) if a.parent else ''
                match = re.search(r'\$\s*[\d\.]{4,}', neighbor_texts)
                if match:
                    price_text = match.group(0)
            price_cop = extract_price_cop(price_text) if price_text else None
            if price_cop:
                results.append({
                    'title': text,
                    'link': href,
                    'price_cop': price_cop,
                    'price_str': format_price_cop(price_cop),
                    'thumbnail': None,
                })
                if len(results) >= max_items:
                    break
    return results


def parse_json_ld(soup: BeautifulSoup, max_items: int = 20) -> list[dict]:
    results: list[dict] = []
    try:
        scripts = soup.find_all('script', {'type': 'application/ld+json'})
        for sc in scripts:
            text = sc.string or sc.get_text(strip=True) or ''
            if not text:
                continue
            # Puede contener múltiples JSONs concatenados
            json_blobs = [text]
            for blob in json_blobs:
                try:
                    data = json.loads(blob)
                except Exception:
                    continue
                candidates = []
                if isinstance(data, dict):
                    candidates.append(data)
                elif isinstance(data, list):
                    candidates.extend([d for d in data if isinstance(d, dict)])
                for d in candidates:
                    atype = d.get('@type')
                    if atype == 'Product':
                        name = d.get('name')
                        url = d.get('url') or (d.get('offers') or {}).get('url')
                        image = d.get('image')
                        offers = d.get('offers')
                        price = None
                        if isinstance(offers, dict):
                            price = offers.get('price')
                        elif isinstance(offers, list) and offers:
                            price = offers[0].get('price')
                        price_cop = int(float(price)) if price is not None else None
                        if name and url and price_cop is not None:
                            results.append({
                                'title': name,
                                'link': url,
                                'price_cop': price_cop,
                                'price_str': format_price_cop(price_cop),
                                'thumbnail': image if isinstance(image, str) else None,
                            })
                    elif atype == 'ItemList':
                        elements = d.get('itemListElement') or []
                        for el in elements:
                            item = el.get('item') if isinstance(el, dict) else None
                            if not isinstance(item, dict):
                                continue
                            name = item.get('name')
                            url = item.get('url')
                            image = item.get('image')
                            offers = item.get('offers')
                            price = None
                            if isinstance(offers, dict):
                                price = offers.get('price')
                            elif isinstance(offers, list) and offers:
                                price = offers[0].get('price')
                            price_cop = int(float(price)) if price is not None else None
                            if name and url and price_cop is not None:
                                results.append({
                                    'title': name,
                                    'link': url,
                                    'price_cop': price_cop,
                                    'price_str': format_price_cop(price_cop),
                                    'thumbnail': image if isinstance(image, str) else None,
                                })
                if len(results) >= max_items:
                    break
            if len(results) >= max_items:
                break
    except Exception:
        logger.debug('ML json-ld parse error', exc_info=True)
    return results


def extract_price_cop(text: str) -> int:
    if not text:
        return 0
    # Mantener solo dígitos
    digits = re.sub(r"[^0-9]", "", text)
    return int(digits) if digits else 0


def format_price_cop(value: int) -> str:
    return f"$ {value:,.0f}".replace(",", ".")


def fallback_ml_api(search_query: str, limit: int = 20) -> list[dict]:
    try:
        api_url = f"https://api.mercadolibre.com/sites/MCO/search?q={quote_plus(search_query)}&limit={limit}"
        headers = {'Accept': 'application/json', 'Accept-Language': 'es-CO'}
        response = requests.get(api_url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        items = []
        for r in data.get('results', [])[:limit]:
            title = r.get('title')
            link = r.get('permalink')
            price = r.get('price')
            thumb = r.get('thumbnail') or r.get('secure_thumbnail')
            if title and link and price is not None:
                price_int = int(price)
                items.append({
                    'title': title,
                    'link': link,
                    'price_cop': price_int,
                    'price_str': format_price_cop(price_int),
                    'thumbnail': thumb,
                })
        logger.debug("ML API parseados=%d de total=%d", len(items), len(data.get('results', [])))
        return items
    except Exception:
        logger.exception("ML API: fallo al consultar")
        return []

def deduplicate_items(items: list[dict], max_items: int) -> list[dict]:
    seen: set[str] = set()
    unique: list[dict] = []
    for it in items:
        key = f"{it.get('link','')}-{it.get('price_cop','')}-{it.get('title','')[:40]}"
        if key in seen:
            continue
        seen.add(key)
        unique.append(it)
        if len(unique) >= max_items:
            break
    unique.sort(key=lambda x: x.get('price_cop', 0))
    return unique

def search_aggregated(search_query: str, sources: list[str] | None = None, max_items_per_source: int = 10):
    if not search_query:
        return {"results": [], "errors": []}

    if not sources:
        sources = ["mercadolibre"]

    aggregated_items: list[dict] = []
    errors: list[str] = []

    logger.info("Agregador: term='%s' sources=%s", search_query, ",".join(sources))
    for source in sources:
        if source == "mercadolibre":
            data = process_search_mercadolibre(search_query, max_items=max_items_per_source)
            if data.get("results"):
                for item in data["results"]:
                    item["source"] = data.get("source_label", "Mercado Libre")
                aggregated_items.extend(data["results"])
            if "error" in data and data["error"]:
                errors.append(f"Mercado Libre: {data['error']}")
        elif source == "falabella":
            # Placeholder: próximamente
            errors.append("Falabella: integración pendiente")
        else:
            errors.append(f"Fuente no soportada: {source}")

    aggregated_items.sort(key=lambda x: x.get("price_cop", 0))
    logger.info("Agregador: total_items=%d errors=%d", len(aggregated_items), len(errors))

    return {
        "results": aggregated_items,
        "errors": errors,
        "query": search_query,
        "sources": sources,
    }

# Compatibilidad hacia atrás

def process_search(search_query: str, max_retries: int = 3, max_items: int = 20):
    return process_search_mercadolibre(search_query, max_retries=max_retries, max_items=max_items)

