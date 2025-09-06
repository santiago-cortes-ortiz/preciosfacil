from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
import random
import re
from urllib.parse import quote_plus
from urllib.parse import urljoin
import logging
import time
import json
from typing import List, Dict, Optional
import concurrent.futures

logger = logging.getLogger(__name__)

PROBABILIDAD_DNT_ACTIVADO = 0.6  # 60% de probabilidad de que DNT sea '1'

# Palabras clave comunes de accesorios para filtrar resultados irrelevantes
ACCESSORY_BLACKLIST = [
    "funda", "fundas", "estuche", "case", "protector", "vidrio", "templado", "mica",
    "audifono", "audífono", "audifonos", "audífonos", "auricular", "auriculares", "earbud",
    "cable", "cargador", "adaptador", "soporte", "holder", "trípode", "tripode", "montura",
    "parlante", "altavoz", "bocina", "speaker", "power bank", "batería", "bateria", "punta",
    "malla", "correa", "tempered", "glass", "protección", "proteccion", "protector de pantalla",
]


# Registro de scrapers disponibles. Para añadir uno nuevo, usa
# register_scraper('clave', 'Etiqueta', funcion_scraper)
SCRAPERS: dict[str, dict] = {}


def register_scraper(key: str, label: str, function) -> None:
    SCRAPERS[key] = {"label": label, "function": function}


def ensure_default_scrapers() -> None:
    if not SCRAPERS:
        register_scraper("mercadolibre", "Mercado Libre", process_search_mercadolibre)
        register_scraper("falabella", "Falabella", process_search_falabella)


def get_available_sources() -> list[dict]:
    ensure_default_scrapers()
    return [{"key": k, "label": v["label"]} for k, v in SCRAPERS.items()]


def process_search_mercadolibre(search_query: str, max_retries: int = 3, max_items: int = 20):
    if not search_query:
        return {"results": []}
    delay = random.uniform(1.5, 4.0)  # Entre 2 y 5 segundos
    time.sleep(delay)
    base_url = "https://listado.mercadolibre.com.co"
    formatted_query = slugify_query(search_query)
    full_url = f"{base_url}/{formatted_query}"
    session = create_http_session(max_retries=max_retries)
    headers = get_realistic_headers()
    session.headers.update(headers)
    warm_up_ml_session(session)
    basic = basic_ml_scraper(formatted_query, max_items=max_items)
    
    # Verificar si hay error de bloqueo
    if basic.get('error') and "BLOQUEADO" in basic.get('error'):
        return {
            "results": [],
            "source": "mercadolibre",
            "source_label": "Mercado Libre",
            "query": search_query,
            "url": full_url,
            "error": basic.get('error')
        }
        
    results_html = basic.get('results', [])
    combined = deduplicate_items(results_html, max_items)
    return {
        "results": combined,
        "source": "mercadolibre",
        "source_label": "Mercado Libre",
        "query": search_query,
        "url": full_url,
    }


def process_search_falabella(search_query: str, max_retries: int = 3, max_items: int = 5):
    if not search_query:
        return {"results": []}

    delay = random.uniform(2.0, 5.0) 
    time.sleep(delay)

    base_url = "https://www.falabella.com.co/falabella-co/"
    full_url = f"{base_url}search?Ntt={quote_plus(search_query)}"

    session = create_http_session(max_retries=max_retries)
    headers = get_realistic_headers()
    headers.update({
        "Referer": "https://www.falabella.com.co/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    })
    session.headers.update(headers)
    session.headers["Accept-Encoding"] = "gzip, deflate"
    try:
        response = session.get(full_url, timeout=15)
        response.raise_for_status()
    except Exception as exc:
        logger.exception("Falabella: error al solicitar la página")
        return {
            "results": [],
            "error": str(exc),
            "source": "falabella",
            "source_label": "Falabella",
            "query": search_query,
            "url": full_url,
        }
    cencoding = (response.headers or {}).get("Content-Encoding", "")
    html = ""
    try:
        if "br" in cencoding.lower():
            try:
                import brotli  # type: ignore
                html = brotli.decompress(response.content).decode("utf-8", "replace")
            except Exception:
                # requests no decodifica br por defecto
                response.encoding = response.encoding or "utf-8"
                html = response.text or ""
        else:
            # gzip/deflate los maneja requests
            response.encoding = response.encoding or response.apparent_encoding or "utf-8"
            html = response.text or ""
    except Exception:
        try:
            html = (response.content or b"").decode("utf-8", "replace")
        except Exception:
            html = response.text or ""
    # Señales de anti-bot / captcha
    text_lower = html.lower()
    if any(k in text_lower for k in ["captcha", "no eres un robot", "robot check", "access denied", "awswaf"]):
        pass  # Posible bloqueo detectado

    # Construcción de soup con fallbacks de parser
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        try:
            soup = BeautifulSoup(html, "lxml")  # type: ignore
        except Exception:
            try:
                soup = BeautifulSoup(html, "html5lib")  # type: ignore
            except Exception as exc2:
                logger.exception("FB soup: all parsers failed")
                return {
                    "results": [],
                    "error": f"BeautifulSoup failed: {exc2}",
                    "source": "falabella",
                    "source_label": "Falabella",
                    "query": search_query,
                    "url": full_url,
                }

    items_cards = parse_falabella_cards(soup, max_items=max_items)
    if not items_cards:
        items_cards = parse_next_data_products(soup, max_items=max_items)
    if not items_cards:
        items_cards = parse_json_ld(soup, max_items=max_items)
    if not items_cards:
        items_cards = parse_generic_by_regex_domain(soup, domain_substring="falabella.com.co", max_items=max_items)

    items_dedup = deduplicate_items(items_cards, max_items)

    return {
        "results": items_dedup,
        "source": "falabella",
        "source_label": "Falabella",
        "query": search_query,
        "url": full_url,
    }


def parse_falabella_cards(soup: BeautifulSoup, max_items: int = 20) -> list[dict]:
    items: list[dict] = []

    # Anclas típicas que apuntan a la página de producto
    anchors = soup.select(
        "a.pod-link, a.pod-product__title, a.grid-pod__title, a.falabella-product-link, a[href*='/falabella-co/product/'], a[href*='/product/']"
    )

    # Si no hay anclas, intentar seleccionar contenedores de tarjetas y obtener el <a> interno
    if not anchors:
        product_cards = soup.select(
            "[data-pod], div[data-pod='product-pod'], li[data-pod], div.pod, li.pod, [data-testid='searchResults-product']"
        )
        for card in product_cards:
            a = card.select_one("a[href*='/product/'], a.falabella-product-link, a")
            if a:
                anchors.append(a)

    seen_links: set[str] = set()

    def find_container(node):
        if not node:
            return None
        # Si el propio nodo parece ser una tarjeta (caso <a class="pod pod-link">), úsalo
        try:
            classes = node.get("class", [])
            if any("pod" in cls for cls in classes):
                return node
        except Exception:
            pass
        # Buscar un contenedor con pistas de tarjeta de producto
        return node.find_parent(lambda t: t.name in ["div", "li"] and (
            t.has_attr("data-pod") or
            (t.has_attr("class") and any(
                any(hint in cls for hint in ["pod", "product", "grid", "cards", "tiles"]) for cls in t.get("class", [])
            ))
        )) or node.parent

    def resolve_link(a_tag, container_tag) -> Optional[str]:
        candidates: list[str] = []
        # 1) href directo del anchor
        if a_tag:
            href = a_tag.get("href") or a_tag.get("data-href")
            if href:
                candidates.append(href)
        # 2) anchor descendiente con /product/
        if container_tag:
            inner_a = container_tag.find("a", href=True)
            if inner_a and "/product/" in (inner_a.get("href") or ""):
                candidates.append(inner_a.get("href"))
        # 3) mirar padres cercanos por anchors
        parent = container_tag
        hop = 0
        while parent is not None and hop < 2 and not candidates:
            parent = parent.parent
            hop += 1
            if parent and hasattr(parent, "find"):
                p_a = parent.find("a", href=True)
                if p_a and "/product/" in (p_a.get("href") or ""):
                    candidates.append(p_a.get("href"))
        # 4) data-key -> construir URL base
        data_key = None
        try:
            data_key = (a_tag.get("data-key") if a_tag else None) or (container_tag.get("data-key") if container_tag else None)
        except Exception:
            data_key = None
        if data_key:
            candidates.append(f"/falabella-co/product/{data_key}")

        # Normalizar y elegir
        for href in candidates:
            if not href:
                continue
            link = href
            if link.startswith("/"):
                link = f"https://www.falabella.com.co{link}"
            if link.startswith("http") and "falabella.com" in link:
                return link
        return None

    def extract_title(a_tag, container_tag):
        # Priorizar estructura pod-title + pod-subTitle
        parts: list[str] = []
        if container_tag:
            t1 = container_tag.select_one("b.pod-title")
            t2 = container_tag.select_one("b.pod-subTitle")
            if t1:
                parts.append(t1.get_text(strip=True))
            if t2:
                parts.append(t2.get_text(strip=True))
        text = " ".join([p for p in parts if p])
        if text:
            return text
        # Fallback: texto del propio anchor
        return a_tag.get_text(strip=True)

    def extract_price(container_tag, around_tag) -> tuple[Optional[int], Optional[str]]:
        if not container_tag and not around_tag:
            return None, None
        # 1) Atributos frecuentes en Falabella
        attr_node = None
        if container_tag:
            attr_node = (
                container_tag.select_one("[data-event-price]")
                or container_tag.select_one("[data-internet-price]")
                or container_tag.select_one("li[data-event-price]")
                or container_tag.select_one("li[data-internet-price]")
                or container_tag.select_one("li[data-cmr-price]")
                or container_tag.select_one("li[data-normal-price]")
            )
        if attr_node:
            val = (
                attr_node.get("data-internet-price")
                or attr_node.get("data-event-price")
                or attr_node.get("data-cmr-price")
                or attr_node.get("data-normal-price")
            )
            if val:
                return extract_price_cop(val), "attr"

        # 2) Nodos de precio comunes por clases/atributos de QA
        price_node = None
        if container_tag:
            price_node = (
                container_tag.select_one(".pod-prices__price")
                or container_tag.select_one(".fb-price")
                or container_tag.select_one("[data-qa='price']")
                or container_tag.select_one("[class*='price']")
            )
        price_text = price_node.get_text(" ", strip=True) if price_node else None
        if price_text:
            return extract_price_cop(price_text), "node"

        # 3) Búsqueda cercana por regex de precio con $
        neighbor_texts = ""
        context_node = around_tag or container_tag
        if context_node:
            try:
                neighbor_texts = " ".join(s.get_text(" ", strip=True) for s in context_node.find_all(limit=8))
            except Exception:
                neighbor_texts = context_node.get_text(" ", strip=True)
        price_text = _regex_find_price_text(neighbor_texts)
        if price_text:
            return extract_price_cop(price_text), "regex"
        return None, None

    def extract_thumb(container_tag):
        if not container_tag:
            return None
        img_tag = (
            container_tag.select_one("picture img") or
            container_tag.select_one("img")
        )
        if img_tag:
            return img_tag.get("data-src") or img_tag.get("src")
        return None

    discards_duplicate = 0
    discards_no_title_or_link = 0
    discards_no_price = 0
    sample_logged = 0
    derived_link_count = 0

    for a in anchors:
        try:
            container = find_container(a)
            href = resolve_link(a, container) or ""
            if not href:
                discards_no_title_or_link += 1
                continue
            else:
                derived_link_count += 1

            if href in seen_links:
                discards_duplicate += 1
                continue
            seen_links.add(href)

            title = extract_title(a, container)
            price_cop, price_method = extract_price(container, a)
            thumbnail = extract_thumb(container)

            if title and href and price_cop is not None:
                # Filtrar accesorios irrelevantes para consultas de celulares
                lower_title = title.lower()
                if any(w in lower_title for w in ACCESSORY_BLACKLIST):
                    continue
                items.append(
                    {
                        "title": title,
                        "link": href,
                        "price_cop": price_cop,
                        "price_str": format_price_cop(price_cop),
                        "thumbnail": thumbnail,
                    }
                )
                sample_logged += 1
            if len(items) >= max_items:
                break
        except Exception:
            continue

    # Contabilizar descartes por precio
    for a in anchors:
        # Nota: segunda pasada para contar no_precio si no estaba en seen_links
        try:
            href = a.get("href") or ""
            if not href:
                continue
            if not href.startswith("http"):
                href = f"https://www.falabella.com.co{href}"
            if href in {it["link"] for it in items}:
                continue
            container = find_container(a)
            price_val, _ = extract_price(container, a)
            if price_val is None:
                discards_no_price += 1
        except Exception:
            pass



    return items


def parse_next_data_products(soup: BeautifulSoup, max_items: int = 20) -> list[dict]:
    results: list[dict] = []
    try:
        scripts: list = []
        by_id = soup.find('script', id='__NEXT_DATA__')
        if by_id:
            scripts.append(by_id)
        # Algunos sitios embeben JSON en scripts type application/json
        scripts.extend(soup.find_all('script', {'type': 'application/json'}))

        def iter_dicts(obj):
            if isinstance(obj, dict):
                yield obj
                for v in obj.values():
                    yield from iter_dicts(v)
            elif isinstance(obj, list):
                for it in obj:
                    yield from iter_dicts(it)

        price_keys = {'price', 'salePrice', 'internetPrice', 'eventPrice', 'cmrPrice', 'displayPrice'}
        title_keys = {'name', 'displayName', 'title'}
        url_keys = {'url', 'link', 'pdpUrl', 'productUrl'}
        image_keys = {'image', 'thumbnail', 'img', 'imageUrl', 'images'}

        def pick_first(d: dict, keys: set[str]):
            for k in keys:
                if k in d and d[k]:
                    return d[k]
            return None

        for sc in scripts:
            raw = sc.string or sc.get_text(strip=True) or ''
            if not raw:
                continue
            # Intentar parsear como JSON completo
            try:
                data = json.loads(raw)
            except Exception:
                # A veces hay JS con asignaciones; intentar extraer bloque JSON entre llaves
                m = re.search(r"\{[\s\S]*\}", raw)
                if not m:
                    continue
                try:
                    data = json.loads(m.group(0))
                except Exception:
                    continue

            count_before = len(results)
            for d in iter_dicts(data):
                if not isinstance(d, dict):
                    continue
                # Detectar estructura de producto básica
                possible_title = pick_first(d, title_keys)
                possible_url = pick_first(d, url_keys)
                possible_price = pick_first(d, price_keys)
                possible_image = pick_first(d, image_keys)

                # Si images es lista, tomar la primera cadena
                if isinstance(possible_image, list) and possible_image:
                    possible_image = next((x for x in possible_image if isinstance(x, str) and x.startswith('http')), None)

                if not possible_title or not possible_url or possible_price is None:
                    continue

                # Filtrar por dominio de falabella y por ruta de producto si es posible
                url_val = str(possible_url)
                if url_val.startswith('/'):
                    url_val = f"https://www.falabella.com.co{url_val}"
                if 'falabella.com' not in url_val:
                    continue

                # Normalizar precio
                if isinstance(possible_price, (int, float)):
                    price_cop = int(float(possible_price))
                else:
                    price_cop = extract_price_cop(str(possible_price))

                if not price_cop:
                    continue

                results.append({
                    'title': str(possible_title),
                    'link': url_val,
                    'price_cop': price_cop,
                    'price_str': format_price_cop(price_cop),
                    'thumbnail': possible_image if isinstance(possible_image, str) else None,
                })

                if len(results) >= max_items:
                    break

            if len(results) >= max_items:
                break
    except Exception:
        pass
    return results


def _regex_find_price_text(text: str | None) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"\$\s*[\d\.]{4,}(?:,[0-9]{2})?", text)
    return m.group(0) if m else None

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
        # Delay más realista entre páginas
        delay = random.uniform(1.0, 3.0)
        time.sleep(delay)
        offers = "https://www.mercadolibre.com.co/ofertas"
        r2 = session.get(offers, timeout=10)
    except Exception:
        pass


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
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
    except Exception as exc:
        logger.exception("BASIC ML: error al solicitar la página")
        return {"results": [], "url": url, "preview": "", "error": str(exc)}

    html = response.text or ""
    preview = html[:1000]
    
    # Detección de bloqueo por Mercado Libre
    text_lower = html.lower()
    if any(k in text_lower for k in ["captcha", "no eres un robot", "robot check", "access denied", "verifica que no eres"]):
        error_msg = "BLOQUEADO: Mercado Libre ha detectado el scraper como bot"
        logger.error(error_msg)
        return {"results": [], "url": url, "preview": preview, "error": error_msg}
        
    soup = BeautifulSoup(html, 'html.parser')

    anchors = _select_basic_title_anchors(soup)
    items = _collect_items_from_anchors(anchors, max_items)
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
    if any(keyword in page_text for keyword in ["no eres un robot", "verifica que no eres", "captcha", "robot check", "access denied"]):
        error_msg = "BLOQUEADO: Mercado Libre ha detectado el scraper como bot"
        logger.error(error_msg)
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
            pass

        if len(items) >= max_items:
            break


    if items:
        return items

    # Fallback: buscar por anclas de título y reconstruir contenedor
    anchors = soup.select('a.poly-component__title')
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
            pass


    if items:
        return items

    # Fallback JSON-LD
    items = parse_json_ld(soup, max_items=max_items)
    if items:
        return items

    # Fallback genérico: buscar anchors a dominios de ML y extraer precios cercanos por regex
    items = parse_generic_by_regex(soup, max_items=max_items)
    return items








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


def parse_generic_by_regex_domain(soup: BeautifulSoup, domain_substring: str, max_items: int = 20) -> list[dict]:
    results: list[dict] = []
    seen_links: set[str] = set()
    anchors = soup.select(f'a[href*="{domain_substring}"]')
    for a in anchors:
        href = a.get('href') or ''
        text = a.get_text(strip=True)
        if not href or len(text) < 10:
            continue
        if href in seen_links:
            continue
        seen_links.add(href)
        container = a.find_parent(['div','li'])
        price_text = None
        if container:
            price_node = (
                container.select_one('.andes-money-amount__fraction') or
                container.select_one('.price-tag-fraction') or
                container.select_one("[class*='price']")
            )
            if price_node:
                price_text = price_node.get_text(strip=True)
        if not price_text:
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
        pass
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
        return items
    except Exception:
        pass
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

    # Inicializar registro por defecto si está vacío
    ensure_default_scrapers()

    if not sources:
        sources = list(SCRAPERS.keys())

    aggregated_items: list[dict] = []
    errors: list[str] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(sources) or 2) as executor:
        # Mapear futuros a la fuente para poder identificar los resultados
        future_to_source = {
            executor.submit(
                SCRAPERS[source]["function"], search_query, max_items=max_items_per_source
            ): source
            for source in sources if source in SCRAPERS
        }

        for future in concurrent.futures.as_completed(future_to_source):
            source = future_to_source[future]
            entry = SCRAPERS.get(source)
            if not entry:
                continue
            
            try:
                data = future.result()
                if data.get("results"):
                    for item in data["results"]:
                        # Etiquetar con el nombre amigable de la fuente
                        item["source"] = entry.get("label", source)
                    aggregated_items.extend(data["results"])
                if data.get("error"):
                    errors.append(f"{entry.get('label', source)}: {data['error']}")
            except Exception as exc:
                errors.append(f"{entry.get('label', source)}: {exc}")

    aggregated_items.sort(key=lambda x: x.get("price_cop", 0))

    best_item = aggregated_items[0] if aggregated_items else None

    return {
        "results": aggregated_items,
        "errors": errors,
        "query": search_query,
        "sources": sources,
        "best_item": best_item,
    }

def process_search(search_query: str, max_retries: int = 3, max_items: int = 20):
    return process_search_mercadolibre(search_query, max_retries=max_retries, max_items=max_items)

