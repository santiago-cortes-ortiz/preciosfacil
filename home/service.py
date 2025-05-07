from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
import json
import random

PROBABILIDAD_DNT_ACTIVADO = 0.6  # 60% de probabilidad de que DNT sea '1'


def process_search(search_query, max_retries=3): 
    base_url = "https://listado.mercadolibre.com.co"
    formatted_query = search_query.replace(" ", "-")
    full_url = f"{base_url}/{formatted_query}"
    session = requests.Session()
    session.headers.update({'Accept-Encoding': 'gzip'})
    adapter = requests.adapters.HTTPAdapter(
        max_retries=3,
        pool_connections=10,
        pool_maxsize=30
    )
    print(f"URL: {full_url}")

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
    
    return {
        'User-Agent': random.choice(user_agents),
        'Accept-Language': 'es-CO,es;q=0.9,en-US;q=0.8',
        'Referer': random.choice(referers),  # Dinamismo adicional
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'DNT': '1' if random.random() < PROBABILIDAD_DNT_ACTIVADO else '0',
        'Connection': 'keep-alive'
    }


