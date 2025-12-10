import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
from typing import List, Dict, Optional


# конфиг

# базовые urk для парсинга (продажа квартир)
# Алматы: /prodazha/kvartiry/almaty/
# Астана: /prodazha/kvartiry/astana/
BASE_URLS = {
    'Алматы': 'https://krisha.kz/prodazha/kvartiry/almaty/',
    'Астана': 'https://krisha.kz/prodazha/kvartiry/astana/'
}

# Список User-Agent для ротации (типо браузер)
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

# настройки парсинга
MAX_PAGES = 5  # максимальное количество страниц для парсинга (потом можно изменить)
MIN_DELAY = 2  # минимальная задержка между запросами (в сек)
MAX_DELAY = 5  # максимальная задержка между запросами (в сек)
REQUEST_TIMEOUT = 30  # таймаут запроса (в сек)



# вспомог функции

# создаём сессию для сохранения cookies между запросами
session = requests.Session()


def get_random_headers() -> Dict[str, str]:
    """
    возвращает случайные HTTP-заголовки для имитации браузера
    помогает избежать блокировки со стороны сайта
    """
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
    }


def random_delay():
    """
    добавляет случайную задержку между запросами
    важно для Anti-detect сайт не заблокирует айпишник
    """
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    print(f"ожидание {delay:.1f} секунд...")
    time.sleep(delay)


def make_request(url: str) -> Optional[BeautifulSoup]:
    """
    выполняет GET-запрос к указанному URL.
    возвращает объект BeautifulSoup или None в случае ошибки.
    """
    global session
    try:
        headers = get_random_headers()
        
        # используем сессию для сохранения cookies
        response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()  # проверяем статус ответа
        
        # устанавливаем правильную кодировку
        response.encoding = 'utf-8'
        
        # парсим HTML с помощью BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup
    
    except requests.exceptions.RequestException as e:
        print(f"ошибка при запросе {url}: {e}")
        return None


# функция парсинга

def get_listing_links(soup: BeautifulSoup) -> List[str]:
    """
    извлекает ссылки на объявления со страницы списка.
    
    структура krisha.kz:
    Ссылки на объявления имеют формат /a/show/XXXXXX
    """
    links = []
    seen = set()  # для избежания дубликатов
    
    # получаем весь HTML как строку
    html_text = str(soup)
    
    # ищем все ссылки на объявления через regex
    pattern = r'href=["\']([^"\']*?/a/show/(\d+))[^"\']*["\']'
    matches = re.findall(pattern, html_text)
    
    for href, listing_id in matches:
        # формируем чистый URL
        clean_url = f'https://krisha.kz/a/show/{listing_id}'
        
        # добавляем только уникальные ссылки
        if clean_url not in seen:
            seen.add(clean_url)
            links.append(clean_url)
    
    return links


def parse_listing_page(soup: BeautifulSoup, city: str) -> Dict[str, str]:
    """
    парсит страницу отдельного объявления.
    извлекает: заголовок, описание, цену, адрес.
    
    аргументы:
        soup: BeautifulSoup объект страницы объявления
        city: Название города (Алматы или Астана)
    
    возвращает:
        словарь с данными объявления
    """
    data = {
        'title': '',
        'description': '',
        'price': '',
        'address': '',
        'city': city,
        'url': ''
    }
    
    try:
        # заголовок находится в теге <title> или <h1>
        title_tag = soup.find('h1')
        if title_tag:
            data['title'] = title_tag.get_text(strip=True)
        else:
            # альтернативно из тега title
            title_tag = soup.find('title')
            if title_tag:
                data['title'] = title_tag.get_text(strip=True).split(' — ')[0]
    except Exception as e:
        print(f"    ⚠️ Ошибка при извлечении заголовка: {e}")
    
    try:
        # ищем текст после "Описание" в странице
        # описание обычно в теге с классом или data-атрибутом
        
        # способ 1: Ищем div с текстом описания
        desc_text = ""
        
        # ищем все текстовые блоки и фильтруем
        for div in soup.find_all(['div', 'p']):
            text = div.get_text(strip=True)
            # описание обычно длинное и содержит характерные слова
            if len(text) > 100 and ('квартир' in text.lower() or 'комнат' in text.lower() or 
                                     'ремонт' in text.lower() or 'этаж' in text.lower() or
                                     'район' in text.lower() or 'дом' in text.lower()):
                if len(text) > len(desc_text):
                    desc_text = text
        
        # способ 2: ищем текст который начинается с эмодзи или определенных слов
        if not desc_text:
            all_text = soup.get_text(separator='\n')
            lines = all_text.split('\n')
            for i, line in enumerate(lines):
                line = line.strip()
                if ('Описание' in line and len(line) < 20) or line.startswith('♥'):
                    # собираем следующие строки как описание
                    desc_lines = []
                    for j in range(i, min(i+50, len(lines))):
                        if lines[j].strip() and 'Цена м2' not in lines[j]:
                            desc_lines.append(lines[j].strip())
                        if 'Пожаловаться' in lines[j] or 'Полезные статьи' in lines[j]:
                            break
                    desc_text = ' '.join(desc_lines)
                    break
        
        data['description'] = desc_text[:5000] if desc_text else ""  # ограничиваем длину
        
    except Exception as e:
        print(f"ошибка при извлечении описания: {e}")
    
    try:
        # цена обычно содержит символ тенге (〒) или слово "млн"
        price_text = ""
        for tag in soup.find_all(['div', 'span']):
            text = tag.get_text(strip=True)
            if '〒' in text and len(text) < 50:
                # проверяем что это цена (содержит число)
                if any(char.isdigit() for char in text):
                    price_text = text
                    break
        
        if not price_text:
            # ищем текст вида "XX млн"
            all_text = soup.get_text()
            price_match = re.search(r'(\d[\d\s]*(?:млн|000\s*000)?\s*〒)', all_text)
            if price_match:
                price_text = price_match.group(1).strip()
        
        data['price'] = price_text
        
    except Exception as e:
        print(f"    ⚠️ Ошибка при извлечении цены: {e}")
    
    try:
        # адрес обычно содержит "р-н" (район) или название улицы
        address_text = ""
        
        # ищем в тексте после "Город"
        all_text = soup.get_text()
        if 'Город' in all_text:
            # Ищем текст после "Город"
            idx = all_text.find('Город')
            chunk = all_text[idx:idx+200]
            # Извлекаем адрес
            lines = chunk.split('\n')
            for line in lines[1:5]:
                line = line.strip()
                if line and 'показать' not in line.lower():
                    address_text = line
                    break
        
        if not address_text:
            # Иием текст с названием района
            for tag in soup.find_all(['div', 'span']):
                text = tag.get_text(strip=True)
                if 'р-н' in text and len(text) < 100:
                    address_text = text
                    break
        
        data['address'] = address_text
        
    except Exception as e:
        print(f"ошибка при извлечении адреса: {e}")
    
    return data


def check_next_page(soup: BeautifulSoup, current_page: int) -> bool:
    """
    Проверяет есть ли следующая страница в пагинации
    """
    # ищем пагинацию
    pagination = soup.find('nav', class_='paginator')
    if pagination:
        # проверяем есть ли ссылка на след страницу
        next_link = pagination.find('a', class_='paginator__btn--next')
        if next_link and not next_link.get('disabled'):
            return True
    return False


# основной функционал парсера

def parse_city(city: str, base_url: str) -> List[Dict[str, str]]:
    """
    парсит все объявления для указанного города.
    
    аргументы:
        city: название города
        base_url: базовый URL для парсинга
    
    возвращает:
        список словарей с данными объявлений
    """
    all_listings = []
    page = 1
    
    print(f"парсинг города: {city}")
    
    while page <= MAX_PAGES:
        if page == 1:
            page_url = base_url
        else:
            page_url = f"{base_url}?page={page}"
        
        print(f"страница {page}: {page_url}")
        
        # получаем страницу со списком объявлений
        soup = make_request(page_url)
        if not soup:
            print(f"не удалось загрузить страницу {page}")
            break
        
        # извлекаем ссылки на объявления
        links = get_listing_links(soup)
        print(f"найдено объявлений на странице: {len(links)}")
        
        if not links:
            print("объявления не найдены завершаем парсинг города")
            break
        
        # парсим каждое объявление
        for i, link in enumerate(links, 1):
            print(f"    [{i}/{len(links)}] Парсинг: {link}")
            
            # задержка перед запросом (Anti-detect)
            random_delay()
            
            # загружаем страницу объявления
            listing_soup = make_request(link)
            if not listing_soup:
                print(f"не удалось загрузить объявление")
                continue
            
            # аарсим данные объявления
            listing_data = parse_listing_page(listing_soup, city)
            listing_data['url'] = link
            
            # добавляем в список если есть хотя бы заголовок или описание
            if listing_data['title'] or listing_data['description']:
                all_listings.append(listing_data)
                print(f"успех : {listing_data['title'][:50]}...")
            else:
                print(f"пустое объявление пропускаем")
        
        # чекаем есть ли следующая страница
        if not check_next_page(soup, page):
            print(f"\n последняя страница")
            break
        
        page += 1
        random_delay()  # задержка перед следующей страницей
    
    print(f"\n город {city}: собрано {len(all_listings)} объявлений")
    return all_listings


def main():
    print("парсер")
    print("цель: сбор данных")
    print("города: Алматы, Астана")
    print("категория: Продажа квартир")
    
    all_data = []
    
    # парсим каждый город
    for city, base_url in BASE_URLS.items():
        try:
            city_data = parse_city(city, base_url)
            all_data.extend(city_data)
        except Exception as e:
            print(f"\n ошибка при парсинге {city}: {e}")
            continue
    
    # сохраняем результаты
    if all_data:
        print("сохранение данных")
        
        # создаем DataFrame
        df = pd.DataFrame(all_data)
        
        # сохраняем в CSV
        output_file = 'krisha_dataset.csv'
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\nданные сохранены в файл: {output_file}")
        print(f"📊 всего объявлений: {len(df)}")
        print(f"\n📋 структура датасета:")
        print(df.info())
        print(f"\n📝 первые 3 записи:")
        print(df.head(3))
        
        # статистика по городам
        print(f"статистика по городам:")
        print(df['city'].value_counts())
        
        # статистика по заполненности описаний
        non_empty_desc = df[df['description'].str.len() > 0].shape[0]
        print(f"\n обьявления с описанием: {non_empty_desc}/{len(df)}")
        
    else:
        print("error")
    

    print("end")



if __name__ == "__main__":
    main()
