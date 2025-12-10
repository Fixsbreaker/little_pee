import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
import os
import json
from typing import List, Dict, Optional


# конфиг

PARSE_CONFIG = {
    'city': 'all',
    'districts': []
}

# 1 чел Алматы (4 района):
# PARSE_CONFIG = {'city': 'almaty', 'districts': ['almalinskij', 'bostandykskij', 'aujezovskij', 'medeuskij']}

# 2 чел Алматы (4 района):
# PARSE_CONFIG = {'city': 'almaty', 'districts': ['zhetysuskij', 'nauryzbajskij', 'turksibskij', 'alatauskij']}

# 3 чел Астана (3 района):
# PARSE_CONFIG = {'city': 'astana', 'districts': ['almatinskij', 'esilskij', 'nura']}

# 4 чел Астана (3 района):
# PARSE_CONFIG = {'city': 'astana', 'districts': ['saryarkinskij', 'bajkonyrskij', 'saraishyk']}

# словари районов: ключ -> (название, слаг для URL)
# ВАЖНО: слаги на krisha.kz: almaty-bostandykskij (не almaty-bostandyk!)
ALMATY_DISTRICTS = {
    'alatauskij': ('Алатауский р-н', 'almaty-alatauskij'),
    'almalinskij': ('Алмалинский р-н', 'almaty-almalinskij'),
    'aujezovskij': ('Ауэзовский р-н', 'almaty-aujezovskij'),
    'bostandykskij': ('Бостандыкский р-н', 'almaty-bostandykskij'),
    'zhetysuskij': ('Жетысуский р-н', 'almaty-zhetysuskij'),
    'medeuskij': ('Медеуский р-н', 'almaty-medeuskij'),
    'nauryzbajskij': ('Наурызбайский р-н', 'almaty-nauryzbajskij'),
    'turksibskij': ('Турксибский р-н', 'almaty-turksibskij'),
}

ASTANA_DISTRICTS = {
    'almatinskij': ('Алматинский р-н', 'astana-almatinskij'),
    'esilskij': ('Есильский р-н', 'astana-esilskij'),
    'nura': ('Нура р-н', 'astana-nura'),
    'saryarkinskij': ('Сарыаркинский р-н', 'astana-saryarkinskij'),
    'bajkonyrskij': ('Байконурский р-н', 'astana-bajkonyrskij'),
    'saraishyk': ('Сарайшык р-н', 'astana-saraishyk'),
}

BASE_URLS = {
    'almaty': 'https://krisha.kz/prodazha/kvartiry/almaty/',
    'astana': 'https://krisha.kz/prodazha/kvartiry/astana/'
}

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
]

# настройки
MAX_PAGES = 100
REQUEST_TIMEOUT = 30

# паузы как в kolesa
MIN_DELAY = 45
MAX_DELAY = 80
MIN_PAGE_DELAY = 15
MAX_PAGE_DELAY = 30

# длинные перерывы
LONG_BREAK_MIN = 120
LONG_BREAK_MAX = 300
BREAK_AFTER_MIN = 4
BREAK_AFTER_MAX = 7

SAVE_EVERY = 5


# глобальные переменные

session = requests.Session()
df = pd.DataFrame()
iteration_cnt = 0
save_cnt = 0
overall_cnt = 0
break_threshold = random.randint(BREAK_AFTER_MIN, BREAK_AFTER_MAX)


# функции очистки данных

def extract_id_from_url(url: str) -> Optional[int]:
    """извлекает ID объявления из URL"""
    match = re.search(r'/a/show/(\d+)', url)
    if match:
        return int(match.group(1))
    return None


def parse_title(title: str) -> Dict:
    """парсит title в структурированные поля
    пример: '3-комнатная квартира · 107 м² · 4/10 этаж'
    """
    result = {
        'rooms': None,
        'area_total': None,
        'floor': None,
        'floors_total': None
    }
    
    # комнаты: "3-комнатная" или "3 комнатная"
    rooms_match = re.search(r'(\d+)[- ]комнат', title)
    if rooms_match:
        result['rooms'] = int(rooms_match.group(1))
    
    # площадь: "107 м²" или "107.5 м²"
    area_match = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', title)
    if area_match:
        result['area_total'] = float(area_match.group(1).replace(',', '.'))
    
    # этаж: "4/10 этаж" или "4 из 10"
    floor_match = re.search(r'(\d+)[/из\s]+(\d+)\s*(?:этаж|эт)', title)
    if floor_match:
        result['floor'] = int(floor_match.group(1))
        result['floors_total'] = int(floor_match.group(2))
    
    return result


def parse_price(price_str: str) -> Optional[int]:
    """парсит цену в число
    пример: '54 999 000〒' -> 54999000
    """
    if not price_str:
        return None
    
    # убираем все кроме цифр
    digits = re.sub(r'[^\d]', '', price_str)
    if digits:
        return int(digits)
    return None


def clean_description(desc: str) -> str:
    """очищает description от мусора krisha"""
    if not desc:
        return ""
    
    # паттерны мусора
    garbage_patterns = [
        r'Оставить заметку.*?В Избранном',
        r'Связывайтесь с продавцом.*?Скрыть подсказку',
        r'Автор объявления.*?Написать сообщение',
        r'Продлить.*?〒',
        r'Отправить в ТОП.*?〒',
        r'В горячие.*?〒',
        r'Срочно, торг.*?〒',
        r'Объявление на карте.*',
        r'Пожаловаться на.*',
        r'Полезные статьи.*',
        r'Объявление посмотрели.*',
        r'Город.*?показать на карте',
        r'Тип дома.*?Бывшее общежитие.*?нет',
        r'О квартире.*?Описание',
        r'Перевести.*?Показать оригинал',
        r'Перевод может быть неточным',
        r'\d+ мин\. на чтение',
        r'Все статьи',
        r'〒',
    ]
    
    clean = desc
    for pattern in garbage_patterns:
        clean = re.sub(pattern, '', clean, flags=re.IGNORECASE | re.DOTALL)
    
    # убираем лишние пробелы
    clean = re.sub(r'\s+', ' ', clean).strip()
    
    # если текст слишком короткий после очистки - вернём пустую строку
    if len(clean) < 20:
        return ""
    
    return clean


def extract_year_built(text: str) -> Optional[int]:
    """извлекает год постройки"""
    match = re.search(r'Год постройки\s*(\d{4})', text)
    if match:
        year = int(match.group(1))
        if 1900 < year <= 2030:
            return year
    return None


def extract_building_type(text: str) -> Optional[str]:
    """извлекает тип дома"""
    match = re.search(r'Тип дома\s*(\w+)', text)
    if match:
        return match.group(1).lower()
    return None


def extract_ceiling_height(text: str) -> Optional[float]:
    """извлекает высоту потолков"""
    match = re.search(r'Высота потолков\s*(\d+(?:[.,]\d+)?)', text)
    if match:
        return float(match.group(1).replace(',', '.'))
    return None


def extract_condition(text: str) -> Optional[str]:
    """извлекает состояние квартиры"""
    match = re.search(r'Состояние квартиры\s*([^\n]+)', text)
    if match:
        return match.group(1).strip()
    return None


def extract_complex_name(text: str) -> Optional[str]:
    """извлекает название ЖК"""
    match = re.search(r'Жилой комплекс\s*([^\n]+)', text)
    if match:
        return match.group(1).strip()
    return None


def extract_kitchen_area(text: str) -> Optional[float]:
    """извлекает площадь кухни"""
    match = re.search(r'(?:Площадь кухни|кухн[яи])\s*[—-]?\s*(\d+(?:[.,]\d+)?)\s*м²', text)
    if match:
        return float(match.group(1).replace(',', '.'))
    return None


def extract_bathroom(text: str) -> Optional[str]:
    """извлекает тип санузла"""
    match = re.search(r'Санузел\s*(\w+)', text)
    if match:
        return match.group(1).lower()
    return None


def extract_parking(text: str) -> Optional[str]:
    """извлекает парковку"""
    match = re.search(r'Парковка\s*(\w+)', text)
    if match:
        return match.group(1).lower()
    return None


def extract_furnished(text: str) -> Optional[str]:
    """извлекает меблировку"""
    match = re.search(r'Квартира меблирована\s*(\w+)', text)
    if match:
        return match.group(1).lower()
    return None


def extract_district_clean(address: str) -> Optional[str]:
    """извлекает чистое название района"""
    match = re.search(r'([А-Яа-яЁё]+(?:ий|ый|ой)?)\s*р-н', address)
    if match:
        return match.group(1)
    return None


def extract_microdistrict(text: str) -> Optional[str]:
    """извлекает микрорайон"""
    match = re.search(r'мкр\.?\s*([А-Яа-яЁё0-9\-]+)', text, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


# вспомогательные функции

def get_random_headers() -> Dict[str, str]:
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
    }


def random_delay(min_d=None, max_d=None):
    if min_d is None:
        min_d = MIN_DELAY
    if max_d is None:
        max_d = MAX_DELAY
    delay = random.uniform(min_d, max_d)
    print(f"ожидание {delay:.1f} сек...")
    time.sleep(delay)


def long_break():
    delay = random.uniform(LONG_BREAK_MIN, LONG_BREAK_MAX)
    print(f"\nдлинный перерыв {delay/60:.1f} мин...")
    time.sleep(delay)


def save_jsonl(data: Dict, filepath: str):
    """сохраняет одну запись в JSONL"""
    with open(filepath, 'a', encoding='utf-8') as f:
        f.write(json.dumps(data, ensure_ascii=False) + '\n')


def save_csv(dataframe, csv_file):
    if dataframe.empty:
        return
    if not os.path.isfile(csv_file):
        dataframe.to_csv(csv_file, mode='w', index=False, header=True, encoding='utf-8-sig')
        print(f'создан {csv_file}')
    else:
        dataframe.to_csv(csv_file, mode='a', index=False, header=False, encoding='utf-8-sig')
    print(f'сохранено {len(dataframe)} записей\n')


def make_request(url: str) -> Optional[BeautifulSoup]:
    global session
    try:
        headers = get_random_headers()
        response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        response.encoding = 'utf-8'
        return BeautifulSoup(response.text, 'html.parser')
    except requests.exceptions.RequestException as e:
        print(f"ошибка: {e}")
        return None


def build_url(base_url: str, district: str = None) -> str:
    """строит URL с фильтром по району
    на krisha.kz районы фильтруются через путь: /almaty/bostandykskij-r-n/
    """
    if district:
        return f"{base_url}{district}-r-n/"
    return base_url


def get_listing_links(soup: BeautifulSoup) -> List[str]:
    links = []
    seen = set()
    html_text = str(soup)
    pattern = r'href=["\']([^"\']*?/a/show/(\d+))[^"\']*["\']'
    matches = re.findall(pattern, html_text)
    
    for href, listing_id in matches:
        clean_url = f'https://krisha.kz/a/show/{listing_id}'
        if clean_url not in seen:
            seen.add(clean_url)
            links.append(clean_url)
    return links


def parse_listing_page(soup: BeautifulSoup, city: str, url: str) -> Dict:
    """парсит страницу в структурированные данные"""
    
    # получаем весь текст для извлечения данных
    full_text = soup.get_text(separator='\n')
    
    # базовые поля
    data = {
        'id': extract_id_from_url(url),
        'url': url,
        'city': city,
        'scraped_at': pd.Timestamp.now(tz="Asia/Almaty").isoformat(),
        
        # из title
        'rooms': None,
        'area_total': None,
        'floor': None,
        'floors_total': None,
        
        # цена
        'price_kzt': None,
        'price_raw': '',
        
        # локация
        'district': None,
        'microdistrict': None,
        'address': '',
        
        # характеристики
        'year_built': None,
        'building_type': None,
        'ceiling_height': None,
        'area_kitchen': None,
        'condition': None,
        'complex_name': None,
        'bathroom': None,
        'parking': None,
        'furnished': None,
        
        # тексты
        'title_raw': '',
        'description_raw': '',
        'description_clean': '',
    }
    
    # title
    try:
        title_tag = soup.find('h1')
        if title_tag:
            data['title_raw'] = title_tag.get_text(strip=True)
        else:
            title_tag = soup.find('title')
            if title_tag:
                data['title_raw'] = title_tag.get_text(strip=True).split(' — ')[0]
        
        # парсим структурированные поля из title
        if data['title_raw']:
            title_parsed = parse_title(data['title_raw'])
            data.update(title_parsed)
    except Exception as e:
        print(f"ошибка title: {e}")
    
    # price
    try:
        for tag in soup.find_all(['div', 'span']):
            text = tag.get_text(strip=True)
            if '〒' in text and len(text) < 50 and any(c.isdigit() for c in text):
                data['price_raw'] = text
                data['price_kzt'] = parse_price(text)
                break
    except Exception as e:
        print(f"ошибка price: {e}")
    
    # description
    try:
        desc_text = ""
        for div in soup.find_all(['div', 'p']):
            text = div.get_text(strip=True)
            if len(text) > 100 and any(word in text.lower() for word in ['квартир', 'комнат', 'ремонт', 'этаж', 'район', 'дом']):
                if len(text) > len(desc_text):
                    desc_text = text
        
        data['description_raw'] = desc_text[:5000] if desc_text else ""
        data['description_clean'] = clean_description(desc_text)
    except Exception as e:
        print(f"ошибка description: {e}")
    
    # address & district
    try:
        if 'Город' in full_text:
            idx = full_text.find('Город')
            chunk = full_text[idx:idx+300]
            lines = chunk.split('\n')
            # ищем в первых 15 строках (много пустых строк из-за HTML)
            for line in lines[1:15]:
                line = line.strip()
                if line and 'р-н' in line:
                    # убираем служебный текст "показать на карте"
                    candidate = re.sub(r'показать на карте', '', line, flags=re.IGNORECASE).strip()
                    if candidate:
                        data['address'] = candidate
                        data['district'] = extract_district_clean(candidate)
                        break
    except Exception as e:
        print(f"ошибка address: {e}")
    
    # дополнительные поля из текста
    try:
        data['year_built'] = extract_year_built(full_text)
        data['building_type'] = extract_building_type(full_text)
        data['ceiling_height'] = extract_ceiling_height(full_text)
        data['area_kitchen'] = extract_kitchen_area(full_text)
        data['condition'] = extract_condition(full_text)
        data['complex_name'] = extract_complex_name(full_text)
        data['bathroom'] = extract_bathroom(full_text)
        data['parking'] = extract_parking(full_text)
        data['furnished'] = extract_furnished(full_text)
        data['microdistrict'] = extract_microdistrict(data['title_raw'] + ' ' + data['address'])
    except Exception as e:
        print(f"ошибка доп полей: {e}")
    
    return data


def matches_district(data: Dict, target_district: str, all_districts: Dict) -> bool:
    """проверяет соответствие объявления целевому району"""
    if not target_district:
        return True  # без фильтра - все подходят
    
    parsed_district = data.get('district')
    if not parsed_district:
        return False
    
    # all_districts[key] = (название, слаг) - берём название
    district_info = all_districts.get(target_district)
    if district_info:
        target_name = district_info[0].lower()  # первый элемент - название
    else:
        target_name = ''
    
    parsed_lower = parsed_district.lower()
    
    # проверяем совпадение названий
    if target_name and target_name in parsed_lower:
        return True
    if parsed_lower in target_name:
        return True
    
    # альтернативные названия для матчинга
    district_aliases = {
        # Алматы
        'alatauskij': ['алатау'],
        'almalinskij': ['алмалин'],
        'aujezovskij': ['ауэзов'],
        'bostandykskij': ['бостандык', 'бостандыкс'],
        'zhetysuskij': ['жетысу'],
        'medeuskij': ['медеу'],
        'nauryzbajskij': ['наурызбай'],
        'turksibskij': ['турксиб'],
        # Астана
        'almatinskij': ['алматин'],
        'esilskij': ['есиль', 'есил', 'есильск'],
        'nura': ['нура'],
        'saryarkinskij': ['сарыарк', 'сарыарка'],
        'bajkonyrskij': ['байконыр', 'байконур'],
        'saraishyk': ['сарайшык'],
    }
    
    aliases = district_aliases.get(target_district, [])
    for alias in aliases:
        if alias in parsed_lower:
            return True
    
    return False


def check_next_page(soup: BeautifulSoup) -> bool:
    pagination = soup.find('nav', class_='paginator')
    if pagination:
        next_link = pagination.find('a', class_='paginator__btn--next')
        if next_link and not next_link.get('disabled'):
            return True
    return False


# основной функционал

def parse_city_district(city_key: str, city_name: str, district_key: str = None, district_name: str = None, district_slug: str = None, all_districts: Dict = None) -> List[Dict]:
    global df, iteration_cnt, save_cnt, overall_cnt, break_threshold
    
    all_listings = []
    skipped_wrong_district = 0
    page = 1
    
    safe_city = city_key.lower()
    safe_district = district_key if district_key else 'all'
    csv_file = f'./krisha_{safe_city}_{safe_district}_clean.csv'
    jsonl_file = f'./krisha_{safe_city}_{safe_district}_raw.jsonl'
    
    print(f"\n{'='*60}")
    if district_name:
        print(f"парсинг: {city_name} -> {district_name}")
        print(f"(двойная фильтрация: URL + проверка района)")
    else:
        print(f"парсинг: {city_name}")
    print(f"csv: {csv_file}")
    print(f"jsonl: {jsonl_file}")
    print(f"{'='*60}")
    
    while page <= MAX_PAGES:
        try:
            # строим URL с правильным слагом
            if district_slug:
                # используем слаг напрямую: almaty-bostandykskij/
                page_url = f"https://krisha.kz/prodazha/kvartiry/{district_slug}/"
                if page > 1:
                    page_url += f"?page={page}"
            else:
                page_url = BASE_URLS[city_key]
                if page > 1:
                    page_url += f"?page={page}"
            
            print(f"\nстраница {page}: {page_url}")
            
            soup = make_request(page_url)
            if not soup:
                print(f"не удалось загрузить страницу {page}")
                break
            
            links = get_listing_links(soup)
            print(f"найдено: {len(links)}")
            
            if not links:
                print("объявления не найдены")
                break
            
            for i, link in enumerate(links, 1):
                print(f"[{i}/{len(links)}] {link}")
                
                random_delay()
                
                listing_soup = make_request(link)
                if not listing_soup:
                    print("не удалось загрузить")
                    continue
                
                # парсим в структурированные данные
                listing_data = parse_listing_page(listing_soup, city_name, link)
                
                # фильтруем по району (сайт может показывать объявления из других районов)
                if district_key and all_districts:
                    if not matches_district(listing_data, district_key, all_districts):
                        parsed_district = listing_data.get('district', 'неизвестен')
                        print(f"  -> пропуск: район '{parsed_district}' != '{district_name}'")
                        skipped_wrong_district += 1
                        continue
                
                if listing_data['title_raw'] or listing_data['description_raw']:
                    all_listings.append(listing_data)
                    
                    # сохраняем сырые данные в JSONL
                    save_jsonl(listing_data, jsonl_file)
                    
                    # добавляем в DataFrame для CSV
                    new_row = pd.DataFrame([listing_data])
                    df = pd.concat([df, new_row], ignore_index=True, sort=False)
                    
                    # краткий вывод
                    rooms = listing_data['rooms'] or '?'
                    area = listing_data['area_total'] or '?'
                    price = listing_data['price_kzt']
                    price_str = f"{price:,}".replace(',', ' ') if price else '?'
                    print(f"  -> {rooms} комн, {area} м², {price_str} тг")
                else:
                    print("пустое объявление")
                
                iteration_cnt += 1
                save_cnt += 1
                overall_cnt += 1
                
                if save_cnt >= SAVE_EVERY:
                    save_csv(df, csv_file)
                    save_cnt = 0
                    df = pd.DataFrame()
                
                if iteration_cnt >= break_threshold:
                    long_break()
                    iteration_cnt = 0
                    break_threshold = random.randint(BREAK_AFTER_MIN + 3, BREAK_AFTER_MAX + 5)
            
            if not check_next_page(soup):
                print("\nпоследняя страница")
                break
            
            page += 1
            if page <= MAX_PAGES:
                print("\nпауза между страницами")
                random_delay(MIN_PAGE_DELAY, MAX_PAGE_DELAY)
                
        except Exception as e:
            print(f"\nошибка: {e}")
            if not df.empty:
                save_csv(df, csv_file)
                df = pd.DataFrame()
            print(f"остановка на странице {page}, собрано: {overall_cnt}")
            break
    
    if not df.empty:
        save_csv(df, csv_file)
        df = pd.DataFrame()
    
    location_str = f"{city_name} - {district_name}" if district_name else city_name
    print(f"\n{location_str}: собрано {len(all_listings)}")
    if skipped_wrong_district > 0:
        print(f"пропущено (другой район): {skipped_wrong_district}")
    return all_listings


def main():
    config_city = PARSE_CONFIG['city']
    config_districts = PARSE_CONFIG['districts']
    
    print("krisha.kz parser v3")
    print("с очисткой и структурированием данных")
    print(f"\nконфиг:")
    print(f"  город: {config_city}")
    print(f"  районы: {config_districts if config_districts else 'все'}")
    print(f"  страниц: {MAX_PAGES}")
    print(f"  задержка: {MIN_DELAY}-{MAX_DELAY} сек")
    
    all_data = []
    
    if config_city == 'all':
        cities = [
            ('almaty', 'Алматы', ALMATY_DISTRICTS),
            ('astana', 'Астана', ASTANA_DISTRICTS)
        ]
    elif config_city == 'almaty':
        cities = [('almaty', 'Алматы', ALMATY_DISTRICTS)]
    elif config_city == 'astana':
        cities = [('astana', 'Астана', ASTANA_DISTRICTS)]
    else:
        print(f"неизвестный город: {config_city}")
        return
    
    for city_idx, (city_key, city_name, all_districts) in enumerate(cities):
        if config_districts:
            # all_districts[key] = (название, слаг)
            districts = [(k, v[0], v[1]) for k, v in all_districts.items() if k in config_districts]
            if not districts:
                print(f"\nне найдены районы {config_districts} для {city_name}")
                continue
        else:
            districts = [(None, None, None)]
        
        for district_idx, (district_key, district_name, district_slug) in enumerate(districts):
            try:
                data = parse_city_district(city_key, city_name, district_key, district_name, district_slug, all_districts)
                all_data.extend(data)
                
                if district_idx < len(districts) - 1:
                    print(f"\nпауза перед следующим районом")
                    time.sleep(60)
            except Exception as e:
                loc = f"{city_name} - {district_name}" if district_name else city_name
                print(f"\nошибка при парсинге {loc}: {e}")
                continue
        
        if city_idx < len(cities) - 1:
            print(f"\nпауза перед следующим городом")
            time.sleep(60)
    
    if all_data:
        print(f"\n{'='*60}")
        print(f"итого собрано: {len(all_data)}")
        print(f"{'='*60}")
    else:
        print("\nданные не собраны")
    
    print("\nend")


if __name__ == "__main__":
    main()


