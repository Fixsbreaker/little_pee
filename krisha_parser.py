import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
import os
from typing import List, Dict, Optional


# конфиг

# выбор города и районов для парсинга
# измени под себя
PARSE_CONFIG = {
    'city': 'all',  # 'all', 'almaty', 'astana'
    'districts': []  # пустой = все районы, или ['bostandykskij', 'medeusskij']
}

# если распределять то 1 чел Алматы (4 района):
# PARSE_CONFIG = {'city': 'almaty', 'districts': ['almalinskij', 'bostandykskij', 'auezovskij', 'medeusskij']}

# 2 чел Алматы (4 района):
# PARSE_CONFIG = {'city': 'almaty', 'districts': ['zhylyojskij', 'nauryzbajskij', 'turksibskij', 'alatausckij']}

# 3 чел- Астана (2 района):
# PARSE_CONFIG = {'city': 'astana', 'districts': ['almatinskij', 'bayjkonyrskij']}

# 4 чел- Астана (2 района):
# PARSE_CONFIG = {'city': 'astana', 'districts': ['esil', 'saryarkinskij']}

# районы Алматы
ALMATY_DISTRICTS = {
    'almalinskij': 'Алмалинский р-н',
    'bostandykskij': 'Бостандыкский р-н', 
    'auezovskij': 'Ауэзовский р-н',
    'medeusskij': 'Медеуский р-н',
    'zhylyojskij': 'Жетысуский р-н',
    'nauryzbajskij': 'Наурызбайский р-н',
    'turksibskij': 'Турксибский р-н',
    'alatausckij': 'Алатауский р-н'
}

# районы Астаны
ASTANA_DISTRICTS = {
    'almatinskij': 'Алматинский р-н',
    'bayjkonyrskij': 'Байконурский р-н',
    'esil': 'Есильский р-н',
    'saryarkinskij': 'Сарыаркинский р-н'
}

BASE_URLS = {
    'almaty': 'https://krisha.kz/prodazha/kvartiry/almaty/',
    'astana': 'https://krisha.kz/prodazha/kvartiry/astana/'
}

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

# настройки парсинга
MAX_PAGES = 100
REQUEST_TIMEOUT = 30

# паузы как в kolesa
MIN_DELAY = 45
MAX_DELAY = 80
MIN_PAGE_DELAY = 15
MAX_PAGE_DELAY = 30

# длинные перерывы как в kolesa
LONG_BREAK_MIN = 120
LONG_BREAK_MAX = 300
BREAK_AFTER_MIN = 4
BREAK_AFTER_MAX = 7

# сохранение
SAVE_EVERY = 5


# вспомог функции

session = requests.Session()
df = pd.DataFrame()
iteration_cnt = 0
save_cnt = 0
overall_cnt = 0
break_threshold = random.randint(BREAK_AFTER_MIN, BREAK_AFTER_MAX)


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
    print(f"ожидание {delay:.1f} секунд...")
    time.sleep(delay)


def long_break():
    delay = random.uniform(LONG_BREAK_MIN, LONG_BREAK_MAX)
    print(f"\nдлинный перерыв {delay/60:.1f} минут...")
    time.sleep(delay)


def save_data(dataframe, csv_file):
    if dataframe.empty:
        return
    if not os.path.isfile(csv_file):
        dataframe.to_csv(csv_file, mode='w', index=False, header=True, encoding='utf-8-sig')
        print(f'создан файл {csv_file}')
    else:
        dataframe.to_csv(csv_file, mode='a', index=False, header=False, encoding='utf-8-sig')
    print(f'сохранено {len(dataframe)} записей в {csv_file}\n')


def make_request(url: str) -> Optional[BeautifulSoup]:
    global session
    try:
        headers = get_random_headers()
        response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup
    except requests.exceptions.RequestException as e:
        print(f"ошибка при запросе {url}: {e}")
        return None


def build_url(base_url: str, district: str = None) -> str:
    if district:
        return f"{base_url}?das[region]={district}"
    return base_url


# функции парсинга

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


def parse_listing_page(soup: BeautifulSoup, city: str) -> Dict[str, str]:
    data = {
        'title': '',
        'description': '',
        'price': '',
        'address': '',
        'district': '',
        'city': city,
        'url': ''
    }
    
    try:
        title_tag = soup.find('h1')
        if title_tag:
            data['title'] = title_tag.get_text(strip=True)
        else:
            title_tag = soup.find('title')
            if title_tag:
                data['title'] = title_tag.get_text(strip=True).split(' — ')[0]
    except Exception as e:
        print(f"ошибка заголовка: {e}")
    
    try:
        desc_text = ""
        for div in soup.find_all(['div', 'p']):
            text = div.get_text(strip=True)
            if len(text) > 100 and ('квартир' in text.lower() or 'комнат' in text.lower() or 
                                     'ремонт' in text.lower() or 'этаж' in text.lower() or
                                     'район' in text.lower() or 'дом' in text.lower()):
                if len(text) > len(desc_text):
                    desc_text = text
        
        if not desc_text:
            all_text = soup.get_text(separator='\n')
            lines = all_text.split('\n')
            for i, line in enumerate(lines):
                line = line.strip()
                if ('Описание' in line and len(line) < 20) or line.startswith('♥'):
                    desc_lines = []
                    for j in range(i, min(i+50, len(lines))):
                        if lines[j].strip() and 'Цена м2' not in lines[j]:
                            desc_lines.append(lines[j].strip())
                        if 'Пожаловаться' in lines[j] or 'Полезные статьи' in lines[j]:
                            break
                    desc_text = ' '.join(desc_lines)
                    break
        data['description'] = desc_text[:5000] if desc_text else ""
    except Exception as e:
        print(f"ошибка описания: {e}")
    
    try:
        price_text = ""
        for tag in soup.find_all(['div', 'span']):
            text = tag.get_text(strip=True)
            if '〒' in text and len(text) < 50:
                if any(char.isdigit() for char in text):
                    price_text = text
                    break
        if not price_text:
            all_text = soup.get_text()
            price_match = re.search(r'(\d[\d\s]*(?:млн|000\s*000)?\s*〒)', all_text)
            if price_match:
                price_text = price_match.group(1).strip()
        data['price'] = price_text
    except Exception as e:
        print(f"ошибка цены: {e}")
    
    try:
        address_text = ""
        district_text = ""
        all_text = soup.get_text()
        if 'Город' in all_text:
            idx = all_text.find('Город')
            chunk = all_text[idx:idx+200]
            lines = chunk.split('\n')
            for line in lines[1:5]:
                line = line.strip()
                if line and 'показать' not in line.lower():
                    address_text = line
                    if 'р-н' in line:
                        district_text = line.split(',')[0] if ',' in line else line
                    break
        if not address_text:
            for tag in soup.find_all(['div', 'span']):
                text = tag.get_text(strip=True)
                if 'р-н' in text and len(text) < 100:
                    address_text = text
                    district_text = text.split(',')[0] if ',' in text else text
                    break
        data['address'] = address_text
        data['district'] = district_text
    except Exception as e:
        print(f"ошибка адреса: {e}")
    
    return data


def check_next_page(soup: BeautifulSoup) -> bool:
    pagination = soup.find('nav', class_='paginator')
    if pagination:
        next_link = pagination.find('a', class_='paginator__btn--next')
        if next_link and not next_link.get('disabled'):
            return True
    return False


# основной функционал

def parse_city_district(city_key: str, city_name: str, district_key: str = None, district_name: str = None) -> List[Dict[str, str]]:
    global df, iteration_cnt, save_cnt, overall_cnt, break_threshold
    
    all_listings = []
    page = 1
    base_url = BASE_URLS[city_key]
    
    safe_city = city_key.lower()
    safe_district = district_key if district_key else 'all'
    csv_file = f'./krisha_{safe_city}_{safe_district}.csv'
    
    print(f"\n{'='*60}")
    if district_name:
        print(f"парсинг: {city_name} -> {district_name}")
    else:
        print(f"парсинг города: {city_name}")
    print(f"файл: {csv_file}")
    print(f"{'='*60}")
    
    while page <= MAX_PAGES:
        try:
            if page == 1:
                page_url = build_url(base_url, district_key)
            else:
                separator = '&' if district_key else '?'
                page_url = build_url(base_url, district_key) + f"{separator}page={page}"
            
            print(f"\nстраница {page}: {page_url}")
            
            soup = make_request(page_url)
            if not soup:
                print(f"не удалось загрузить страницу {page}")
                break
            
            links = get_listing_links(soup)
            print(f"найдено объявлений: {len(links)}")
            
            if not links:
                print("объявления не найдены, завершаем")
                break
            
            for i, link in enumerate(links, 1):
                print(f"[{i}/{len(links)}] {link}")
                
                random_delay()
                
                listing_soup = make_request(link)
                if not listing_soup:
                    print("не удалось загрузить")
                    continue
                
                listing_data = parse_listing_page(listing_soup, city_name)
                listing_data['url'] = link
                listing_data['dtime_inserted'] = pd.Timestamp.now(tz="Asia/Almaty").isoformat()
                
                if listing_data['title'] or listing_data['description']:
                    all_listings.append(listing_data)
                    new_row = pd.DataFrame([listing_data])
                    df = pd.concat([df, new_row], ignore_index=True, sort=False)
                    print(f"успех: {listing_data['title'][:40]}...")
                else:
                    print("пустое объявление")
                
                iteration_cnt += 1
                save_cnt += 1
                overall_cnt += 1
                
                if save_cnt >= SAVE_EVERY:
                    save_data(df, csv_file)
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
                save_data(df, csv_file)
                df = pd.DataFrame()
            print(f"остановка на странице {page}, собрано: {overall_cnt}")
            break
    
    if not df.empty:
        save_data(df, csv_file)
        df = pd.DataFrame()
    
    location_str = f"{city_name} - {district_name}" if district_name else city_name
    print(f"\n{location_str}: собрано {len(all_listings)} объявлений")
    return all_listings


def main():    
    config_city = PARSE_CONFIG['city']
    config_districts = PARSE_CONFIG['districts']
    
    print(f"\nконфиг:")
    print(f"город: {config_city}")
    print(f"районы: {config_districts if config_districts else 'все'}")
    print(f"страниц: {MAX_PAGES}")
    print(f"задержка: {MIN_DELAY}-{MAX_DELAY} сек")
    print(f"перерыв: каждые {BREAK_AFTER_MIN}-{BREAK_AFTER_MAX} запросов")
    
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
            districts = [(k, v) for k, v in all_districts.items() if k in config_districts]
            if not districts:
                print(f"\nне найдены районы {config_districts} для {city_name}")
                continue
        else:
            districts = [(None, None)]
        
        for district_idx, (district_key, district_name) in enumerate(districts):
            try:
                data = parse_city_district(city_key, city_name, district_key, district_name)
                all_data.extend(data)
                
                if district_idx < len(districts) - 1:
                    print(f"\nпауза перед следующим районом")
                    time.sleep(60)
            except Exception as e:
                loc = f"{city_name} - {district_name}" if district_name else city_name
                print(f"\nошибка при парсинге {loc}: {e}")
                continue
        
        if city_idx < len(cities) - 1:
            print(f"\nпауза перед следующим городом...")
            time.sleep(60)
    
    if all_data:
        print(f"\nитого собрано: {len(all_data)} объявлений")
    else:
        print("\nданные не собраны")
    
    print("\nend")


if __name__ == "__main__":
    main()
