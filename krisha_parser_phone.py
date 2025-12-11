#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
import re
import sys
import csv
import json
import time
import random
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Tuple, Any
from urllib.parse import urljoin

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# WebDriver manager
from webdriver_manager.chrome import ChromeDriverManager

# Stealth mode
try:
    from selenium_stealth import stealth
    HAS_STEALTH = True
except ImportError:
    HAS_STEALTH = False
    print("[WARN] selenium_stealth не установлен, режим stealth отключен")

# BeautifulSoup для парсинга HTML
from bs4 import BeautifulSoup



BASE_URL = "https://krisha.kz"

# Задержки (секунды)
MIN_DELAY_LISTING = 45  # мин между листингами
MAX_DELAY_LISTING = 80  # макс между листингами
SLEEP_BETWEEN_PAGES = (5, 10)  # между страницами списка
SLEEP_BETWEEN_ADS = (10, 15)  # между объявлениями при парсинге телефонов
LONG_PAUSE_EVERY = (15, 25)  # долгая пауза каждые N объявлений
LONG_PAUSE_DURATION = (60, 120)  # длительность долгой паузы

# Бан детекция
BAN_COOLDOWN = 900  # 15 минут
MAX_ERRORS_BEFORE_BAN = 3
CONSECUTIVE_ERRORS = 0

# CapSolver
CAPSOLVER_API_KEY = os.getenv("CAPSOLVER_API_KEY", "CAP-7769D270809A08CCC9AFF9B3468B333FDC4A61A86C60F50F5F5D76F25822384C")
CAPSOLVER_EXTENSION_PATH = "/Users/toast/Desktop/pars/kolesa_parser/CapSolver.Browser.Extension"

# Глобальные состояния
IS_LOGGED_IN = False
PROCESSED_URLS_HISTORY = []



ALMATY_DISTRICTS = {
    "almaty-alatauskij": ["Алатауский", "Алатауский район", "Алатауский р-н"],
    "almaty-almalinskij": ["Алмалинский", "Алмалинский район", "Алмалинский р-н"],
    "almaty-aujezovskij": ["Ауэзовский", "Ауэзовский район", "Ауэзовский р-н"],
    "almaty-bostandykskij": ["Бостандыкский", "Бостандыкский район", "Бостандыкский р-н"],
    "almaty-zhetysuskij": ["Жетысуский", "Жетысуский район", "Жетысуский р-н"],
    "almaty-medeuskij": ["Медеуский", "Медеуский район", "Медеуский р-н"],
    "almaty-nauryzbajskiy": ["Наурызбайский", "Наурызбайский район", "Наурызбайский р-н"],
    "almaty-turksibskij": ["Турксибский", "Турксибский район", "Турксибский р-н"],
}

ASTANA_DISTRICTS = {
    "astana-almatinskij": ["Алматы", "Алматы район", "Алматы р-н"],
    "astana-esilskij": ["Есильский", "Есильский район", "Есильский р-н", "Есиль"],
    "astana-nura": ["Нуринский", "Нуринский район", "Нуринский р-н", "Нура"],
    "astana-saryarkinskij": ["Сарыаркинский", "Сарыаркинский район", "Сарыаркинский р-н", "Сарыарка"],
    "r-n-bajkonur": ["Байконурский", "Байконурский район", "Байконурский р-н", "Байконур"],
    "astana-saraishyk": ["Сарайшық", "Сарайшықский", "Сарайшық район", "Сарайшық р-н"],
}

# Распределение на 4 человек
PERSON_CONFIGS = {
    1: {"city": "almaty", "districts": ["almalinskij", "bostandykskij", "aujezovskij", "medeuskij"]},
    2: {"city": "almaty", "districts": ["zhetysuskij", "nauryzbajskiy", "turksibskij", "alatauskij"]},
    3: {"city": "astana", "districts": ["almatinskij", "esilskij", "nura"]},
    4: {"city": "astana", "districts": ["saryarkinskij", "bajkonur", "saraishyk"]},
}



def now_iso() -> str:
    """Текущее время в ISO формате"""
    return datetime.now().isoformat()

def sleep_range(r: Tuple[int, int]):
    """Случайная пауза в диапазоне"""
    time.sleep(random.uniform(r[0], r[1]))

def human_like_click(driver: webdriver.Chrome, element):
    """Имитация человеческого клика"""
    actions = ActionChains(driver)
    actions.move_to_element(element)
    actions.pause(random.uniform(0.1, 0.3))
    actions.click()
    actions.perform()

def random_scroll(driver: webdriver.Chrome):
    """Случайный скролл страницы"""
    scroll_amount = random.randint(200, 600)
    driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
    time.sleep(random.uniform(0.5, 1.5))

def is_ban_error(error_str: str) -> bool:
    """Проверка на ошибку бана"""
    ban_markers = ["timeout", "connection", "refused", "reset", "network"]
    return any(m in error_str.lower() for m in ban_markers)

def handle_ban_cooldown():
    """Обработка бана - пауза"""
    print(f"\n[BAN]  Обнаружен бан! Пауза {BAN_COOLDOWN // 60} минут...")
    for i in range(BAN_COOLDOWN, 0, -60):
        print(f"[BAN] Осталось {i // 60} мин...")
        time.sleep(60)
    print("[BAN] ✓ Продолжаем работу")



def make_driver(headless: bool = False, mobile_ua: bool = True, use_profile: bool = False) -> webdriver.Chrome:
    """Создание Chrome драйвера с CapSolver расширением"""
    
    opts = Options()
    
    # НЕ используем профиль - будем логиниться вручную через скрипт
    # (профиль Chrome нельзя использовать пока Chrome открыт)
    
    # Загружаем CapSolver расширение ЧЕРЕЗ --load-extension (работает!)
    if os.path.exists(CAPSOLVER_EXTENSION_PATH):
        opts.add_argument(f"--load-extension={CAPSOLVER_EXTENSION_PATH}")
        print(f"[CAPSOLVER]  Расширение будет загружено")
    else:
        print(f"[CAPSOLVER]  Расширение не найдено: {CAPSOLVER_EXTENSION_PATH}")
    
    if headless:
        # Headless mode с расширениями требует новый режим
        opts.add_argument("--headless=new")
    
    # Базовые опции
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-notifications")
    
    # User-Agent
    if mobile_ua:
        ua = "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    else:
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    
    opts.add_argument(f"--user-agent={ua}")
    
    # Создаем драйвер
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    
    # Stealth mode
    if HAS_STEALTH:
        stealth(driver,
            languages=["ru-RU", "ru", "en-US", "en"],
            vendor="Google Inc.",
            platform="Linux armv8l" if mobile_ua else "MacIntel",
            webgl_vendor="ARM",
            renderer="Mali-G76",
            fix_hairline=True
        )
    
    # Даём время на инициализацию расширения
    time.sleep(2)
    
    return driver



def detect_recaptcha(driver: webdriver.Chrome) -> bool:
    """Проверка наличия reCAPTCHA на странице"""
    try:
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for iframe in iframes:
            src = iframe.get_attribute("src") or ""
            if "recaptcha" in src or "captcha" in src:
                return True
        
        # Проверяем также элементы капчи
        captcha_elements = driver.find_elements(By.CLASS_NAME, "g-recaptcha")
        if captcha_elements:
            return True
            
        return False
    except:
        return False

def try_solve_recaptcha(driver: webdriver.Chrome, timeout: int = 120) -> bool:
    """Ожидание решения капчи от CapSolver"""
    print(f"[CAPTCHA] Ожидание решения (до {timeout} сек)...")
    start = time.time()
    
    while time.time() - start < timeout:
        # Проверяем исчезновение капчи
        if not detect_recaptcha(driver):
            print("[CAPTCHA]  Капча решена!")
            return True
        
        # Проверяем появление телефонов
        phones = driver.find_elements(By.XPATH, "//a[starts-with(@href, 'tel:')]")
        if phones:
            print("[CAPTCHA]  Телефоны появились!")
            return True
        
        time.sleep(2)
    
    print("[CAPTCHA]  Таймаут решения капчи")
    return False

def try_get_phones(driver: webdriver.Chrome, timeout: int = 10) -> Optional[List[str]]:
    """Извлечение телефонов со страницы"""
    start = time.time()
    
    while time.time() - start < timeout:
        elements = driver.find_elements(By.XPATH, "//a[starts-with(@href, 'tel:')]")
        phones = []
        
        for el in elements:
            href = el.get_attribute("href") or ""
            if href.startswith("tel:"):
                phone = href[4:].strip()
                # Нормализация
                phone = re.sub(r'[^\d+]', '', phone)
                if phone and phone not in phones:
                    phones.append(phone)
        
        if phones:
            return phones
        
        time.sleep(0.5)
    
    return None



def perform_login(driver: webdriver.Chrome, phone: str, password: str) -> bool:
    """Авторизация на krisha.kz"""
    global IS_LOGGED_IN
    
    print(f"[LOGIN] Попытка входа: {phone[:7]}...")
    
    try:
        # Переходим на страницу входа
        driver.get("https://krisha.kz/my")
        time.sleep(3)
        
        # --- ШАГ 1: Ввод телефона ---
        print("[LOGIN] Шаг 1: Ввод телефона")
        try:
            phone_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "login"))
            )
        except:
             # Фоллбек, если name="login" не найден
             phone_input = driver.find_element(By.XPATH, "//input[@type='text']")
             
        phone_input.clear()
        for char in phone:
            phone_input.send_keys(char)
            time.sleep(random.uniform(0.05, 0.1))
        
        time.sleep(1)
        
        # Нажимаем "Продолжить"
        continue_btn = driver.find_element(By.XPATH, "//button[contains(@class, 'ui-button--blue')]")
        human_like_click(driver, continue_btn)
        time.sleep(2)
        
        # --- ШАГ 2: Ввод пароля ---
        print("[LOGIN] Шаг 2: Ввод пароля")
        
        # Ждем появления поля пароля
        try:
            pwd_input = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.NAME, "password"))
            )
        except:
            # Если поле не появилось, возможно капча или ошибка
            if detect_recaptcha(driver):
                print("[LOGIN] Капча после телефона!")
                if not try_solve_recaptcha(driver):
                    return False
                # Ждем поле снова
                pwd_input = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.NAME, "password"))
                )
            else:
                 raise Exception("Поле пароля не появилось")

        pwd_input.clear()
        for char in password:
            pwd_input.send_keys(char)
            time.sleep(random.uniform(0.08, 0.15))
            
        time.sleep(1)
        
        # Нажимаем "Войти" (это та же кнопка или новая, ищем снова)
        try:
             # Ищем кнопку по тексту или классу, так как DOM мог обновиться
             submit = driver.find_element(By.XPATH, "//button[contains(@class, 'ui-button--blue')]")
             human_like_click(driver, submit)
        except:
             # Если клик не прошел, пробуем Enter на поле
             pwd_input.send_keys("\n")
             
        time.sleep(5)
        
        # Проверка успеха - редирект или наличие меню пользователя
        if "krisha.kz/my" in driver.current_url or "login" not in driver.current_url:
             IS_LOGGED_IN = True
             print("[LOGIN] Успех!")
             return True
        else:
             print("[LOGIN]  Не уверен в успехе, URL остался: " + driver.current_url)
             # Проверим наличие элемента профиля
             if driver.find_elements(By.CLASS_NAME, "cabinet-link-item") or driver.find_elements(By.ID, "ab-header-user-menu"):
                 IS_LOGGED_IN = True
                 print("[LOGIN]  Успех (профиль найден)!")
                 return True
                 
        return False
        
    except Exception as e:
        print(f"[LOGIN]  Ошибка: {e}")
        return False



def extract_district_clean(address_text: str) -> Optional[str]:
    """Извлечение района из адреса"""
    
    all_districts = {}
    all_districts.update(ALMATY_DISTRICTS)
    all_districts.update(ASTANA_DISTRICTS)
    
    # Паттерн: "р-н Байконур" (район ПЕРЕД названием)
    match = re.search(r'р-н\s+(\w+)', address_text)
    if match:
        district_name = match.group(1)
        for slug, aliases in all_districts.items():
            for alias in aliases:
                if district_name.lower() in alias.lower() or alias.lower() in district_name.lower():
                    return slug
    
    # Паттерн: "Алматы р-н" (название БЕЗ "инский")
    match = re.search(r'(\w+)\s+р-н', address_text)
    if match:
        district_name = match.group(1)
        for slug, aliases in all_districts.items():
            for alias in aliases:
                if district_name.lower() in alias.lower() or alias.lower().startswith(district_name.lower()):
                    return slug
    
    # Паттерн: стандартный "Xxxский район" или "Xxxский р-н"
    match = re.search(r'(\w+(?:ий|ый|ой))\s*(?:район|р-н)?', address_text)
    if match:
        district_adj = match.group(1)
        for slug, aliases in all_districts.items():
            for alias in aliases:
                if district_adj.lower() in alias.lower():
                    return slug
    
    return None

def parse_listing_details(html: str, url: str) -> Dict[str, Any]:
    """Парсинг деталей объявления"""
    soup = BeautifulSoup(html, 'html.parser')
    data = {"url": url, "id": None, "city": None, "district": None}
    
    # ID из URL
    m = re.search(r'/(\d+)/?$', url)
    if m:
        data["id"] = m.group(1)
    
    # Заголовок
    title_el = soup.find("h1") or soup.find(class_=re.compile(r'title'))
    if title_el:
        data["title_raw"] = title_el.get_text(strip=True)
    
    # Адрес и район
    addr_el = soup.find("div", class_=re.compile(r'address|location'))
    if addr_el:
        addr_text = addr_el.get_text(separator="\n", strip=True)
        lines = addr_text.split("\n")
        
        for line in lines[1:15]:
            line = line.replace("показать на карте", "").strip()
            if not line:
                continue
            
            # Город
            if "Алматы" in line and "р-н" not in line.replace("Алматы р-н", ""):
                data["city"] = "Алматы"
            elif "Астана" in line:
                data["city"] = "Астана"
            
            # Район
            district = extract_district_clean(line)
            if district:
                data["district"] = district
                data["address"] = line
                break
    
    # Цена
    price_el = soup.find(class_=re.compile(r'price'))
    if price_el:
        price_text = price_el.get_text(strip=True)
        price_num = re.sub(r'\D', '', price_text)
        if price_num:
            data["price_kzt"] = int(price_num)
    
    # Характеристики
    for param_el in soup.find_all(class_=re.compile(r'param|offer__info')):
        text = param_el.get_text(strip=True).lower()
        
        if "комнат" in text:
            m = re.search(r'(\d+)', text)
            if m:
                data["rooms"] = int(m.group(1))
        
        if "площадь" in text or "м²" in text:
            m = re.search(r'(\d+(?:[.,]\d+)?)', text)
            if m:
                data["area_total"] = float(m.group(1).replace(',', '.'))
        
        if "этаж" in text:
            m = re.search(r'(\d+)\s*из\s*(\d+)', text)
            if m:
                data["floor"] = int(m.group(1))
                data["floors_total"] = int(m.group(2))
        
        if "год постройки" in text:
            m = re.search(r'(\d{4})', text)
            if m:
                data["year_built"] = int(m.group(1))
    
    return data

def build_search_url(city: str, district: str, page: int = 1) -> str:
    """Построение URL поиска"""
    city_lower = city.lower()
    
    # Формируем правильный slug
    if city_lower == "almaty":
        slug = f"almaty-{district}" if not district.startswith("almaty-") else district
    elif city_lower == "astana":
        if district == "bajkonur":
            slug = "r-n-bajkonur"
        else:
            slug = f"astana-{district}" if not district.startswith("astana-") else district
    else:
        slug = district
    
    url = f"{BASE_URL}/prodazha/kvartiry/{slug}/"
    if page > 1:
        url += f"?page={page}"
    
    return url



def get_listing_urls(driver: webdriver.Chrome, search_url: str) -> List[str]:
    """Получение списка URL объявлений со страницы поиска"""
    
    driver.get(search_url)
    time.sleep(random.uniform(3, 5))
    
    urls = []
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    # Ищем ссылки на объявления
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.match(r'/a/show/\d+', href):
            full_url = urljoin(BASE_URL, href)
            if full_url not in urls:
                urls.append(full_url)
    
    return urls

def reveal_phone_on_page(driver: webdriver.Chrome, url: str, phone: str, password: str) -> Tuple[Optional[List[str]], Dict]:
    """Получение телефона со страницы объявления"""
    global IS_LOGGED_IN, PROCESSED_URLS_HISTORY
    
    meta = {"url": url, "ts": now_iso()}
    
    try:
        driver.get(url)
        time.sleep(random.uniform(2.5, 4.0))
    except Exception as e:
        return None, {**meta, "error": f"load: {e}", "is_ban": is_ban_error(str(e))}
    
    # Скроллим вниз
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
        time.sleep(1)
    except:
        pass
    
    # Ищем кнопку "Показать телефон" / "Позвонить"
    call_button = None
    
    try:
        call_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(., 'телефон') or contains(., 'Позвонить')]"))
        )
    except:
        pass
    
    if not call_button:
        # Пробуем другие селекторы
        for xpath in [
            "//button[@data-test='call-button']",
            "//a[contains(@class, 'phone')]",
            "//div[contains(@class, 'phone')]//button"
        ]:
            try:
                call_button = driver.find_element(By.XPATH, xpath)
                break
            except:
                continue
    
    if not call_button:
        # Может телефон уже виден?
        phones = try_get_phones(driver, timeout=3)
        if phones:
            PROCESSED_URLS_HISTORY.append(url)
            return phones, {**meta, "status": "ok_visible"}
        return None, {**meta, "error": "no_button"}
    
    # Скроллим к кнопке и кликаем
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", call_button)
        time.sleep(0.5)
        human_like_click(driver, call_button)
        print("[CLICK] Кликнул кнопку телефона")
        time.sleep(3.5)
    except Exception as e:
        return None, {**meta, "error": f"click: {e}"}
    
    # Проверяем требование авторизации
    if not IS_LOGGED_IN:
        try:
            login_needed = driver.find_elements(By.XPATH, "//a[contains(., 'Войти') or contains(., 'регистрац')]")
            if login_needed:
                print("[LOGIN] Требуется авторизация")
                
                if not phone or not password:
                    return None, {**meta, "error": "need_creds"}
                
                if not perform_login(driver, phone, password):
                    return None, {**meta, "error": "login_failed"}
                
                # Возвращаемся на страницу
                driver.get(url)
                time.sleep(3)
                
                # Снова кликаем кнопку
                try:
                    call_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'телефон') or contains(., 'Позвонить')]"))
                    )
                    human_like_click(driver, call_button)
                    time.sleep(3.5)
                except:
                    pass
        except:
            pass
    
    # Пробуем получить телефоны
    print("[PHONE] Проверяю наличие телефонов...")
    phones = try_get_phones(driver, timeout=8)
    
    if phones:
        print(f"[PHONE]  Найдено {len(phones)} без капчи: {', '.join(phones)}")
        PROCESSED_URLS_HISTORY.append(url)
        return phones, {**meta, "status": "ok"}
    
    # Проверяем капчу
    print("[PHONE] Телефонов нет, проверяю капчу...")
    
    if detect_recaptcha(driver):
        print("[CAPTCHA]  Обнаружена! Жду решения от CapSolver...")
        if not try_solve_recaptcha(driver):
            return None, {**meta, "error": "captcha_timeout"}
        
        time.sleep(2)
        phones = try_get_phones(driver, timeout=15)
        
        if phones:
            print(f"[PHONE]  После капчи: {', '.join(phones)}")
            PROCESSED_URLS_HISTORY.append(url)
            return phones, {**meta, "status": "ok_after_captcha"}
        else:
            return None, {**meta, "error": "no_phone_after_captcha"}
    else:
        return None, {**meta, "error": "no_phone_no_captcha"}

def save_results(filepath: str, data: List[Dict], mode: str = 'a'):
    """Сохранение результатов в CSV и JSONL"""
    if not data:
        return
    
    # CSV
    csv_path = filepath if filepath.endswith('.csv') else filepath.replace('.jsonl', '.csv')
    file_exists = os.path.exists(csv_path)
    
    with open(csv_path, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        if not file_exists or mode == 'w':
            writer.writeheader()
        writer.writerows(data)
    
    # JSONL
    jsonl_path = csv_path.replace('.csv', '.jsonl')
    with open(jsonl_path, mode, encoding='utf-8') as f:
        for row in data:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')



def main():
    global IS_LOGGED_IN, CAPSOLVER_API_KEY, CONSECUTIVE_ERRORS
    
    parser = argparse.ArgumentParser(description="Krisha.kz Phone Parser")
    parser.add_argument("--city", required=True, choices=["almaty", "astana"], help="Город")
    parser.add_argument("--district", required=True, help="Район (slug без города)")
    parser.add_argument("--pages", type=int, default=1, help="Количество страниц")
    parser.add_argument("--max-listings", type=int, default=0, help="Макс. объявлений (0 = все)")
    parser.add_argument("--output", type=str, default="", help="Файл вывода")
    parser.add_argument("--headless", action="store_true", help="Режим без GUI")
    parser.add_argument("--phone", type=str, default=os.getenv("KRISHA_PHONE"), help="Телефон для входа")
    parser.add_argument("--password", type=str, default=os.getenv("KRISHA_PASSWORD"), help="Пароль")
    parser.add_argument("--capsolver-key", type=str, default=os.getenv("CAPSOLVER_API_KEY"), help="CapSolver API key")
    parser.add_argument("--person", type=int, choices=[1, 2, 3, 4], help="Конфиг для человека 1-4")
    
    args = parser.parse_args()
    
    # Если указан person, используем готовый конфиг
    if args.person:
        config = PERSON_CONFIGS[args.person]
        districts = config["districts"]
        city = config["city"]
        print(f"[CONFIG] Человек {args.person}: {city}, районы: {districts}")
    else:
        districts = [args.district]
        city = args.city
    
    if args.capsolver_key:
        CAPSOLVER_API_KEY = args.capsolver_key
        print(f"[CAPSOLVER]  API ключ установлен")
    
    if not args.phone or not args.password:
        print("[WARN] Логин/пароль не заданы! Телефоны могут быть недоступны.")
    
    # Выходной файл
    output_file = args.output or f"krisha_{city}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    print(f"\n{'='*60}")
    print(f"KRISHA.KZ PHONE PARSER")
    print(f"{'='*60}")
    print(f"Город: {city}")
    print(f"Районы: {districts}")
    print(f"Страниц: {args.pages}")
    print(f"Вывод: {output_file}")
    print(f"{'='*60}\n")
    
    # Создаём драйвер
    driver = make_driver(headless=args.headless, mobile_ua=True)
    
    all_results = []
    processed = 0
    next_pause = random.randint(*LONG_PAUSE_EVERY)
    
    try:
        for district in districts:
            print(f"\n{'='*60}")
            print(f"РАЙОН: {district}")
            print(f"{'='*60}")
            
            for page in range(1, args.pages + 1):
                search_url = build_search_url(city, district, page)
                print(f"\n[PAGE {page}] {search_url}")
                
                listing_urls = get_listing_urls(driver, search_url)
                print(f"[PAGE {page}] Найдено {len(listing_urls)} объявлений")
                
                if not listing_urls:
                    print("[WARN] Нет объявлений на странице")
                    continue
                
                for idx, listing_url in enumerate(listing_urls, 1):
                    if args.max_listings > 0 and processed >= args.max_listings:
                        print(f"\n[LIMIT] Достигнут лимит {args.max_listings} объявлений")
                        break
                    
                    processed += 1
                    print(f"\n[{idx}/{len(listing_urls)} | #{processed}] {listing_url}")
                    
                    try:
                        # Парсим данные объявления
                        driver.get(listing_url)
                        time.sleep(random.uniform(2, 4))
                        
                        listing_data = parse_listing_details(driver.page_source, listing_url)
                        
                        # Получаем телефон
                        phones, meta = reveal_phone_on_page(driver, listing_url, args.phone, args.password)
                        
                        if phones:
                            listing_data["phones"] = ",".join(phones)
                            listing_data["phone_status"] = "ok"
                            print(f"[OK]  {', '.join(phones)}")
                            CONSECUTIVE_ERRORS = 0
                        else:
                            listing_data["phones"] = ""
                            listing_data["phone_status"] = meta.get("error", "unknown")
                            
                            if meta.get("is_ban"):
                                CONSECUTIVE_ERRORS += 1
                                print(f"[BAN?] Ошибка ({CONSECUTIVE_ERRORS}/{MAX_ERRORS_BEFORE_BAN})")
                                
                                if CONSECUTIVE_ERRORS >= MAX_ERRORS_BEFORE_BAN:
                                    handle_ban_cooldown()
                                    CONSECUTIVE_ERRORS = 0
                                    
                                    driver.quit()
                                    driver = make_driver(headless=args.headless, mobile_ua=True)
                                    IS_LOGGED_IN = False
                            else:
                                CONSECUTIVE_ERRORS = 0
                            
                            print(f"[MISS]  {meta.get('error', '?')}")
                        
                        listing_data["parsed_at"] = now_iso()
                        all_results.append(listing_data)
                        
                        # Инкрементальное сохранение
                        save_results(output_file, [listing_data])
                        
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        print(f"[ERR]  {e!r}")
                        
                        if "session" in str(e).lower():
                            driver.quit()
                            driver = make_driver(headless=args.headless, mobile_ua=True)
                            IS_LOGGED_IN = False
                    
                    # Долгая пауза
                    if processed >= next_pause:
                        pause = random.randint(*LONG_PAUSE_DURATION)
                        print(f"\n[PAUSE] ⏸ {pause} сек...")
                        time.sleep(pause)
                        next_pause = processed + random.randint(*LONG_PAUSE_EVERY)
                    
                    # Пауза между объявлениями
                    sleep_range(SLEEP_BETWEEN_ADS)
                
                # Пауза между страницами
                sleep_range(SLEEP_BETWEEN_PAGES)
        
        print(f"\n{'='*60}")
        print(f"✓ ГОТОВО!")
        print(f"Обработано: {processed}")
        print(f"С телефонами: {sum(1 for r in all_results if r.get('phones'))}")
        print(f"Файл: {output_file}")
        print(f"{'='*60}")
        
    except KeyboardInterrupt:
        print(f"\n\n⚠ [INTERRUPT] | Обработано: {processed}")
    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    main()
