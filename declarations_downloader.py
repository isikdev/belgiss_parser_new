import requests
import json
import os
import time
from datetime import datetime
import urllib3
import concurrent.futures
import threading
import argparse
import math
import random
from tqdm import tqdm

# Отключаем предупреждения для незащищенных запросов
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Создаем директорию для сохранения данных
output_dir = "declarations_data"
os.makedirs(output_dir, exist_ok=True)

# Базовый URL API
base_url = "https://api.belgiss.by/tsouz/tsouz-certifs-light"

# Параметры запроса по умолчанию
default_params = {
    "page": 1,
    "per-page": 500, 
    "sort": "-certdecltr_id",
    "filter[DocStartDate][gte]": "01.02.2025",
    "filter[DocStartDate][lte]": "01.03.2025",
    "query[trts]": 1
}

# Список заголовков для имитации браузера
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84"
]

# Переменные для работы с прокси
proxy_list = []
current_proxy_index = 0
proxy_lock = threading.Lock()

class RateLimiter:
    """Класс для ограничения частоты запросов к API"""
    def __init__(self, max_calls_per_second=1):
        self.max_calls = max_calls_per_second
        self.calls = []
        self.lock = threading.Lock()

    def _cleanup_old_calls(self):
        current_time = time.time()
        self.calls = [t for t in self.calls if current_time - t < 1.0]

    def wait_for_permission(self):
        with self.lock:
            self._cleanup_old_calls()
            if len(self.calls) >= self.max_calls:
                sleep_time = 1.0 - (time.time() - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time + 0.1)  # Добавляем небольшую задержку
                self._cleanup_old_calls()
            
            self.calls.append(time.time())

# Создаем ограничитель запросов - максимум 2 запроса в секунду
rate_limiter = RateLimiter(max_calls_per_second=2)

def load_proxies(proxy_file):
    """Загружает прокси из файла"""
    global proxy_list
    
    if not os.path.exists(proxy_file):
        print(f"[ОШИБКА] Файл прокси {proxy_file} не найден")
        return False
    
    # Список кодировок для попытки чтения файла
    encodings = ['utf-8', 'utf-16', 'utf-16-le', 'utf-16-be', 'cp1251', 'latin-1']
    
    for encoding in encodings:
        try:
            with open(proxy_file, 'r', encoding=encoding) as f:
                proxies = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            
            proxy_list = proxies
            print(f"Успешно загружено {len(proxy_list)} прокси из файла {proxy_file} (кодировка: {encoding})")
            
            # Выводим первые 5 прокси для проверки формата
            if len(proxy_list) > 0:
                print("Примеры загруженных прокси:")
                for i, proxy in enumerate(proxy_list[:5]):
                    print(f"  {i+1}. {proxy}")
                
            return True
        except UnicodeDecodeError:
            continue
    
    print(f"[ОШИБКА] Не удалось прочитать файл {proxy_file} ни в одной из известных кодировок")
    return False

def get_proxy():
    """Возвращает прокси из списка, равномерно распределяя нагрузку"""
    global current_proxy_index, proxy_list
    
    if not proxy_list:
        return None
    
    with proxy_lock:
        proxy = proxy_list[current_proxy_index]
        current_proxy_index = (current_proxy_index + 1) % len(proxy_list)
        return proxy

def get_random_user_agent():
    """Возвращает случайный User-Agent из списка"""
    return random.choice(USER_AGENTS)

def save_to_json(data, filename):
    """Сохраняет данные в JSON-файл"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def make_request_with_retry(url, params, max_retries=5, delay=3, use_proxy=False):
    """Выполняет запрос к API с поддержкой повторных попыток при ошибке"""
    headers = {
        "User-Agent": get_random_user_agent()
    }
    
    for attempt in range(max_retries):
        try:
            rate_limiter.wait_for_permission()
            
            # Настраиваем прокси, если нужно
            proxies = None
            proxy_used = None
            if use_proxy and proxy_list:
                proxy = get_proxy()
                if proxy:
                    # Для SOCKS прокси мы используем схему прокси как есть
                    if 'socks' in proxy.lower():
                        proxies = {
                            "http": proxy,
                            "https": proxy
                        }
                    # Для HTTP/HTTPS прокси убедимся, что протокол соответствует запросу
                    else:
                        if 'http://' in proxy:
                            proxy_http = proxy
                            proxy_https = proxy.replace('http://', 'https://')
                        elif 'https://' in proxy:
                            proxy_https = proxy
                            proxy_http = proxy.replace('https://', 'http://')
                        else:
                            # Если протокол не указан, добавим его
                            proxy_http = f"http://{proxy}"
                            proxy_https = f"https://{proxy}"
                        
                        proxies = {
                            "http": proxy_http,
                            "https": proxy_https
                        }
                    
                    proxy_used = proxy
                    print(f"[ИНФО] Использую прокси: {proxy}")
            
            # Отключаем проверку SSL-сертификата
            response = requests.get(
                url, 
                params=params, 
                headers=headers,
                proxies=proxies,
                verify=False,
                timeout=30  # Увеличиваем таймаут для работы через прокси
            )
            response.raise_for_status()  # Проверяем на ошибки HTTP
            
            # Если запрос успешный и использовался прокси, выводим информацию
            if proxy_used:
                print(f"[ИНФО] Успешный запрос через прокси: {proxy_used}")
                
            return response.json()
            
        except requests.exceptions.RequestException as e:
            proxy_info = f" через прокси {proxy_used}" if proxy_used else ""
            print(f"[ОШИБКА] Ошибка при запросе{proxy_info} (попытка {attempt+1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:
                # Увеличиваем задержку с каждой попыткой (экспоненциальная задержка)
                retry_delay = delay * (attempt + 1)
                print(f"[ИНФО] Повторная попытка через {retry_delay} секунд...")
                time.sleep(retry_delay)
            else:
                print("[ОШИБКА] Достигнуто максимальное количество попыток. Выход.")
                raise

def download_page(page_num, per_page, timestamp, batch_prefix, use_proxy=False):
    """Загружает одну страницу деклараций"""
    params = default_params.copy()
    params["page"] = page_num
    params["per-page"] = per_page
    
    # Выполняем запрос
    data = make_request_with_retry(base_url, params, use_proxy=use_proxy)
    
    # Получаем данные из ответа
    items = data.get("items", [])
    
    # Сохраняем полученные данные
    filename = os.path.join(output_dir, f"{batch_prefix}_{page_num}_{timestamp}.json")
    save_to_json(data, filename)
    
    return {
        "page": page_num,
        "count": len(items),
        "total_count": data.get("_meta", {}).get("totalCount", 0)
    }

def format_time(seconds):
    """Форматирует время в читаемый вид"""
    if seconds < 60:
        return f"{seconds:.0f} сек"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{minutes:.0f} мин {seconds:.0f} сек"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:.0f} ч {minutes:.0f} мин"

def download_all_declarations(workers=3, per_page=500, use_proxy=False):
    """Главная функция для загрузки всех деклараций"""
    print("="*80)
    print("Инструмент для загрузки деклараций")
    print("Многопоточная загрузка данных с API belgiss.by")
    print("="*80)
    
    print("\nНачало загрузки деклараций...")
    print("ВНИМАНИЕ: Проверка SSL-сертификата отключена. Это может представлять риск безопасности.")
    print(f"Используется {workers} параллельных потоков, {per_page} записей на страницу")
    if use_proxy and proxy_list:
        print(f"Используются прокси: {len(proxy_list)} шт.")
    else:
        print("Прокси отключены")
    print()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_prefix = "declarations_batch"
    
    # Сначала делаем один запрос, чтобы получить общее количество записей
    print("Получение информации о количестве записей...")
    params = default_params.copy()
    params["per-page"] = 1
    data = make_request_with_retry(base_url, params, use_proxy=use_proxy)
    
    total_count = data.get("_meta", {}).get("totalCount", 0)
    # Проверка на наличие данных в заданном периоде
    if total_count == 0:
        print("В указанном периоде не найдено данных деклараций. Пожалуйста, проверьте параметры поиска.")
        return 0
    
    # Рассчитываем количество страниц
    total_pages = math.ceil(total_count / per_page)
    
    print(f"Всего записей: {total_count:,}")
    print(f"Страниц: {total_pages:,}")
    print(f"Запуск многопоточной загрузки с {workers} потоками\n")
    
    # Словарь для отслеживания прогресса
    page_statuses = {page: "в очереди" for page in range(1, total_pages + 1)}
    completed_pages = 0
    error_pages = 0
    downloaded_count = 0
    
    # Используем ThreadPoolExecutor для параллельной загрузки
    start_time = time.time()
    
    # Создаем прогресс-бар для отслеживания
    progress_bar = tqdm(total=total_count, desc="Загрузка записей", unit="декл")
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            # Создаем задачи для всех страниц
            future_to_page = {
                executor.submit(download_page, page, per_page, timestamp, batch_prefix, use_proxy): page
                for page in range(1, total_pages + 1)
            }
            
            # Обрабатываем результаты по мере их завершения
            for future in concurrent.futures.as_completed(future_to_page):
                page = future_to_page[future]
                try:
                    result = future.result()
                    page_count = result["count"]
                    
                    # Увеличиваем счетчики
                    downloaded_count += page_count
                    completed_pages += 1
                    
                    # Обновляем прогресс-бар
                    progress_bar.update(page_count)
                    
                    # Обновляем статус страницы
                    page_statuses[page] = "завершено"
                    
                    # Выводим информацию в консоль каждые 10 страниц или первые 5
                    if completed_pages % 10 == 0 or completed_pages <= 5:
                        elapsed = time.time() - start_time
                        speed = int((downloaded_count / elapsed) * 60) if elapsed > 0 else 0
                        percent_complete = (downloaded_count / total_count) * 100 if total_count > 0 else 0
                        
                        # Оценка времени до завершения
                        if percent_complete > 0:
                            est_total_time = elapsed / (percent_complete / 100)
                            est_remaining = est_total_time - elapsed
                            remaining_time = format_time(est_remaining)
                        else:
                            remaining_time = "неизвестно"
                        
                        print(f"\n[ПРОГРЕСС] Стр. {page}: {downloaded_count}/{total_count} записей ({percent_complete:.1f}%), "
                              f"стр. {completed_pages}/{total_pages}, скорость: {speed} декл/мин, "
                              f"осталось: {remaining_time}")
                    
                except Exception as e:
                    print(f"[ОШИБКА] Ошибка при загрузке страницы {page}: {e}")
                    page_statuses[page] = "ошибка"
                    error_pages += 1
    
    except KeyboardInterrupt:
        print("\n[ИНФО] Загрузка прервана пользователем")
    except Exception as e:
        print(f"[ОШИБКА] Ошибка при многопоточной загрузке: {e}")
    finally:
        progress_bar.close()
    
    # Проверка завершенности загрузки и наличия файлов
    file_count = len(os.listdir(output_dir))
    if file_count == 0:
        print("[ОШИБКА] Не удалось загрузить ни одного файла! Проверьте параметры поиска и доступность API.")
        return 0
    
    # Если загружено меньше файлов, чем ожидалось
    if completed_pages < total_pages:
        print(f"[ПРЕДУПРЕЖДЕНИЕ] Загружено {completed_pages} страниц из {total_pages}. Возможно, не все данные были получены.")
    
    # Выводим итоговую статистику
    elapsed_time = time.time() - start_time
    minutes, seconds = divmod(int(elapsed_time), 60)
    
    print("\n" + "="*80)
    print("РЕЗУЛЬТАТЫ ЗАГРУЗКИ")
    print("="*80)
    print(f"Всего загружено: {downloaded_count:,} записей из {total_count:,}")
    print(f"Успешно обработано: {completed_pages:,} страниц из {total_pages:,}")
    
    if error_pages > 0:
        print(f"Страниц с ошибками: {error_pages:,}")
    
    print(f"Время выполнения: {minutes} мин {seconds} сек")
    
    if downloaded_count > 0 and elapsed_time > 0:
        speed = int((downloaded_count / elapsed_time) * 60)
        print(f"Средняя скорость: {speed:,} записей/мин")
    
    print("="*80)
    
    return downloaded_count

if __name__ == "__main__":
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Скачивание деклараций с API в многопоточном режиме')
    parser.add_argument('--workers', type=int, default=3, help='Количество параллельных потоков (по умолчанию: 3)')
    parser.add_argument('--per-page', type=int, default=500, help='Количество записей на странице (по умолчанию: 500)')
    parser.add_argument('--date-from', type=str, default="01.01.2020", help='Дата начала периода (по умолчанию: 01.01.2020)')
    parser.add_argument('--date-to', type=str, default="31.12.2020", help='Дата окончания периода (по умолчанию: 31.12.2020)')
    parser.add_argument('--proxies', type=str, help='Путь к файлу со списком прокси')
    parser.add_argument('--disable-proxies', action='store_true', help='Отключить использование прокси')
    args = parser.parse_args()
    
    # Обновляем параметры запроса на основе аргументов командной строки
    default_params["filter[DocStartDate][gte]"] = args.date_from
    default_params["filter[DocStartDate][lte]"] = args.date_to
    
    # Загружаем прокси, если указаны
    use_proxy = False
    if args.proxies and not args.disable_proxies:
        if load_proxies(args.proxies):
            use_proxy = True
    
    try:
        download_all_declarations(workers=args.workers, per_page=args.per_page, use_proxy=use_proxy)
    except Exception as e:
        print(f"[ОШИБКА] Ошибка при выполнении программы: {e}") 