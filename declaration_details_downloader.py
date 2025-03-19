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
from tqdm import tqdm
import sys
import random
import queue
import logging
from colorama import init, Fore, Style

# Инициализация colorama для поддержки цветов в Windows
init()

# Отключаем предупреждения для незащищенных запросов
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Создаем директории для сохранения данных
output_dir = "declarations_details"
os.makedirs(output_dir, exist_ok=True)

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"details_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Настраиваем логгер
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)

# Создаем логгер
logger = logging.getLogger('declaration_details')
# Устанавливаем уровень для вывода в файл - всё
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
# Добавляем обработчик только в файл
logger.addHandler(file_handler)

# Добавляем фильтр для NullHandler вместо StreamHandler, чтобы логи не выводились в консоль
logger.handlers = []
logger.addHandler(logging.NullHandler())

# Базовый URL API для деталей деклараций
base_url = "https://api.belgiss.by/tsouz/tsouz-certifs"

# Список заголовков для имитации браузера
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84"
]

# Переменные для отображения статистики на одной строке
status_line = ""
error_line = ""

# Глобальные списки прокси-серверов и блокировка для безопасного доступа
proxy_list = []
proxy_lock = threading.Lock()
proxy_stats = {}  # Статистика работы прокси-серверов
proxy_limiters = {}  # Ограничители для каждого прокси
current_proxy_index = 0

def load_proxies(proxy_file=None):
    """Загружает список прокси-серверов из файла или использует встроенный список"""
    global proxy_list, proxy_stats, proxy_limiters
    
    # Если указан файл с прокси, загружаем из него
    if proxy_file and os.path.exists(proxy_file):
        # Список кодировок для попытки чтения файла
        encodings = ['utf-8', 'utf-16', 'utf-16-le', 'utf-16-be', 'cp1251', 'latin-1']
        
        for encoding in encodings:
            try:
                with open(proxy_file, 'r', encoding=encoding) as f:
                    loaded_proxies = []
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            loaded_proxies.append(line)
                    
                    proxy_list = loaded_proxies
                    
                    # Инициализируем статистику и лимитеры для каждого прокси
                    for proxy in proxy_list:
                        proxy_stats[proxy] = {
                            "success": 0,        # Успешные запросы
                            "errors": 0,         # Ошибки
                            "rate_limit_errors": 0,  # Ошибки превышения лимита
                            "last_used": 0,      # Время последнего использования
                            "active": True       # Флаг активности
                        }
                        
                        # Создаем отдельный лимитер для каждого прокси
                        proxy_limiters[proxy] = AdaptiveRateLimiter(
                            initial_rate=0.5,
                            max_rate=1.0,
                            backoff_factor=0.5,
                            recovery_factor=1.05
                        )
                    
                    print_message(f"Загружено {len(proxy_list)} прокси-серверов из файла {proxy_file} (кодировка: {encoding})")
                    return
            except UnicodeDecodeError:
                # Пробуем следующую кодировку
                continue
            except Exception as e:
                print_message(f"Ошибка при загрузке прокси из файла {proxy_file}: {e}", True)
                break
        
        # Если не удалось прочитать файл ни в одной кодировке, пробуем бинарный режим
        try:
            with open(proxy_file, 'rb') as f:
                loaded_proxies = []
                for line in f:
                    try:
                        # Пробуем декодировать каждую строку по-отдельности
                        decoded_line = line.decode('utf-8', errors='ignore').strip()
                        if decoded_line and not decoded_line.startswith('#'):
                            loaded_proxies.append(decoded_line)
                    except:
                        pass
                
                if loaded_proxies:
                    proxy_list = loaded_proxies
                    
                    # Инициализируем статистику и лимитеры для каждого прокси
                    for proxy in proxy_list:
                        proxy_stats[proxy] = {
                            "success": 0,
                            "errors": 0,
                            "rate_limit_errors": 0,
                            "last_used": 0,
                            "active": True
                        }
                        
                        proxy_limiters[proxy] = AdaptiveRateLimiter(
                            initial_rate=0.5,
                            max_rate=1.0,
                            backoff_factor=0.5,
                            recovery_factor=1.05
                        )
                    
                    print_message(f"Загружено {len(proxy_list)} прокси-серверов из файла {proxy_file} (бинарный режим)")
                    return
        except Exception as e:
            print_message(f"Ошибка при загрузке прокси из файла {proxy_file} в бинарном режиме: {e}", True)
    
    # Если файл не указан или произошла ошибка, используем пустой список
    proxy_list = []
    print_message("Прокси-серверы не загружены. Запросы будут выполняться напрямую.")

def get_next_proxy():
    """Возвращает следующий прокси-сервер из списка с учетом статистики использования"""
    global current_proxy_index
    
    if not proxy_list:
        return None
    
    with proxy_lock:
        # Подсчитываем количество активных прокси
        active_proxies = [p for p in proxy_list if proxy_stats[p]["active"]]
        
        # Если активных прокси нет, пробуем восстановить все
        if not active_proxies:
            print_message("Нет активных прокси. Восстанавливаем все прокси.", True)
            for proxy in proxy_list:
                proxy_stats[proxy]["active"] = True
            active_proxies = proxy_list
                
        # Если все еще нет активных прокси, возвращаем None
        if not active_proxies:
            return None
        
        # Находим прокси, который давно не использовался и активен
        current_time = time.time()
        selected_proxy = min(
            active_proxies, 
            key=lambda p: proxy_stats[p]["last_used"]
        )
        
        # Обновляем время последнего использования
        proxy_stats[selected_proxy]["last_used"] = current_time
        
        # Получаем лимитер для этого прокси
        limiter = proxy_limiters[selected_proxy]
        
        return selected_proxy

def update_proxy_stats(proxy, success=True, rate_limit_error=False, timeout=300):
    """Обновляет статистику использования прокси"""
    if not proxy or proxy not in proxy_stats:
        return
    
    with proxy_lock:
        if success:
            proxy_stats[proxy]["success"] += 1
        else:
            proxy_stats[proxy]["errors"] += 1
            
        if rate_limit_error:
            proxy_stats[proxy]["rate_limit_errors"] += 1
            
            # Если много ошибок с превышением лимита, деактивируем прокси на время
            if proxy_stats[proxy]["rate_limit_errors"] >= 5:
                proxy_stats[proxy]["active"] = False
                print_message(f"Прокси {proxy} временно деактивирован из-за частых ошибок превышения лимита")
                
                # Планируем реактивацию через некоторое время
                # Создаем таймер для восстановления прокси через указанное время
                def reactivate_proxy():
                    with proxy_lock:
                        if proxy in proxy_stats:
                            proxy_stats[proxy]["active"] = True
                            proxy_stats[proxy]["rate_limit_errors"] = 0
                            print_message(f"Прокси {proxy} снова активен")
                
                # Запускаем таймер в отдельном потоке
                timer_thread = threading.Timer(timeout, reactivate_proxy)
                timer_thread.daemon = True
                timer_thread.start()

def get_proxy_stats():
    """Возвращает статистику использования прокси-серверов для отображения"""
    with proxy_lock:
        total_success = sum(stats["success"] for stats in proxy_stats.values())
        total_errors = sum(stats["errors"] for stats in proxy_stats.values())
        active_count = sum(1 for stats in proxy_stats.values() if stats["active"])
        
        return {
            "total": len(proxy_list),
            "active": active_count,
            "inactive": len(proxy_list) - active_count,
            "total_success": total_success,
            "total_errors": total_errors
        }

def get_random_headers():
    """Генерирует случайные заголовки для запроса, имитирующие браузер"""
    user_agent = random.choice(USER_AGENTS)
    
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://tsouz.belgiss.by/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache"
    }
    
    return headers

# Класс ограничителя частоты запросов с адаптивным контролем
class AdaptiveRateLimiter:
    def __init__(self, initial_rate=1.0, max_rate=3.0, backoff_factor=0.5, recovery_factor=1.05):
        self.current_rate = initial_rate  # Текущее количество запросов в секунду
        self.max_rate = max_rate          # Максимальное количество запросов в секунду
        self.backoff_factor = backoff_factor  # Множитель для снижения скорости при ошибках
        self.recovery_factor = recovery_factor  # Множитель для постепенного восстановления скорости
        self.calls = []
        self.lock = threading.Lock()
        self.error_count = 0              # Счетчик последовательных ошибок
        self.success_count = 0            # Счетчик последовательных успехов
        self.last_adjustment_time = time.time()
        self.cooldown_period = 10.0       # Время в секундах между корректировками скорости
        self.min_rate = 0.1               # Минимальная скорость запросов

    def _cleanup_old_calls(self):
        current_time = time.time()
        self.calls = [t for t in self.calls if current_time - t < 1.0]

    def wait_for_permission(self):
        with self.lock:
            self._cleanup_old_calls()
            delay = 0
            
            # Рассчитываем время ожидания на основе текущей скорости
            if len(self.calls) >= self.current_rate:
                delay = max(0, (1.0 / self.current_rate) - (time.time() - self.calls[0]))
                
            # Добавляем случайность для избежания синхронных запросов
            jitter = random.uniform(0, 0.3)
            delay += jitter
            
            if delay > 0:
                time.sleep(delay)
                
            # Записываем время нового запроса
            self.calls.append(time.time())
            
            return delay

    def report_success(self):
        """Сообщаем о успешном запросе для корректировки скорости"""
        with self.lock:
            self.error_count = 0
            self.success_count += 1
            
            # Постепенно увеличиваем скорость, если достаточно успешных запросов
            current_time = time.time()
            if (self.success_count >= 25 and 
                current_time - self.last_adjustment_time >= self.cooldown_period and 
                self.current_rate < self.max_rate):
                self.current_rate = min(self.max_rate, self.current_rate * self.recovery_factor)
                self.last_adjustment_time = current_time
                self.success_count = 0
                return True  # Скорость была изменена
        return False

    def report_error(self, is_rate_limit_error=True):
        """Сообщаем об ошибке для корректировки скорости"""
        with self.lock:
            self.success_count = 0
            self.error_count += 1
            
            # Если это ошибка ограничения скорости, немедленно снижаем скорость
            if is_rate_limit_error:
                old_rate = self.current_rate
                self.current_rate = max(self.min_rate, self.current_rate * self.backoff_factor)
                # Если получили несколько ошибок подряд, снижаем скорость еще больше
                if self.error_count > 3:
                    self.current_rate = max(self.min_rate, self.current_rate * 0.7)
                self.last_adjustment_time = time.time()
                return old_rate != self.current_rate  # Скорость была изменена
        return False

    def get_rate(self):
        """Возвращает текущую скорость запросов"""
        with self.lock:
            return self.current_rate

# Функция для сохранения данных в JSON файл
def save_to_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# Функция для вывода сообщений в консоль
def print_message(message, is_error=False, log_only=True, important=False):
    # Всегда записываем в лог-файл
    if is_error:
        logger.error(message)
    else:
        logger.info(message)
    
    # Если это только для лога или не важное сообщение, не выводим в консоль
    if log_only and not important and not is_error:
        return
    
    # Не выводим сообщения в консоль, чтобы не нарушать прогресс-бар
    # Все сообщения теперь будут только в логе
    # Это исправляет проблему с нарушением структуры прогресс-бара
    # Только в случае если нам нужно явно что-то сообщить пользователю
    if important and not log_only:
        # Сначала очищаем текущие строки прогресс-бара
        print("\r" + " " * 150 + "\n" + " " * 150)
        
        # Выводим сообщение
        if is_error:
            print(f"\r{Fore.RED}{message}{Style.RESET_ALL}")
        else:
            print(f"\r{Fore.GREEN}{message}{Style.RESET_ALL}")
        
        # Даем время на прочтение и обновим прогресс-бар через секунду
        sys.stdout.flush()

# Функция для обновления статусной строки
def update_status_line(completed, total, success, errors, rate=None, proxy_info=None):
    global status_line, error_line
    
    # Очищаем предыдущую строку (два раза для гарантии очистки)
    print("\r" + " " * 150, end="\r")
    
    # Вычисляем проценты
    percent = (completed / total) * 100 if total > 0 else 0
    
    # Расчет оставшегося времени
    remaining_time = ""
    if rate and rate > 0:
        remaining_items = total - completed
        remaining_minutes = remaining_items / rate
        
        if remaining_minutes < 60:
            remaining_time = f" | Осталось: {remaining_minutes:.1f} мин"
        else:
            remaining_hours = remaining_minutes / 60
            remaining_time = f" | Осталось: {remaining_hours:.1f} ч"
    
    # Форматируем строку статуса
    status_line = f"Прогресс: {percent:.1f}% ({completed}/{total}) | Скорость: {rate:.1f} декл/мин{remaining_time}"
    if proxy_info:
        status_line += f" | Прокси: {proxy_info}"
    
    # Печатаем строку статуса
    print(f"\r{Fore.CYAN}{status_line}{Style.RESET_ALL}", end="")
    
    # Форматируем строку ошибок (на второй строке)
    error_percent = (errors / total) * 100 if total > 0 else 0
    success_percent = (success / completed) * 100 if completed > 0 else 0
    
    error_line = f"Ошибки: {error_percent:.1f}% ({errors}/{total}) | Успешно: {success} ({success_percent:.1f}%) | Неудачно: {errors}"
    
    # Если есть ошибки, показываем красным
    error_color = Fore.RED if errors > 0 else Fore.GREEN
    print(f"\n{error_color}{error_line}{Style.RESET_ALL}", end="")
    
    # Возвращаем курсор на основную строку статуса
    print("\033[1A", end="")
    
    # Убеждаемся, что вывод немедленно отображается
    sys.stdout.flush()

# Функция для выполнения запроса с повторными попытками и адаптивным контролем скорости
def make_request_with_retry(url, proxy=None, max_retries=5, initial_delay=2.0, proxy_timeout=300):
    """Выполняет запрос к API с контролем скорости и повторными попытками"""
    # Получаем случайные заголовки для имитации браузера
    headers = get_random_headers()
    
    proxies = None
    proxy_used = None
    
    if proxy:
        # Для SOCKS прокси
        if proxy.lower().startswith(('socks4://', 'socks5://')):
            proxies = {
                "http": proxy,
                "https": proxy
            }
        # Для HTTP/HTTPS прокси
        else:
            parts = proxy.split(':')
            # Если прокси содержит логин и пароль
            if len(parts) == 4:
                ip, port, username, password = parts
                proxy_auth = f"{username}:{password}@{ip}:{port}"
                proxies = {
                    "http": f"http://{proxy_auth}",
                    "https": f"http://{proxy_auth}"
                }
            # Если прокси без аутентификации
            elif len(parts) == 2:
                ip, port = parts
                proxies = {
                    "http": f"http://{ip}:{port}",
                    "https": f"http://{ip}:{port}"
                }
            # Если прокси уже содержит протокол
            else:
                if not proxy.startswith(('http://', 'https://')):
                    proxy = f"http://{proxy}"
                
                proxies = {
                    "http": proxy,
                    "https": proxy
                }
        
        proxy_used = proxy.split('@')[-1] if '@' in proxy else proxy
        print_message(f"Использую прокси: {proxy_used}", log_only=True)
    
    # Счетчик попыток
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            # Если это повторная попытка, добавляем задержку
            if attempt > 1:
                delay = initial_delay
                print_message(f"Повторная попытка {attempt}/{max_retries} через {delay:.1f} сек...", log_only=True)
                time.sleep(delay)
            
            # Выполняем запрос с фиксированным таймаутом 10 секунд
            response = requests.get(
                url, 
                headers=headers,
                proxies=proxies,
                verify=False,
                timeout=10  # Фиксированный таймаут 10 секунд
            )
            
            # Проверяем код статуса
            if response.status_code == 200:
                # Успешный запрос
                if proxy:
                    # Обновляем статистику прокси
                    update_proxy_stats(proxy, success=True)
                
                # Возвращаем JSON-данные
                try:
                    return response.json()
                except json.JSONDecodeError:
                    print_message(f"Ошибка декодирования JSON (статус 200)", is_error=True)
                    if proxy:
                        update_proxy_stats(proxy, success=False)
                    # Записываем ответ в файл для отладки
                    with open(f"error_response_{int(time.time())}.html", "w", encoding="utf-8") as f:
                        f.write(response.text)
                    continue
            
            # Обрабатываем различные коды статуса
            elif response.status_code == 429:
                # Превышение лимита запросов
                print_message(f"Превышен лимит запросов (HTTP 429)", is_error=True)
                if proxy:
                    update_proxy_stats(proxy, success=False, rate_limit_error=True)
                
                # Увеличиваем задержку для этого прокси
                time.sleep(5)  # Фиксированная задержка при 429
                continue
            
            elif response.status_code == 404:
                # Ресурс не найден
                print_message(f"Декларация не найдена (HTTP 404)", log_only=True)
                if proxy:
                    update_proxy_stats(proxy, success=True)  # Считаем успешным, т.к. это не ошибка прокси
                return None
            
            else:
                # Другие ошибки HTTP
                print_message(f"HTTP ошибка {response.status_code}: {response.reason}", is_error=True)
                if proxy:
                    update_proxy_stats(proxy, success=False)
        
        except requests.exceptions.Timeout:
            print_message(f"Таймаут запроса", is_error=True)
            if proxy:
                update_proxy_stats(proxy, success=False)
        
        except requests.exceptions.ConnectionError:
            proxy_info = f" через {proxy_used}" if proxy_used else ""
            print_message(f"Ошибка соединения{proxy_info}", is_error=True)
            if proxy:
                update_proxy_stats(proxy, success=False)
        
        except Exception as e:
            proxy_info = f" через {proxy_used}" if proxy_used else ""
            print_message(f"Непредвиденная ошибка{proxy_info}: {e}", is_error=True)
            if proxy:
                update_proxy_stats(proxy, success=False)
    
    # Если все попытки были неудачными
    print_message(f"Все попытки выполнить запрос были неудачными", is_error=True, log_only=True)
    return None

# Функция для загрузки деталей по одному ID
def download_declaration_details(declaration_id, batch_folder, limiter):
    url = f"{base_url}/{declaration_id}"
    
    # Выполняем запрос
    data = make_request_with_retry(url, limiter)
    
    if data is None:
        return {
            "id": declaration_id,
            "success": False,
            "error": "Не удалось получить данные после нескольких попыток"
        }
    
    # Сохраняем полученные данные
    filename = os.path.join(batch_folder, f"{declaration_id}.json")
    save_to_json(data, filename)
    
    return {
        "id": declaration_id,
        "success": True,
        "doc_id": data.get("DocId", "Unknown")
    }

# Функция для загрузки списка деклараций из файла
def load_declarations_from_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            items = data.get("items", [])
            return [item.get("certdecltr_id") for item in items if "certdecltr_id" in item]
    except Exception as e:
        print_message(f"Ошибка при загрузке файла {file_path}: {e}", True)
        return []

# Функция для сканирования директории и поиска всех JSON файлов
def scan_directory_for_json(directory):
    all_ids = set()
    
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            ids = load_declarations_from_json(file_path)
            all_ids.update(ids)
    
    return list(all_ids)

# Вспомогательная функция для форматирования времени
def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.0f} сек"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{minutes:.0f} мин {seconds:.0f} сек"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:.0f} ч {minutes:.0f} мин"

# Главная функция для загрузки деталей всех деклараций
def download_all_declaration_details(declaration_ids, workers=50, resume=False, batch_size=500, 
                                    initial_delay=2.0, max_retries=5, proxy_timeout=300):
    # Проверяем наличие прокси
    if not proxy_list:
        print_message("Для режима '1 прокси - 1 декларация' требуются прокси-серверы. Завершаем работу.", is_error=True, important=True)
        return 0
    
    # Определяем оптимальное количество рабочих потоков на основе доступных прокси
    # В режиме "1 прокси - 1 декларация" количество одновременных запросов = количеству прокси, но не более заданного максимума
    max_concurrent = min(len(proxy_list), workers)
    print_message(f"Одновременно будет обрабатываться до {max_concurrent} деклараций", important=True)
    
    # Создаем директорию для текущего батча
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_folder = os.path.join(output_dir, f"batch_{timestamp}")
    os.makedirs(batch_folder, exist_ok=True)
    
    # Показываем информацию о начале загрузки
    print_message("=" * 80, important=True)
    print_message("Инструмент для загрузки детальной информации о декларациях", important=True)
    print_message("Пакетная загрузка данных с API belgiss.by (режим: 1 прокси - 1 декларация)", important=True)
    print_message("=" * 80, important=True)
    
    print_message("\nНачало загрузки деталей деклараций...", important=True)
    print_message("ВНИМАНИЕ: Проверка SSL-сертификата отключена. Это может представлять риск безопасности.", is_error=True)
    
    # Выводим информацию о настройках только в лог-файл
    print_message(f"Доступно прокси: {len(proxy_list)}", log_only=True)
    print_message(f"Одновременно обрабатывается: {max_concurrent} деклараций", log_only=True)
    print_message(f"Таймаут запросов: 10 секунд", log_only=True)
    print_message(f"Пауза между циклами: 5 секунд", log_only=True)
    print_message(f"Максимум повторов: {max_retries}", log_only=True)
    
    # Проверяем, есть ли уже загруженные файлы, если режим продолжения
    completed_ids = set()
    if resume:
        # Проверяем существующие каталоги с батчами
        for dirname in sorted(os.listdir(output_dir), reverse=True):
            batch_dir = os.path.join(output_dir, dirname)
            if os.path.isdir(batch_dir) and dirname.startswith("batch_"):
                # Проверяем файлы JSON в этом каталоге
                for filename in os.listdir(batch_dir):
                    if filename.endswith(".json") and not filename == "download_report.json":
                        try:
                            id_str = filename.split(".")[0]
                            completed_ids.add(int(id_str))
                        except (ValueError, IndexError):
                            pass
                # Берем только из самого последнего батча, чтобы быстрее обработать
                if completed_ids:
                    print_message(f"Найдено {len(completed_ids)} уже загруженных деклараций в {batch_dir}", log_only=True)
                    break
    
    # Удаляем уже загруженные ID из списка
    if completed_ids:
        original_count = len(declaration_ids)
        declaration_ids = [id for id in declaration_ids if id not in completed_ids]
        print_message(f"Пропускаем {original_count - len(declaration_ids)} уже загруженных деклараций", important=True)
        print_message(f"Осталось загрузить: {len(declaration_ids)} деклараций", important=True)
    
    # Разбиваем данные на пакеты для регулярного сохранения прогресса
    total_count = len(declaration_ids)
    all_success_count = 0
    all_error_count = 0
    all_completed_count = 0
    
    # Инициализируем отображение статистики
    update_status_line(all_completed_count, total_count, all_success_count, all_error_count, rate=0.0)
    
    # Создаем блокировку для обновления статистики
    stats_lock = threading.Lock()
    
    # Создаем очередь задач для всех деклараций
    task_queue = queue.Queue()
    for declaration_id in declaration_ids:
        task_queue.put(declaration_id)
    
    # Создаем список для хранения результатов
    results = []
    
    # Функция для обновления статистики
    def update_stats(success=True):
        nonlocal all_completed_count, all_success_count, all_error_count
        with stats_lock:
            all_completed_count += 1
            if success:
                all_success_count += 1
            else:
                all_error_count += 1
            
            # Вычисляем скорость и оставшееся время
            elapsed = time.time() - start_time
            if all_completed_count > 0 and elapsed > 0:
                speed = all_completed_count / elapsed * 60  # декларации в минуту
                
                # Отображаем количество активных прокси
                active_proxies = sum(1 for p in proxy_stats.values() if p.get("active", False))
                proxy_info = f"{active_proxies}/{len(proxy_list)} активных"
                
                # Обновляем строку статуса
                update_status_line(all_completed_count, total_count, all_success_count, all_error_count, 
                                  rate=speed, proxy_info=proxy_info)
    
    # Основной цикл загрузки
    start_time = time.time()
    try:
        # Обрабатываем задачи пакетами, пока очередь не пуста
        while not task_queue.empty():
            batch_task_count = min(max_concurrent, task_queue.qsize())
            batch_tasks = []
            
            # Получаем следующий пакет задач
            for _ in range(batch_task_count):
                try:
                    declaration_id = task_queue.get(block=False)
                    batch_tasks.append(declaration_id)
                except queue.Empty:
                    break
            
            if not batch_tasks:
                break  # Если нет задач, завершаем цикл
            
            # Получаем уникальные прокси для каждой задачи
            batch_proxies = []
            with proxy_lock:
                # Подсчитываем количество активных прокси
                active_proxies = [p for p in proxy_list if proxy_stats[p]["active"]]
                if len(active_proxies) < len(batch_tasks):
                    print_message(f"Внимание: не хватает активных прокси ({len(active_proxies)}) для обработки пакета ({len(batch_tasks)})", log_only=True)
                    # Активируем все прокси, если их не хватает
                    for proxy in proxy_list:
                        proxy_stats[proxy]["active"] = True
                    active_proxies = proxy_list
                
                # Сортируем прокси по времени последнего использования
                sorted_proxies = sorted(active_proxies, key=lambda p: proxy_stats[p]["last_used"])
                
                # Выбираем необходимое количество прокси
                batch_proxies = sorted_proxies[:len(batch_tasks)]
                
                # Обновляем время использования для выбранных прокси
                current_time = time.time()
                for proxy in batch_proxies:
                    proxy_stats[proxy]["last_used"] = current_time
            
            # Создаем результаты для каждой задачи
            batch_results = [None] * len(batch_tasks)
            
            # Создаем и запускаем потоки для одновременной обработки задач
            threads = []
            
            def process_task(index, declaration_id, proxy):
                try:
                    # Загружаем декларацию используя выделенный прокси
                    url = f"{base_url}/{declaration_id}"
                    data = make_request_with_retry(
                        url, 
                        proxy=proxy, 
                        max_retries=max_retries, 
                        initial_delay=initial_delay,
                        proxy_timeout=proxy_timeout
                    )
                    
                    # Обрабатываем результат
                    if data is None:
                        result = {
                            "id": declaration_id,
                            "success": False,
                            "error": "Не удалось получить данные после нескольких попыток",
                            "proxy": proxy.split('@')[-1] if '@' in proxy else proxy
                        }
                        with stats_lock:
                            results.append(result)
                        update_stats(success=False)
                    else:
                        # Сохраняем полученные данные
                        filename = os.path.join(batch_folder, f"{declaration_id}.json")
                        save_to_json(data, filename)
                        
                        result = {
                            "id": declaration_id,
                            "success": True,
                            "doc_id": data.get("DocId", "Unknown"),
                            "proxy": proxy.split('@')[-1] if '@' in proxy else proxy
                        }
                        with stats_lock:
                            results.append(result)
                        update_stats(success=True)
                except Exception as e:
                    # В случае непредвиденной ошибки
                    logger.error(f"Ошибка при обработке декларации {declaration_id}: {e}")
                    with stats_lock:
                        results.append({
                            "id": declaration_id,
                            "success": False,
                            "error": f"Ошибка: {str(e)}",
                            "proxy": proxy.split('@')[-1] if '@' in proxy else proxy
                        })
                    update_stats(success=False)
            
            # Запускаем потоки для каждой пары задача-прокси
            for i, (declaration_id, proxy) in enumerate(zip(batch_tasks, batch_proxies)):
                thread = threading.Thread(target=process_task, args=(i, declaration_id, proxy))
                thread.daemon = True
                thread.start()
                threads.append(thread)
            
            # Ожидаем завершения всех потоков в пакете
            for thread in threads:
                thread.join()
            
            # Помечаем все задачи как выполненные
            for _ in batch_tasks:
                task_queue.task_done()
            
            # Делаем паузу между циклами запросов (5 секунд)
            if not task_queue.empty():
                print_message(f"Пауза 5 секунд между циклами запросов...", log_only=True)
                time.sleep(5)
        
        # Сохраняем отчет о загрузке
        report_file = os.path.join(batch_folder, "download_report.json")
        report_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_ids": total_count,
            "completed": all_completed_count,
            "success": all_success_count,
            "errors": all_error_count,
            "time_elapsed": time.time() - start_time,
            "results": results
        }
        save_to_json(report_data, report_file)
    
    except KeyboardInterrupt:
        print_message("\nПрервано пользователем. Останавливаем процесс...", is_error=True, important=True)
    except Exception as e:
        print_message(f"\nНепредвиденная ошибка в основном процессе: {e}", is_error=True, important=True)
    
    # Выводим общие итоги
    total_elapsed = time.time() - start_time
    
    # Выводим финальную статистику
    print_message("\n" + "=" * 80, important=True)
    print_message("РЕЗУЛЬТАТЫ ЗАГРУЗКИ ДЕКЛАРАЦИЙ", important=True)
    print_message("=" * 80, important=True)
    
    print_message(f"Всего обработано: {all_completed_count}/{total_count} ({all_completed_count/total_count*100:.1f}%)", important=True)
    print_message(f"Успешно загружено: {all_success_count} ({all_success_count/total_count*100:.1f}%)", important=True)
    print_message(f"Ошибок: {all_error_count} ({all_error_count/total_count*100:.1f}%)", important=True)
    
    if total_elapsed > 0:
        overall_speed = all_completed_count / total_elapsed * 60
        print_message(f"Общее время выполнения: {format_time(total_elapsed)}", important=True)
        print_message(f"Средняя скорость: {overall_speed:.1f} деклараций/мин", important=True)
    
    # Статистика по прокси
    if proxy_list:
        active_proxies = sum(1 for p in proxy_stats.values() if p.get("active", False))
        print_message(f"Активных прокси: {active_proxies}/{len(proxy_list)}", log_only=True)
        
        # Сохраняем работающие прокси
        save_working_proxies()
    
    print_message("=" * 80, important=True)
    
    # Возвращаем количество успешно загруженных деклараций
    return all_success_count

def save_working_proxies(output_file="working_proxies.txt"):
    """Сохраняет список рабочих прокси-серверов в файл на основе статистики использования"""
    if not proxy_list or not proxy_stats:
        print_message("Нет статистики по прокси-серверам для сохранения")
        return False
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# Список рабочих прокси-серверов\n")
            f.write("# Автоматически сгенерировано на основе статистики использования\n")
            f.write(f"# Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Сортируем прокси по успешности запросов
            sorted_proxies = sorted(
                [p for p in proxy_list if p in proxy_stats],
                key=lambda p: (
                    proxy_stats[p]["active"],  # Активные прокси в приоритете
                    proxy_stats[p]["success"],  # Затем по успешности
                    -proxy_stats[p]["errors"]   # Затем по минимальному количеству ошибок
                ),
                reverse=True
            )
            
            active_count = 0
            for proxy in sorted_proxies:
                stats = proxy_stats[proxy]
                
                # Добавляем только активные прокси с успешными запросами
                if stats["active"] and stats["success"] > 0:
                    f.write(f"{proxy}\n")
                    active_count += 1
            
            print_message(f"Сохранено {active_count} рабочих прокси-серверов в файл {output_file}")
            return True
    except Exception as e:
        print_message(f"Ошибка при сохранении рабочих прокси: {e}", True)
        return False

def test_proxies(proxies_file, timeout=10):
    """Тестирует прокси-серверы из файла и сохраняет рабочие в новый файл"""
    # Загружаем список прокси из файла
    load_proxies(proxies_file)
    if not proxy_list:
        print_message("Не удалось загрузить прокси из файла", True)
        return
    
    print_message(f"Тестирование {len(proxy_list)} прокси-серверов...")
    
    # Тестовый URL для проверки прокси
    test_url = "https://api.belgiss.by/"
    
    # Создаем прогресс-бар
    with tqdm(total=len(proxy_list), desc="Тестирование прокси") as progress_bar:
        # Тестируем каждый прокси
        for proxy in proxy_list:
            proxies = {"http": proxy, "https": proxy}
            success = False
            
            try:
                # Пытаемся выполнить запрос через прокси
                response = requests.get(
                    test_url,
                    proxies=proxies,
                    timeout=timeout,
                    verify=False,
                    headers=get_random_headers()
                )
                
                if response.status_code == 200:
                    success = True
                    update_proxy_stats(proxy, success=True)
                else:
                    update_proxy_stats(proxy, success=False)
            except Exception:
                update_proxy_stats(proxy, success=False)
            
            # Обновляем прогресс-бар
            progress_bar.update(1)
            progress_bar.set_postfix_str(f"Текущий: {proxy.split('@')[-1] if '@' in proxy else proxy}, Результат: {'OK' if success else 'FAIL'}")
    
    # Выводим результаты тестирования
    stats = get_proxy_stats()
    print_message("\nРезультаты тестирования:")
    print_message(f"- Всего прокси: {stats['total']}")
    print_message(f"- Рабочих прокси: {stats['total_success']}")
    print_message(f"- Нерабочих прокси: {stats['total_errors']}")
    
    # Сохраняем рабочие прокси в новый файл
    working_proxies_file = "working_" + os.path.basename(proxies_file)
    save_working_proxies(working_proxies_file)
    
    return working_proxies_file

def convert_proxy_file_to_utf8(input_file, output_file=None):
    """Конвертирует файл с прокси-серверами в кодировку UTF-8"""
    if not output_file:
        output_file = f"{os.path.splitext(input_file)[0]}_utf8.txt"
    
    # Список кодировок для попытки чтения файла
    encodings = ['utf-8', 'utf-16', 'utf-16-le', 'utf-16-be', 'cp1251', 'latin-1']
    
    for encoding in encodings:
        try:
            with open(input_file, 'r', encoding=encoding) as f:
                content = f.read()
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print_message(f"Файл успешно пересохранен в UTF-8: {output_file}")
            return output_file
        except UnicodeDecodeError:
            # Пробуем следующую кодировку
            continue
        except Exception as e:
            print_message(f"Ошибка при конвертации файла {input_file}: {e}", True)
    
    # Если не удалось прочитать файл ни в одной кодировке, пробуем бинарный режим
    try:
        with open(input_file, 'rb') as f:
            content = f.read()
        
        # Пробуем декодировать, игнорируя ошибки
        text = content.decode('utf-8', errors='ignore')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(text)
        
        print_message(f"Файл преобразован в UTF-8 (с игнорированием ошибок): {output_file}")
        return output_file
    except Exception as e:
        print_message(f"Ошибка при конвертации файла {input_file} в бинарном режиме: {e}", True)
    
    return None

if __name__ == "__main__":
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Скачивание детальной информации о декларациях с API')
    parser.add_argument('--source-dir', type=str, default="declarations_data", help='Директория с JSON-файлами списков деклараций (по умолчанию: declarations_data)')
    parser.add_argument('--workers', type=int, default=50, help='Максимальное количество одновременных запросов (по умолчанию: 50)')
    parser.add_argument('--ids', type=str, help='Список ID деклараций через запятую (если указан, директория не сканируется)')
    parser.add_argument('--limit', type=int, help='Ограничение количества деклараций для загрузки (для тестирования)')
    parser.add_argument('--resume', action='store_true', help='Продолжить загрузку с места остановки, пропуская уже загруженные декларации')
    parser.add_argument('--shuffle', action='store_true', help='Перемешать список ID для более равномерной загрузки')
    parser.add_argument('--batch-size', type=int, default=200, help='Размер пакета для сохранения промежуточных результатов (по умолчанию: 200)')
    parser.add_argument('--proxies', type=str, help='Путь к файлу со списком прокси-серверов (один прокси на строку)')
    parser.add_argument('--delay', type=float, default=2.0, help='Начальная задержка между запросами в секундах при ошибке (по умолчанию: 2.0)')
    parser.add_argument('--max-retries', type=int, default=3, help='Максимальное количество повторных попыток при ошибке (по умолчанию: 3)')
    parser.add_argument('--proxy-timeout', type=int, default=300, help='Время деактивации прокси после частых ошибок в секундах (по умолчанию: 300)')
    parser.add_argument('--disable-proxies', action='store_true', help='Отключить использование прокси даже если они загружены')
    parser.add_argument('--test-proxies', action='store_true', help='Протестировать прокси-серверы перед загрузкой данных')
    parser.add_argument('--test-timeout', type=int, default=10, help='Таймаут при тестировании прокси в секундах (по умолчанию: 10)')
    parser.add_argument('--save-working-proxies', action='store_true', help='Сохранить рабочие прокси в файл после завершения')
    parser.add_argument('--convert-proxy-file', action='store_true', help='Конвертировать файл с прокси-серверами в UTF-8 перед использованием')
    args = parser.parse_args()
    
    try:
        # Загружаем прокси-серверы, если указаны и не отключены явно
        if args.proxies and not args.disable_proxies:
            # Если указана опция конвертации, пересохраняем файл в UTF-8
            if args.convert_proxy_file:
                converted_file = convert_proxy_file_to_utf8(args.proxies)
                if converted_file:
                    args.proxies = converted_file
            
            load_proxies(args.proxies)
            
            # Тестируем прокси, если запрошено
            if args.test_proxies:
                print_message("Тестирование прокси-серверов перед началом загрузки...")
                working_proxies_file = test_proxies(args.proxies, args.test_timeout)
                if working_proxies_file:
                    print_message(f"Загружаем рабочие прокси из файла {working_proxies_file}")
                    load_proxies(working_proxies_file)
            
        # Определяем список ID деклараций для загрузки
        declaration_ids = []
        
        if args.ids:
            # Если ID указаны через командную строку
            declaration_ids = [int(id_str.strip()) for id_str in args.ids.split(',') if id_str.strip().isdigit()]
            print_message(f"Загружаем {len(declaration_ids)} деклараций по указанным ID")
        else:
            # Сканируем директорию с файлами деклараций
            print_message(f"Сканирование директории {args.source_dir} для поиска ID деклараций...")
            declaration_ids = scan_directory_for_json(args.source_dir)
            print_message(f"Найдено {len(declaration_ids)} ID деклараций для загрузки")
        
        # Перемешиваем список ID, если указан соответствующий параметр
        if args.shuffle:
            random.shuffle(declaration_ids)
            print_message("Список ID перемешан для более равномерной загрузки")
        
        # Применяем ограничение, если оно задано
        if args.limit and args.limit > 0 and args.limit < len(declaration_ids):
            declaration_ids = declaration_ids[:args.limit]
            print_message(f"Ограничение: будет загружено только {args.limit} деклараций")
        
        if not declaration_ids:
            print_message("Не найдено ID деклараций для загрузки! Проверьте директорию с файлами или укажите ID через параметр --ids.", True)
        else:
            download_all_declaration_details(
                declaration_ids, 
                workers=args.workers, 
                resume=args.resume,
                batch_size=args.batch_size,
                initial_delay=args.delay,
                max_retries=args.max_retries,
                proxy_timeout=args.proxy_timeout
            )
            
            # Сохраняем рабочие прокси в файл, если указан соответствующий параметр
            if args.save_working_proxies and proxy_list:
                working_proxies_file = f"working_proxies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                save_working_proxies(working_proxies_file)
            
    except KeyboardInterrupt:
        # Даже при прерывании сохраняем рабочие прокси, если включена опция
        if args.save_working_proxies and proxy_list:
            working_proxies_file = f"working_proxies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            save_working_proxies(working_proxies_file)
        print_message("\nПрограмма прервана пользователем!", True)
    except Exception as e:
        print_message(f"Ошибка при выполнении программы: {e}", True) 