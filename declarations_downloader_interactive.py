import os
import sys
import subprocess
import tkinter as tk
from tkinter import filedialog
from datetime import datetime
import re
from colorama import init, Fore, Back, Style
import logging
import traceback
import uuid
import importlib.util
import importlib.machinery

# Настройка логирования
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

# Создаем уникальный ID сессии, чтобы отличать запуски
SESSION_ID = str(uuid.uuid4())[:8]

# Настраиваем логгер
logging.basicConfig(
    filename=os.path.join(log_dir, f"interactive_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{SESSION_ID}.log"),
    level=logging.DEBUG,
    format='%(asctime)s - [%(levelname)s] - %(message)s'
)

# Добавляем вывод логов в консоль
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# Функция для логирования и вывода сообщений
def log_info(message):
    logging.info(message)

def log_error(message):
    logging.error(message)

def log_debug(message):
    logging.debug(message)

def log_warning(message):
    logging.warning(message)

# Инициализация colorama для поддержки цветного текста в Windows
init(autoreset=True)

# Скрываем основное окно tkinter (для диалога выбора файла)
try:
    root = tk.Tk()
    root.withdraw()
    log_info("Tkinter успешно инициализирован и скрыт")
except Exception as e:
    log_error(f"Ошибка при инициализации Tkinter: {e}")
    log_error(traceback.format_exc())

# Флаг для предотвращения повторного запуска
EXECUTION_TRACKER = f"._execution_tracker_{SESSION_ID}"

def clear_screen():
    """Очистка экрана терминала"""
    try:
        os.system('cls' if os.name == 'nt' else 'clear')
        log_debug("Экран очищен")
    except Exception as e:
        log_error(f"Ошибка при очистке экрана: {e}")

def print_header():
    """Выводит заголовок приложения"""
    log_debug("Вывод заголовка")
    clear_screen()
    print(Fore.CYAN + "=" * 80 + Style.RESET_ALL)
    print(Fore.CYAN + " " * 20 + Fore.YELLOW + Style.BRIGHT + "ЗАГРУЗКА ДЕКЛАРАЦИЙ С API БЕЛГИСС" + Fore.CYAN + " " * 20 + Style.RESET_ALL)
    print(Fore.CYAN + "=" * 80 + Style.RESET_ALL)
    print("\n")

def print_footer():
    """Выводит подвал приложения"""
    log_debug("Вывод подвала")
    print(Fore.CYAN + "=" * 80 + Style.RESET_ALL)
    print(Fore.CYAN + "=" * 80 + Style.RESET_ALL)

def validate_date(date_str):
    """Проверяет корректность формата даты и преобразует в ДД.ММ.ГГГГ"""
    log_debug(f"Проверка даты: {date_str}")
    
    # Поддерживаемые форматы разделителей
    date_patterns = [
        r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$",  # DD.MM.YYYY
        r"^(\d{1,2})/(\d{1,2})/(\d{4})$",    # DD/MM/YYYY
        r"^(\d{1,2})-(\d{1,2})-(\d{4})$",    # DD-MM-YYYY
        r"^(\d{4})\.(\d{1,2})\.(\d{1,2})$",  # YYYY.MM.DD
        r"^(\d{4})/(\d{1,2})/(\d{1,2})$",    # YYYY/MM/DD
        r"^(\d{4})-(\d{1,2})-(\d{1,2})$"     # YYYY-MM-DD
    ]
    
    # Проверяем входную строку
    if not date_str or len(date_str) < 8:
        log_error(f"Дата слишком короткая: '{date_str}'")
        return False, "Дата должна быть в формате ДД.ММ.ГГГГ"
    
    for pattern in date_patterns:
        match = re.match(pattern, date_str)
        if match:
            log_debug(f"Найдено соответствие шаблону: {pattern}")
            groups = match.groups()
            
            # Определяем, какой формат использован
            if len(groups[0]) == 4:  # YYYY-MM-DD формат
                year, month, day = groups
                log_debug(f"Формат YYYY-MM-DD: год={year}, месяц={month}, день={day}")
            else:  # DD-MM-YYYY формат
                day, month, year = groups
                log_debug(f"Формат DD-MM-YYYY: день={day}, месяц={month}, год={year}")
            
            # Преобразуем в числа
            try:
                day = int(day)
                month = int(month)
                year = int(year)
                
                log_debug(f"Преобразовано в числа: день={day}, месяц={month}, год={year}")
                
                # Проверяем диапазоны
                if day < 1 or day > 31 or month < 1 or month > 12 or year < 1900 or year > 2100:
                    log_error(f"Значения вне допустимого диапазона: день={day}, месяц={month}, год={year}")
                    return False, "Значения дня, месяца или года находятся вне допустимого диапазона"
                
                # Проверяем валидность даты, создав объект datetime
                try:
                    date_obj = datetime(year, month, day)
                    log_debug(f"Создан объект datetime: {date_obj}")
                    
                    # Форматируем дату в нужный формат ДД.ММ.ГГГГ
                    formatted_date = f"{day:02d}.{month:02d}.{year}"
                    log_info(f"Дата успешно валидирована и форматирована: {formatted_date}")
                    return True, formatted_date
                except ValueError as e:
                    log_error(f"Ошибка создания datetime: {e}")
                    return False, f"Некорректная дата: {e}"
                
            except ValueError as e:
                log_error(f"Ошибка преобразования в числа: {e}")
                continue
    
    log_error(f"Дата не соответствует ни одному шаблону: {date_str}")
    return False, "Неверный формат даты. Используйте форматы ДД.ММ.ГГГГ, ММ/ДД/ГГГГ, ДД-ММ-ГГГГ и т.п."

def find_declarations_downloader():
    """Находит файл declarations_downloader.py и возвращает его путь"""
    log_info("Поиск declarations_downloader.py")
    
    # Сначала ищем в текущей директории
    script_path = "declarations_downloader.py"
    if os.path.exists(script_path):
        log_info(f"Найден файл declarations_downloader.py в текущей директории")
        return script_path
    
    # Ищем в директории, где находится текущий скрипт
    current_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(current_dir, "declarations_downloader.py")
    if os.path.exists(script_path):
        log_info(f"Найден файл declarations_downloader.py в директории {current_dir}")
        return script_path
    
    # Ищем в ресурсах PyInstaller
    if getattr(sys, 'frozen', False):
        bundle_dir = getattr(sys, '_MEIPASS', os.path.abspath(os.path.dirname(__file__)))
        script_path = os.path.join(bundle_dir, "declarations_downloader.py")
        
        if os.path.exists(script_path):
            log_info(f"Найден файл declarations_downloader.py в PyInstaller bundle: {script_path}")
            return script_path
    
    # Если не найдено, пытаемся найти как модуль
    try:
        spec = importlib.util.find_spec("declarations_downloader")
        if spec is not None:
            log_info(f"Найден модуль declarations_downloader: {spec.origin}")
            return spec.origin
    except:
        pass
        
    # Дополнительные места для поиска
    additional_paths = [
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "declarations_downloader.py"),
        os.path.join(os.getcwd(), "declarations_downloader.py"),
        os.path.normpath(os.path.join(os.path.dirname(sys.executable), "declarations_downloader.py")),
    ]
    
    for path in additional_paths:
        if os.path.exists(path):
            log_info(f"Найден файл declarations_downloader.py по пути: {path}")
            return path
    
    log_error("Файл declarations_downloader.py не найден")
    return None

def run_declarations_downloader(args):
    """Запускает загрузку деклараций"""
    log_info(f"Запуск загрузки деклараций с аргументами: {args}")
    
    # Поиск скрипта declarations_downloader.py
    script_path = find_declarations_downloader()
    
    if script_path is None:
        log_error("Скрипт declarations_downloader.py не найден")
        print(Fore.RED + "\nОШИБКА: Скрипт declarations_downloader.py не найден!" + Style.RESET_ALL)
        print(Fore.RED + "Проверьте, что файл declarations_downloader.py находится рядом с текущим скриптом." + Style.RESET_ALL)
        return False
    
    log_info(f"Используем скрипт по пути: {script_path}")
    
    try:
        # Загружаем модуль
        spec = importlib.util.spec_from_file_location("declarations_downloader", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Сохраняем оригинальные аргументы командной строки
        original_argv = sys.argv
        # Подставляем наши аргументы
        sys.argv = ["declarations_downloader.py"] + args
        
        log_info(f"Запуск модуля declarations_downloader с аргументами: {sys.argv}")
        
        # Запускаем функцию main из модуля
        result = module.main()
        
        # Восстанавливаем оригинальные аргументы
        sys.argv = original_argv
        
        log_info("Модуль declarations_downloader успешно выполнен")
        return result > 0  # Возвращаем True, если загружена хотя бы одна декларация
        
    except Exception as e:
        log_error(f"Ошибка при импорте или выполнении модуля: {e}")
        log_error(traceback.format_exc())
        
        # Если не получилось импортировать, запускаем как внешний процесс
        try:
            command = [sys.executable, script_path] + args
            log_info(f"Запускаем как внешний процесс: {' '.join(command)}")
            result = subprocess.call(command)
            log_info(f"Процесс завершен с кодом возврата: {result}")
            return result == 0
        except Exception as e2:
            log_error(f"Ошибка при запуске как внешнего процесса: {e2}")
            log_error(traceback.format_exc())
            print(Fore.RED + f"\nОшибка при запуске declarations_downloader: {e2}" + Style.RESET_ALL)
            return False

def main():
    log_info(f"====== ЗАПУСК ПРОГРАММЫ (Сессия: {SESSION_ID}) ======")
    
    # Проверка на повторный запуск
    if os.path.exists(EXECUTION_TRACKER):
        log_warning(f"Обнаружен файл-трекер {EXECUTION_TRACKER}, возможно повторный запуск")
        try:
            os.remove(EXECUTION_TRACKER)
            log_info(f"Файл-трекер {EXECUTION_TRACKER} удален")
        except Exception as e:
            log_error(f"Ошибка при удалении файла-трекера: {e}")
    else:
        # Создаем метку выполнения
        try:
            with open(EXECUTION_TRACKER, 'w') as f:
                now = datetime.now()
                f.write(str(now))
                log_info(f"Файл-трекер {EXECUTION_TRACKER} создан в {now}")
        except Exception as e:
            log_error(f"Ошибка при создании файла-трекера: {e}")

    try:
        print_header()
        
        # Шаг 1: Запрос о наличии прокси
        log_info("Шаг 1: Запрос о наличии прокси")
        proxy_file = None
        while True:
            print(Fore.GREEN + "Есть ли у вас файл с прокси?" + Style.RESET_ALL)
            print(Fore.YELLOW + "1. " + Fore.WHITE + "Да")
            print(Fore.YELLOW + "2. " + Fore.WHITE + "Нет")
            log_debug("Ожидание ввода пользователя (1/2)")
            
            try:
                proxy_choice = input(Fore.CYAN + "\nВаш выбор (1/2): " + Style.RESET_ALL).strip()
                log_debug(f"Получен ввод: '{proxy_choice}'")
                
                if proxy_choice == "1":
                    log_info("Пользователь выбрал использование прокси-файла")
                    print(Fore.WHITE + "\nПожалуйста, выберите файл с прокси в открывшемся окне..." + Style.RESET_ALL)
                    
                    try:
                        proxy_file = filedialog.askopenfilename(
                            title="Выберите файл с прокси",
                            filetypes=[("Текстовые файлы", "*.txt"), ("Все файлы", "*.*")]
                        )
                        log_debug(f"Выбор файла: '{proxy_file}'")
                        
                        if not proxy_file:
                            log_warning("Файл не был выбран")
                            print(Fore.RED + "\nФайл не выбран. Попробуйте снова.\n" + Style.RESET_ALL)
                            continue
                        
                        log_info(f"Выбран файл с прокси: {proxy_file}")
                        print(Fore.GREEN + f"\nВыбран файл: {proxy_file}" + Style.RESET_ALL)
                        break
                    except Exception as e:
                        log_error(f"Ошибка при выборе файла: {e}")
                        log_error(traceback.format_exc())
                        print(Fore.RED + f"\nОшибка при выборе файла: {e}\n" + Style.RESET_ALL)
                        continue
                        
                elif proxy_choice == "2":
                    log_info("Пользователь выбрал работу без прокси")
                    print(Fore.YELLOW + "\nПродолжаем без использования прокси." + Style.RESET_ALL)
                    break
                else:
                    log_warning(f"Некорректный ввод: '{proxy_choice}'")
                    print(Fore.RED + "\nНеверный выбор. Пожалуйста, введите 1 или 2.\n" + Style.RESET_ALL)
            except Exception as e:
                log_error(f"Ошибка при обработке ввода: {e}")
                log_error(traceback.format_exc())
                print(Fore.RED + f"\nОшибка при вводе: {e}" + Style.RESET_ALL)
        
        # Шаг 2: Запрос диапазона дат
        log_info("Шаг 2: Запрос диапазона дат")
        print_header()
        print(Fore.GREEN + "Укажите диапазон дат для парсинга деклараций:" + Style.RESET_ALL)
        
        # Запрос даты "ОТ"
        log_info("Запрос даты 'ОТ'")
        date_from = None
        date_from_attempts = 0
        
        while not date_from:
            date_from_attempts += 1
            log_debug(f"Попытка ввода даты 'ОТ' №{date_from_attempts}")
            
            try:
                date_from_input = input(Fore.CYAN + "\nДата ОТ (ДД.ММ.ГГГГ): " + Style.RESET_ALL).strip()
                log_debug(f"Получен ввод даты 'ОТ': '{date_from_input}'")
                
                is_valid, result = validate_date(date_from_input)
                if is_valid:
                    date_from = result
                    log_info(f"Установлена дата 'ОТ': {date_from}")
                    print(Fore.GREEN + f"Установлена дата: {date_from}" + Style.RESET_ALL)
                else:
                    log_warning(f"Некорректный ввод даты 'ОТ': {result}")
                    print(Fore.RED + result + Style.RESET_ALL)
                    
                # Защита от бесконечного цикла
                if date_from_attempts >= 10:
                    log_error(f"Превышен лимит попыток ввода даты 'ОТ' ({date_from_attempts})")
                    print(Fore.RED + "\nПревышен лимит попыток ввода. Используем текущую дату." + Style.RESET_ALL)
                    now = datetime.now()
                    date_from = now.strftime("%d.%m.%Y")
                    log_info(f"Установлена текущая дата 'ОТ' по умолчанию: {date_from}")
                    print(Fore.YELLOW + f"Установлена текущая дата: {date_from}" + Style.RESET_ALL)
            except Exception as e:
                log_error(f"Ошибка при вводе даты 'ОТ': {e}")
                log_error(traceback.format_exc())
                print(Fore.RED + f"Ошибка: {e}" + Style.RESET_ALL)
                
                # Защита от бесконечного цикла при ошибке
                if date_from_attempts >= 5:
                    now = datetime.now()
                    date_from = now.strftime("%d.%m.%Y")
                    log_info(f"Установлена текущая дата 'ОТ' после ошибки: {date_from}")
                    print(Fore.YELLOW + f"Автоматически установлена текущая дата: {date_from}" + Style.RESET_ALL)
        
        # Запрос даты "ДО"
        log_info("Запрос даты 'ДО'")
        date_to = None
        date_to_attempts = 0
        
        while not date_to:
            date_to_attempts += 1
            log_debug(f"Попытка ввода даты 'ДО' №{date_to_attempts}")
            
            try:
                date_to_input = input(Fore.CYAN + "\nДата ДО (ДД.ММ.ГГГГ): " + Style.RESET_ALL).strip()
                log_debug(f"Получен ввод даты 'ДО': '{date_to_input}'")
                
                is_valid, result = validate_date(date_to_input)
                if is_valid:
                    date_to = result
                    log_info(f"Установлена дата 'ДО': {date_to}")
                    print(Fore.GREEN + f"Установлена дата: {date_to}" + Style.RESET_ALL)
                else:
                    log_warning(f"Некорректный ввод даты 'ДО': {result}")
                    print(Fore.RED + result + Style.RESET_ALL)
                    
                # Защита от бесконечного цикла
                if date_to_attempts >= 10:
                    log_error(f"Превышен лимит попыток ввода даты 'ДО' ({date_to_attempts})")
                    print(Fore.RED + "\nПревышен лимит попыток ввода. Используем текущую дату +1 день." + Style.RESET_ALL)
                    now = datetime.now()
                    tomorrow = now.replace(day=now.day+1)
                    date_to = tomorrow.strftime("%d.%m.%Y")
                    log_info(f"Установлена завтрашняя дата 'ДО' по умолчанию: {date_to}")
                    print(Fore.YELLOW + f"Установлена дата: {date_to}" + Style.RESET_ALL)
            except Exception as e:
                log_error(f"Ошибка при вводе даты 'ДО': {e}")
                log_error(traceback.format_exc())
                print(Fore.RED + f"Ошибка: {e}" + Style.RESET_ALL)
                
                # Защита от бесконечного цикла при ошибке
                if date_to_attempts >= 5:
                    now = datetime.now()
                    tomorrow = now.replace(day=now.day+1)
                    date_to = tomorrow.strftime("%d.%m.%Y")
                    log_info(f"Установлена завтрашняя дата 'ДО' после ошибки: {date_to}")
                    print(Fore.YELLOW + f"Автоматически установлена завтрашняя дата: {date_to}" + Style.RESET_ALL)
        
        # Формируем аргументы для запуска основного скрипта
        log_info("Формирование аргументов для запуска основного скрипта")
        args = ["--workers", "3", "--date-from", date_from, "--date-to", date_to]
        
        if proxy_file:
            args.extend(["--proxies", proxy_file])
            log_debug(f"Добавлен прокси-файл: {proxy_file}")
        
        log_info(f"Итоговые аргументы: {' '.join(args)}")
        
        # Выводим информацию о запуске
        print("\n" + Fore.CYAN + "=" * 80 + Style.RESET_ALL)
        print(Fore.GREEN + "Запуск загрузки деклараций со следующими параметрами:" + Style.RESET_ALL)
        print(Fore.WHITE + f"Диапазон дат: с {date_from} по {date_to}" + Style.RESET_ALL)
        print(Fore.WHITE + f"Использование прокси: {'Да' if proxy_file else 'Нет'}" + Style.RESET_ALL)
        if proxy_file:
            print(Fore.WHITE + f"Файл с прокси: {proxy_file}" + Style.RESET_ALL)
        print(Fore.CYAN + "=" * 80 + Style.RESET_ALL + "\n")
        
        # Запуск загрузки деклараций
        log_info("Запуск процесса загрузки деклараций")
        print(Fore.YELLOW + "Запуск процесса загрузки деклараций...\n" + Style.RESET_ALL)
        
        # Запускаем загрузку деклараций
        success = run_declarations_downloader(args)
        
        if success:
            log_info("Загрузка деклараций успешно завершена")
            print("\n" + Fore.CYAN + "=" * 80 + Style.RESET_ALL)
            print(Fore.GREEN + Style.BRIGHT + "Загрузка деклараций успешно завершена!" + Style.RESET_ALL)
            print(Fore.CYAN + "=" * 80 + Style.RESET_ALL + "\n")
        else:
            log_error("Ошибка при загрузке деклараций")
            print("\n" + Fore.CYAN + "=" * 80 + Style.RESET_ALL)
            print(Fore.RED + Style.BRIGHT + "Ошибка при загрузке деклараций!" + Style.RESET_ALL)
            print(Fore.CYAN + "=" * 80 + Style.RESET_ALL + "\n")
                
        # Удаляем метку выполнения
        try:
            if os.path.exists(EXECUTION_TRACKER):
                os.remove(EXECUTION_TRACKER)
                log_info(f"Файл-трекер {EXECUTION_TRACKER} удален")
            else:
                log_warning(f"Файл-трекер {EXECUTION_TRACKER} не найден для удаления")
        except Exception as e:
            log_error(f"Ошибка при удалении файла-трекера: {e}")
        
        # Ожидаем нажатия Enter перед закрытием
        log_info("Ожидание нажатия Enter для завершения")
        input(Fore.YELLOW + "\nНажмите Enter для завершения работы..." + Style.RESET_ALL)
        
        # Явный выход из программы
        log_info(f"====== ЗАВЕРШЕНИЕ ПРОГРАММЫ (Сессия: {SESSION_ID}) ======")
        sys.exit(0)
        
    except Exception as e:
        log_error(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        log_error(traceback.format_exc())
        print(Fore.RED + f"\nКРИТИЧЕСКАЯ ОШИБКА: {e}" + Style.RESET_ALL)
        
        # Удаляем метку выполнения в случае ошибки
        try:
            if os.path.exists(EXECUTION_TRACKER):
                os.remove(EXECUTION_TRACKER)
        except:
            pass
        
        # Ожидаем нажатия Enter перед закрытием
        input(Fore.RED + "\nНажмите Enter для завершения работы..." + Style.RESET_ALL)
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.critical(f"Необработанное исключение: {e}")
        logging.critical(traceback.format_exc())
        print(Fore.RED + f"КРИТИЧЕСКАЯ ОШИБКА: {e}" + Style.RESET_ALL)
        input("\nНажмите Enter для завершения работы...")
        sys.exit(1) 