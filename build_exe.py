import os
import subprocess
import sys
import shutil
from colorama import init, Fore, Style

# Инициализация colorama
init(autoreset=True)

def check_pyinstaller():
    """Проверяет, установлен ли PyInstaller, и устанавливает его при необходимости"""
    try:
        import PyInstaller
        print(Fore.GREEN + "PyInstaller уже установлен")
        return True
    except ImportError:
        print(Fore.YELLOW + "PyInstaller не найден. Выполняется установка...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])
            print(Fore.GREEN + "PyInstaller успешно установлен")
            return True
        except Exception as e:
            print(Fore.RED + f"Ошибка при установке PyInstaller: {e}")
            return False

def check_dependencies():
    """Проверяет наличие всех необходимых зависимостей"""
    dependencies = [
        "pandas",
        "openpyxl",
        "colorama",
        "tqdm",
        "requests",
        "urllib3"
    ]
    
    missing = []
    
    for dep in dependencies:
        try:
            __import__(dep)
        except ImportError:
            missing.append(dep)
    
    if missing:
        print(Fore.YELLOW + f"Отсутствуют следующие зависимости: {', '.join(missing)}")
        print(Fore.YELLOW + "Установка недостающих зависимостей...")
        
        for dep in missing:
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", dep])
                print(Fore.GREEN + f"Установлена зависимость: {dep}")
            except Exception as e:
                print(Fore.RED + f"Ошибка при установке {dep}: {e}")
                return False
    
    print(Fore.GREEN + "Все зависимости установлены")
    return True

def build_exe():
    """Собирает EXE-файл с помощью PyInstaller"""
    print(Fore.CYAN + "=" * 80)
    print(Fore.CYAN + " " * 20 + Fore.YELLOW + Style.BRIGHT + "СБОРКА EXE-ФАЙЛА" + Fore.CYAN + " " * 20)
    print(Fore.CYAN + "=" * 80)
    
    # Проверяем наличие PyInstaller и зависимостей
    if not check_pyinstaller() or not check_dependencies():
        print(Fore.RED + "Не удалось установить необходимые компоненты для сборки")
        return False
    
    # Проверяем наличие исходного файла
    source_file = "declaration_details_excel_interactive.py"
    if not os.path.exists(source_file):
        print(Fore.RED + f"Исходный файл {source_file} не найден")
        return False
    
    print(Fore.GREEN + f"Исходный файл {source_file} найден")
    
    # Создаем директорию для вывода, если ее нет
    output_dir = "dist"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Имя исполняемого файла
    exe_name = "DeclarationExcelGenerator"
    
    # Опции для PyInstaller
    options = [
        "--name=" + exe_name,
        "--onefile",  # Собираем все в один файл
        "--icon=app_icon.ico" if os.path.exists("app_icon.ico") else "",  # Добавляем иконку, если есть
        "--clean",  # Очищаем временные файлы перед сборкой
        source_file  # Исходный файл
    ]
    
    # Удаляем пустые опции
    options = [opt for opt in options if opt]
    
    print(Fore.YELLOW + "Запуск сборки с PyInstaller...")
    print(Fore.YELLOW + "Это может занять несколько минут...")
    
    try:
        # Запускаем PyInstaller
        subprocess.check_call([sys.executable, "-m", "PyInstaller"] + options)
        
        print(Fore.GREEN + "Сборка успешно завершена!")
        exe_path = os.path.join("dist", exe_name + ".exe")
        
        if os.path.exists(exe_path):
            print(Fore.GREEN + f"Исполняемый файл создан: {exe_path}")
            
            # Копируем EXE-файл в текущую директорию для удобства
            shutil.copy(exe_path, ".")
            print(Fore.GREEN + f"Копия исполняемого файла создана в текущей директории: {exe_name}.exe")
            
            return True
        else:
            print(Fore.RED + "Исполняемый файл не найден после сборки")
            return False
        
    except Exception as e:
        print(Fore.RED + f"Ошибка при сборке: {e}")
        return False

if __name__ == "__main__":
    try:
        if build_exe():
            print(Fore.GREEN + "Сборка успешно завершена!")
            print(Fore.YELLOW + "Нажмите Enter для выхода...")
        else:
            print(Fore.RED + "Сборка завершилась с ошибками")
            print(Fore.YELLOW + "Нажмите Enter для выхода...")
        
        input()
    except KeyboardInterrupt:
        print(Fore.YELLOW + "\nСборка прервана пользователем")
        sys.exit(0)
    except Exception as e:
        print(Fore.RED + f"Непредвиденная ошибка: {e}")
        print(Fore.YELLOW + "Нажмите Enter для выхода...")
        input() 