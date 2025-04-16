#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import shutil

def check_requirements():
    """Проверяет наличие необходимых зависимостей"""
    try:
        import PyInstaller
        print("[✓] PyInstaller установлен")
    except ImportError:
        print("[!] PyInstaller не найден. Устанавливаем...")
        subprocess.call([sys.executable, "-m", "pip", "install", "pyinstaller"])
        print("[✓] PyInstaller установлен")

    # Проверяем наличие рабочих файлов
    required_files = ['declarations_downloader_interactive.py', 'declarations_downloader.py']
    for file in required_files:
        if not os.path.exists(file):
            print(f"[✗] Ошибка: файл {file} не найден!")
            return False
    
    print("[✓] Все необходимые файлы найдены")
    return True

def build_executable():
    """Собирает исполняемый файл"""
    print("\n=== Начинаем сборку исполняемого файла ===\n")
    
    # Создаем список зависимостей для включения в сборку
    hidden_imports = [
        'requests', 'urllib3', 'certifi', 'chardet', 'idna',
        'argparse', 'datetime', 'logging', 'os', 'sys', 're',
        'json', 'uuid', 'traceback', 'time', 'concurrent.futures',
        'tkinter', 'tkinter.filedialog', 'colorama', 'tqdm',
        'importlib', 'importlib.util', 'importlib.machinery',
        'subprocess', 'math', 'random'
    ]
    
    # Формируем команду для PyInstaller
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--clean",
        "--onefile",
        "--name", "belgiss_downloader"
    ]
    
    # Добавляем скрытые импорты
    for module in hidden_imports:
        cmd.extend(["--hidden-import", module])
    
    # Добавляем data-файлы
    cmd.extend(["--add-data", f"declarations_downloader.py{os.pathsep}."]) 
    
    # Добавляем главный файл
    cmd.append("declarations_downloader_interactive.py")
    
    print(f"Выполняем команду: {' '.join(cmd)}")
    
    try:
        # Запускаем процесс сборки
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Выводим вывод в реальном времени
        for line in process.stdout:
            print(line.strip())
        
        # Ждем завершения процесса
        process.wait()
        
        if process.returncode == 0:
            print("\n[✓] Сборка успешно завершена!")
            
            # Проверяем, создался ли исполняемый файл
            exe_path = os.path.join("dist", "belgiss_downloader.exe")
            if os.path.exists(exe_path):
                size_mb = os.path.getsize(exe_path) / (1024 * 1024)
                print(f"[✓] Создан файл: {exe_path} (размер: {size_mb:.2f} МБ)")
                
                # Копируем EXE-файл в текущую директорию для удобства
                dest_path = "belgiss_downloader.exe"
                shutil.copy2(exe_path, dest_path)
                print(f"[✓] Файл скопирован в текущую директорию: {dest_path}")
            else:
                print(f"[!] Предупреждение: файл {exe_path} не найден!")
                
            return True
        else:
            print("\n[✗] Ошибка при сборке!")
            return False
            
    except Exception as e:
        print(f"\n[✗] Ошибка при запуске PyInstaller: {e}")
        return False

def main():
    print("=" * 70)
    print("      СБОРЩИК BELGISS PARSER В ИСПОЛНЯЕМЫЙ ФАЙЛ      ")
    print("=" * 70)
    print()
    
    # Проверяем требования
    if not check_requirements():
        print("\n[✗] Не удалось выполнить предварительные проверки.")
        input("\nНажмите Enter для выхода...")
        return
    
    # Собираем исполняемый файл
    success = build_executable()
    
    if success:
        print("\n" + "=" * 70)
        print("    СБОРКА УСПЕШНО ЗАВЕРШЕНА    ")
        print("=" * 70)
        print("\nИсполняемый файл создан: belgiss_downloader.exe")
        print("Теперь вы можете запустить его двойным щелчком мыши.")
        print("\nПримечание: при первом запуске Windows может показать предупреждение")
        print("безопасности. Это нормально, так как файл не имеет цифровой подписи.")
    else:
        print("\n" + "=" * 70)
        print("    СБОРКА ЗАВЕРШИЛАСЬ С ОШИБКАМИ    ")
        print("=" * 70)
        print("\nПроверьте сообщения об ошибках выше и повторите попытку.")
        
    input("\nНажмите Enter для выхода...")

if __name__ == "__main__":
    main() 