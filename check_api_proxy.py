import requests
import urllib3
import time
import sys
import os
import socket
from tqdm import tqdm

# Отключаем предупреждения о небезопасных SSL соединениях
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def load_proxies(proxy_file):
    """Загружает прокси из файла"""
    if not os.path.exists(proxy_file):
        print(f"[ОШИБКА] Файл прокси {proxy_file} не найден")
        return []
    
    try:
        # Список кодировок для попытки чтения файла
        encodings = ['utf-8', 'utf-16', 'utf-16-le', 'utf-16-be', 'cp1251', 'latin-1']
        
        for encoding in encodings:
            try:
                with open(proxy_file, 'r', encoding=encoding) as f:
                    proxies = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                print(f"Успешно загружено {len(proxies)} прокси из файла {proxy_file} (кодировка: {encoding})")
                
                # Выводим первые 5 прокси для проверки
                if len(proxies) > 0:
                    print("Примеры загруженных прокси:")
                    for i, proxy in enumerate(proxies[:5]):
                        print(f"  {i+1}. {proxy}")
                
                return proxies
            except UnicodeDecodeError:
                continue
        
        print(f"[ОШИБКА] Не удалось прочитать файл {proxy_file} ни в одной из известных кодировок")
        return []
    except Exception as e:
        print(f"[ОШИБКА] Ошибка при загрузке прокси: {e}")
        return []

def check_proxy(proxy, target_url, timeout=10, user_agent=None):
    """Проверяет работу прокси для доступа к целевому URL"""
    headers = {
        "User-Agent": user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    # Определяем тип прокси по префиксу
    proxy_type = proxy.split('://')[0] if '://' in proxy else 'http'
    
    # Настройка прокси в соответствии с типом
    proxies = {
        "http": proxy,
        "https": proxy
    }
    
    try:
        start_time = time.time()
        
        # Используем специальные настройки для SOCKS прокси
        if 'socks' in proxy_type.lower():
            print(f"[ИНФО] Используется SOCKS прокси: {proxy}")
        
        # Увеличиваем таймаут для проверки через прокси
        response = requests.get(
            target_url, 
            proxies=proxies, 
            timeout=timeout, 
            verify=False,  # Отключаем проверку SSL сертификата
            headers=headers
        )
        elapsed = time.time() - start_time
        
        return {
            "success": response.status_code == 200,
            "status_code": response.status_code,
            "time": elapsed,
            "response_size": len(response.content),
            "error": None
        }
    except requests.exceptions.ProxyError as e:
        return {"success": False, "error": f"Ошибка прокси: {e}"}
    except requests.exceptions.ConnectTimeout:
        return {"success": False, "error": "Таймаут соединения"}
    except requests.exceptions.ReadTimeout:
        return {"success": False, "error": "Таймаут чтения"}
    except socket.timeout:
        return {"success": False, "error": "Таймаут сокета"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def main():
    if len(sys.argv) < 2:
        print("Использование: python check_api_proxy.py <файл_с_прокси> [url] [timeout]")
        print("Пример: python check_api_proxy.py formatted_proxies.txt https://api.belgiss.by/ 15")
        return
    
    proxy_file = sys.argv[1]
    target_url = sys.argv[2] if len(sys.argv) > 2 else "https://api.belgiss.by/"
    timeout = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    
    print(f"Проверка прокси для доступа к {target_url} (таймаут: {timeout} сек)")
    print("Убедитесь, что установлен пакет 'requests[socks]' для поддержки SOCKS прокси:")
    print("  pip install requests[socks]")
    
    # Загрузка прокси из файла
    proxies = load_proxies(proxy_file)
    if not proxies:
        print("[ОШИБКА] Не удалось загрузить прокси. Завершение работы.")
        return
    
    # Статистика
    working_proxies = []
    non_working_proxies = []
    
    # Проверка каждого прокси
    print(f"Начинаем проверку {len(proxies)} прокси...")
    
    for proxy in tqdm(proxies, desc="Проверка прокси"):
        result = check_proxy(proxy, target_url, timeout)
        if result["success"]:
            working_proxies.append((proxy, result))
            # Выводим информацию о рабочем прокси
            print(f"\n[УСПЕХ] {proxy} - Статус: {result['status_code']}, Время: {result['time']:.2f} сек, Размер ответа: {result['response_size']} байт")
        else:
            non_working_proxies.append((proxy, result))
            # Выводим информацию о нерабочем прокси, но менее подробную
            print(f"\n[ОШИБКА] {proxy} - {result['error']}")
    
    # Выводим общую статистику
    print("\n" + "="*80)
    print("РЕЗУЛЬТАТЫ ПРОВЕРКИ ПРОКСИ")
    print("="*80)
    print(f"Всего проверено: {len(proxies)} прокси")
    print(f"Рабочих прокси: {len(working_proxies)} ({len(working_proxies)/len(proxies)*100:.1f}%)")
    print(f"Нерабочих прокси: {len(non_working_proxies)} ({len(non_working_proxies)/len(proxies)*100:.1f}%)")
    
    # Если есть рабочие прокси, выводим топ-5 по скорости
    if working_proxies:
        print("\nТоп 5 самых быстрых прокси:")
        sorted_proxies = sorted(working_proxies, key=lambda x: x[1]["time"])
        for i, (proxy, result) in enumerate(sorted_proxies[:5], 1):
            print(f"{i}. {proxy} - Время: {result['time']:.2f} сек")
        
        # Сохраняем рабочие прокси в файл
        working_file = f"working_{os.path.basename(proxy_file)}"
        with open(working_file, 'w', encoding='utf-8') as f:
            for proxy, _ in working_proxies:
                f.write(f"{proxy}\n")
        print(f"\nРабочие прокси сохранены в файл {working_file}")
    else:
        print("\n[ВНИМАНИЕ] Рабочих прокси не найдено!")
    
    # Выводим наиболее частые ошибки
    error_stats = {}
    for _, result in non_working_proxies:
        error = result["error"]
        error_stats[error] = error_stats.get(error, 0) + 1
    
    if error_stats:
        print("\nЧастые ошибки:")
        for error, count in sorted(error_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"- {error}: {count} раз")

if __name__ == "__main__":
    main() 