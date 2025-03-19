import os
import sys
import chardet

def format_proxies(input_file, output_file=None, proxy_type='socks5'):
    """
    Преобразует прокси из формата IP:PORT:LOGIN:PASSWORD 
    в формат socks5://LOGIN:PASSWORD@IP:PORT или другой указанный формат
    
    Args:
        input_file: Путь к входному файлу с прокси
        output_file: Путь к выходному файлу (по умолчанию formatted_<input_file>)
        proxy_type: Тип прокси (socks5, http, https)
    """
    if not os.path.exists(input_file):
        print(f"[ОШИБКА] Файл {input_file} не найден")
        return False
    
    if output_file is None:
        # Создаем имя выходного файла на основе входного
        basename = os.path.basename(input_file)
        name, ext = os.path.splitext(basename)
        output_file = f"formatted_{name}{ext}"
    
    print("ВАЖНО: Для работы с SOCKS5 прокси требуется установить дополнительный пакет:")
    print("  pip install requests[socks]")
    
    # Определяем кодировку файла
    with open(input_file, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        encoding = result['encoding']
    
    print(f"Определена кодировка файла: {encoding}")
    print(f"Преобразование прокси из файла {input_file} в формат {proxy_type}://LOGIN:PASSWORD@IP:PORT")
    
    try:
        # Попытка чтения файла с обнаруженной кодировкой
        try:
            with open(input_file, 'r', encoding=encoding) as f_in:
                proxies = f_in.readlines()
        except UnicodeDecodeError:
            # Если не удалось прочитать с обнаруженной кодировкой, пробуем другие
            for enc in ['utf-8', 'utf-16', 'utf-16-le', 'utf-16-be', 'cp1251', 'latin-1']:
                try:
                    with open(input_file, 'r', encoding=enc) as f_in:
                        proxies = f_in.readlines()
                    print(f"Успешно прочитан файл с кодировкой: {enc}")
                    encoding = enc
                    break
                except UnicodeDecodeError:
                    continue
            else:
                # Если не удалось прочитать ни с одной кодировкой
                print("[ОШИБКА] Не удалось определить кодировку файла")
                return False
        
        formatted_proxies = []
        for proxy in proxies:
            proxy = proxy.strip()
            if not proxy or proxy.startswith('#'):
                # Пропускаем пустые строки и комментарии
                continue
            
            # Если прокси уже в правильном формате, оставляем как есть
            if proxy.startswith(f"{proxy_type}://"):
                formatted_proxies.append(proxy)
                continue
            
            # Убираем префикс с протоколом, если есть
            if '://' in proxy:
                proxy = proxy.split('://', 1)[1]
            
            # Разбиваем строку прокси на компоненты
            parts = proxy.split(':')
            if len(parts) == 2:
                # Формат IP:PORT
                ip, port = parts
                formatted_proxy = f"{proxy_type}://{ip}:{port}"
            elif len(parts) == 4:
                # Формат IP:PORT:LOGIN:PASSWORD
                ip, port, login, password = parts
                formatted_proxy = f"{proxy_type}://{login}:{password}@{ip}:{port}"
            else:
                # Пробуем извлечь логин/пароль в формате IP:PORT@LOGIN:PASSWORD
                if '@' in proxy:
                    auth_parts = proxy.split('@')
                    if len(auth_parts) == 2:
                        credentials, address = auth_parts
                        if ':' in address and ':' in credentials:
                            login, password = credentials.split(':', 1)
                            ip, port = address.split(':', 1)
                            formatted_proxy = f"{proxy_type}://{login}:{password}@{ip}:{port}"
                            formatted_proxies.append(formatted_proxy)
                            continue
                
                print(f"[ПРЕДУПРЕЖДЕНИЕ] Неподдерживаемый формат прокси: {proxy}")
                continue
                
            formatted_proxies.append(formatted_proxy)
        
        # Выводим первые 5 отформатированных прокси для проверки
        if formatted_proxies:
            print("\nПримеры отформатированных прокси:")
            for i, proxy in enumerate(formatted_proxies[:5]):
                print(f"  {i+1}. {proxy}")
        
        # Записываем отформатированные прокси в выходной файл
        with open(output_file, 'w', encoding='utf-8') as f_out:
            for proxy in formatted_proxies:
                f_out.write(f"{proxy}\n")
        
        print(f"\nГотово! Отформатировано {len(formatted_proxies)} прокси из {len(proxies)} исходных.")
        print(f"Результат сохранен в файл: {output_file}")
        return True
    
    except Exception as e:
        print(f"[ОШИБКА] Ошибка при преобразовании прокси: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python format_proxies.py <input_file> [output_file] [proxy_type]")
        print("Доступные типы прокси: socks5 (по умолчанию), http, https")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    proxy_type = sys.argv[3] if len(sys.argv) > 3 else 'socks5'
    
    format_proxies(input_file, output_file, proxy_type) 