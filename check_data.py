import json
from generate_declarations_excel import extract_declaration_data

# Путь к тестовому файлу
test_file = 'declarations_details/batch_20250313_175926/2878546.json'

# Извлекаем данные
result = extract_declaration_data(test_file)

# Выводим интересующие нас поля
print("Заявитель Контактный реквизит:", result.get('Заявитель Контактный реквизит'))
print("Изготовитель Контактный реквизит:", result.get('Изготовитель Контактный реквизит'))
print("Код товара по ТН ВЭД ЕАЭС:", result.get('Код товара по ТН ВЭД ЕАЭС')) 