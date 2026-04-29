import random
from utils import sanitize_text, transliterate
from datetime import datetime
from config import *
from faker import Faker

fake = Faker('ru_RU')

def find_existing_value(existing_records, field_index, exclude_guid=None):
    """
    Находит существующее значение из уже созданных записей
    """
    if not existing_records:
        return None
    
    # Собираем все значения указанного поля из существующих записей
    values = []
    for record in existing_records:
        if exclude_guid and record[0] == exclude_guid:
            continue  # Пропускаем оригинал
        value = record[field_index]
        if value is not None:
            values.append(value)
    
    if not values:
        return None
    
    # Возвращаем случайное существующее значение
    return random.choice(values)

def create_partial_dupe_by_inn(original_row, existing_records, p_dup_guid):
    """
    Создает дубль с другим инн (берет существующий инн из других записей)
    """
    mutated = list(original_row)
    
    # Ищем существующий инн из других записей
    existing_inn = find_existing_value(existing_records, 12, original_row[0])
    
    if existing_inn:
        mutated[12] = existing_inn  # Используем существующий инн
        print(f"[ЛОГ]: Частичный дубль по инн: {original_row[12]} -> {existing_inn}")
    else:
        # Если нет существующих, генерируем новый с небольшой мутацией
        old_inn = str(original_row[12])
        if len(old_inn) >= 2:
            idx = random.randint(0, len(old_inn)-1)
            new_inn = old_inn[:idx] + str(random.randint(0,9)) + old_inn[idx+1:]
            mutated[12] = sanitize_text(new_inn)
            print(f"[ЛОГ]: Частичный дубль по инн (мутация): {old_inn} -> {new_inn}")
    
    mutated[0] = p_dup_guid
    return tuple(mutated)

def create_partial_dupe_by_snils(original_row, existing_records, p_dup_guid):
    """
    Создает дубль с другим снилс (берет существующий снилс из других записей)
    """
    mutated = list(original_row)
    
    # Ищем существующий снилс из других записей
    existing_snils = find_existing_value(existing_records, 13, original_row[0])
    
    if existing_snils:
        mutated[13] = existing_snils # Используем существующий снилс
        print(f"[ЛОГ]: Частичный дубль по снилс: {original_row[13]} -> {existing_snils}")
    else:
        # Если нет существующих меняем одну группу цифр
        old_snils = str(original_row[13])
        parts = old_snils.split('-')
        if len(parts) >= 3:
            group_idx = random.randint(0, 2)
            parts[group_idx] = str(random.randint(100, 999))
            new_snils = '-'.join(parts)
            mutated[13] = sanitize_text(new_snils)
            print(f"[ЛОГ]: Частичный дубль по снилс (мутация): {old_snils} -> {new_snils}")
    
    mutated[0] = p_dup_guid
    return tuple(mutated)

def create_partial_dupe_by_passport(original_row, existing_records, p_dup_guid):
    """
    Создает дубль с другими паспортными данными (серия, номер, код подразделения)
    """
    mutated = list(original_row)
    
    # Для паспортаы меняем серию номер или код подразделенич
    what_to_change = random.choice(['series', 'number', 'dept_code', 'all'])
    
    if what_to_change == 'series':
        existing_series = find_existing_value(existing_records, 10, original_row[0])
        if existing_series:
            mutated[10] = existing_series
            print(f"[ЛОГ]: Частичный дубль по паспорту (серия): {original_row[10]} -> {existing_series}")
        else:
            new_series = str(random.randint(1000, 9999))
            mutated[10] = sanitize_text(new_series)
            print(f"[ЛОГ]: Частичный дубль по паспорту (новая серия): {original_row[10]} -> {new_series}")
    
    elif what_to_change == 'number':
        existing_number = find_existing_value(existing_records, 9, original_row[0])
        if existing_number:
            mutated[9] = existing_number
            print(f"[ЛОГ]: Частичный дубль по паспорту (номер): {original_row[9]} -> {existing_number}")
        else:
            new_number = str(random.randint(100000, 999999))
            mutated[9] = sanitize_text(new_number)
            print(f"[ЛОГ]: Частичный дубль по паспорту (новый номер): {original_row[9]} -> {new_number}")
    
    elif what_to_change == 'dept_code':
        existing_dept = find_existing_value(existing_records, 6, original_row[0])
        if existing_dept:
            mutated[6] = existing_dept
            print(f"[ЛОГ]: Частичный дубль по паспорту (код подразделения): {original_row[6]} -> {existing_dept}")
        else:
            new_dept = f"{random.randint(100, 999)}-{random.randint(100, 999)}"
            mutated[6] = sanitize_text(new_dept)
            print(f"[ЛОГ]: Частичный дубль по паспорту (новый код): {original_row[6]} -> {new_dept}")
    
    else: # all - меняем всее
        new_series = str(random.randint(1000, 9999))
        new_number = str(random.randint(100000, 999999))
        new_dept = f"{random.randint(100, 999)}-{random.randint(100, 999)}"
        mutated[10] = sanitize_text(new_series)
        mutated[9] = sanitize_text(new_number)
        mutated[6] = sanitize_text(new_dept)
        print(f"[ЛОГ]: Частичный дубль по паспорту (полностью изменен): серия {original_row[10]}->{new_series}, номер {original_row[9]}->{new_number}")
    
    mutated[0] = p_dup_guid
    return tuple(mutated)

def create_partial_dupe_by_fio_birthdate(original_row, existing_records, p_dup_guid, pools):
    """
    Создает дубль с другими ФИО и датой рождения (берет существующие значения)
    """
    mutated = list(original_row)
    
    # Меняем фамилию, имя, отчество и дату рождения
    fields_to_change = []
    
    # Фамилия
    existing_lastname = find_existing_value(existing_records, 1, original_row[0])
    if existing_lastname:
        mutated[1] = existing_lastname
        fields_to_change.append(f"фамилия {original_row[1]}->{existing_lastname}")
    else:
        mutated[1] = sanitize_text(random.choice(pools["last_names"]))
        fields_to_change.append(f"фамилия {original_row[1]}->{mutated[1]}")
    
    # Имя
    existing_firstname = find_existing_value(existing_records, 2, original_row[0])
    if existing_firstname:
        mutated[2] = existing_firstname
        fields_to_change.append(f"имя {original_row[2]}->{existing_firstname}")
    else:
        mutated[2] = sanitize_text(random.choice(pools["first_names"]))
        fields_to_change.append(f"имя {original_row[2]}->{mutated[2]}")
    
    # Отчество
    existing_patronymic = find_existing_value(existing_records, 3, original_row[0])
    if existing_patronymic:
        mutated[3] = existing_patronymic
        fields_to_change.append(f"отчество {original_row[3]}->{existing_patronymic}")
    else:
        mutated[3] = sanitize_text(random.choice(pools["patronymics"]))
        fields_to_change.append(f"отчество {original_row[3]}->{mutated[3]}")
    
    # Дата рождения
    existing_birthdate = find_existing_value(existing_records, 14, original_row[0])
    if existing_birthdate:
        mutated[14] = existing_birthdate
        fields_to_change.append(f"дата рождения {original_row[14]}->{existing_birthdate}")
    else:
        mutated[14] = fake.date_of_birth(minimum_age=14, maximum_age=90)
        fields_to_change.append(f"дата рождения {original_row[14]}->{mutated[14]}")
    
    mutated[0] = p_dup_guid
    print(f"[ЛОГ]: Частичный дубль по ФИО+дата рождения: {', '.join(fields_to_change)}")
    
    return tuple(mutated)

def generate_mutated_email_for_fio_dupe(mutated_dul_row, original_full_name):
    """
    Генерирует новый емайл для дубля с измененным ФИО
    """
    last_name = mutated_dul_row[1] if mutated_dul_row[1] else "user"
    birth_date = mutated_dul_row[14] if mutated_dul_row[14] else datetime.now()
    doc_number = mutated_dul_row[9] if mutated_dul_row[9] else "000000"
    
    email_prefix = transliterate(last_name)
    year = birth_date.year if hasattr(birth_date, 'year') else datetime.now().year
    
    return sanitize_text(f"{email_prefix}.{year}.{doc_number}@example.com")
