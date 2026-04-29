import itertools
import random
import uuid
import json
import csv
from pathlib import Path
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import extras
from faker import Faker

import sys
try:
    import pkg_resources
except ImportError:
    import pip._vendor.pkg_resources as pkg_resources
    sys.modules['pkg_resources'] = pkg_resources

from spellchecker import SpellChecker
from natasha import (
    Segmenter, MorphVocab, NewsEmbedding, 
    NewsMorphTagger, NamesExtractor, Doc
)

from config import *

segmenter = Segmenter()
morph_vocab = MorphVocab()
emb = NewsEmbedding()
morph_tagger = NewsMorphTagger(emb)
names_extractor = NamesExtractor(morph_vocab)

def save_before_data(records, filename=None):
    """
    Сохраняет данные до исправления
    records: список кортежей (guid, full_name, last_name, first_name, patronymic)
    """
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"before_correction_{timestamp}.csv"
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(['guid', 'full_name', 'last_name', 'first_name', 'patronymic'])
        writer.writerows(records)
    
    print(f"[ЛОГ]: Данные ДО сохранены в {filename}")
    return filename


def save_after_data(records, corrected_guids, filename=None):
    """
    Сохраняет данные после исправления
    records: список кортежей (guid, full_name, last_name, first_name, patronymic)
    corrected_guids: множество guid которые были исправлены
    """
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"after_correction_{timestamp}.csv"
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(['guid', 'full_name', 'last_name', 'first_name', 'patronymic', 'was_fixed'])
        
        for record in records:
            was_fixed = 'YES' if record[0] in corrected_guids else 'NO'
            writer.writerow(list(record) + [was_fixed])
    
    print(f"[ЛОГ]: Данные ПОСЛЕ сохранены в {filename}")
    return filename


def save_corrections_log(corrections, filename=None):
    """
    Сохраняет список исправлений
    corrections: список словарей [{'guid': '...', 'before': '...', 'after': '...'}, ...]
    """
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"corrections_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(corrections, f, ensure_ascii=False, indent=2)
    
    print(f"[ЛОГ]: Лог исправлений сохранен в {filename}")
    return filename


def print_correction_summary(corrections):
    """Печатает краткую сводку исправлений"""
    if not corrections:
        print("Нет исправлений")
        return
    
    print(f"СВОДКА: исправлено {len(corrections)} записей")
    
    # Считаем уникальные guid
    unique_guids = set(c['guid'] for c in corrections)
    print(f"Уникальных записей: {len(unique_guids)}")

class FioCorrector:
    def __init__(self, pools):
        """Инициализация спеллчекера с русским языком и базой имен"""
        # ВАЖНО: указываем language='ru' для русского языка
        self.spell = SpellChecker(language='ru', distance=2)
        
        # Добавляем все эталонные ФИО в словарь корректора
        print(f"[ИНИЦ]: Загрузка словаря: {len(pools['last_names'])} фамилий, "
              f"{len(pools['first_names'])} имен, {len(pools['patronymics'])} отчеств")
        
        # Загружаем словарь из пулов
        self.spell.word_frequency.load_words([w.lower() for w in pools["last_names"]])
        self.spell.word_frequency.load_words([w.lower() for w in pools["first_names"]])
        self.spell.word_frequency.load_words([w.lower() for w in pools["patronymics"]])
        
        # Добавляем базовые русские имена
        base_names = [
            'александр', 'сергей', 'дмитрий', 'андрей', 'алексей',
            'владимир', 'никита', 'иван', 'михаил', 'евгений', 'николай',
            'елена', 'ольга', 'наталья', 'ирина', 'светлана', 'татьяна'
        ]
        self.spell.word_frequency.load_words(base_names)
        
        # Сохраняем пулы для проверки
        self.last_names_pool = [n.lower() for n in pools["last_names"]]
        self.first_names_pool = [n.lower() for n in pools["first_names"]]
        self.patronymics_pool = [n.lower() for n in pools["patronymics"]]

    def _fix_single_word(self, word, pool=None):
        """Исправляет одно слово с проверкой по пулу"""
        if not word or len(word) < 2:
            return word
        
        # Очищаем от пунктуации
        clean_word = word.strip('.,!?;:()[]{}"\'')
        if not clean_word:
            return word
        
        # Сохраняем original регистр
        is_upper = word[0].isupper()
        original_case = word
        
        # Если слово уже в пуле - не трогаем
        if pool:
            if clean_word.lower() in pool:
                return word
        
        # Пробуем найти исправление через pyspellchecker
        try:
            correction = self.spell.correction(clean_word.lower())
            if correction and correction.lower() != clean_word.lower():
                # Проверяем, что исправление имеет смысл
                if pool and correction.lower() in pool:
                    result = correction.capitalize() if is_upper else correction
                    print(f"[ОЧИСТКА]: Исправление '{word}' на '{result}'")
                    return result
                elif not pool:
                    result = correction.capitalize() if is_upper else correction
                    print(f"[ОЧИСТКА]: Исправление '{word}' на '{result}'")
                    return result
        except Exception as e:
            # пропускаем ошибки
            pass
        
        return word

    def fix_text(self, text):
        """Исправляет опечатки в ФИО"""
        if not text or not isinstance(text, str):
            return text
        
        # Разбиваем на части
        parts = text.split()
        fixed_parts = []
        
        # Определяем тип формата
        if len(parts) >= 3 and parts[0].upper() == "ИП":
            # Формат с ИП
            fixed_parts.append(parts[0]) # ИП не трогаем
            # Исправляем фамилию
            if len(parts) > 1:
                fixed_parts.append(self._fix_single_word(parts[1], self.last_names_pool))
            # Исправляем инициалы - это не трогать
            if len(parts) > 2:
                fixed_parts.append(parts[2])
        else:
            # Обычный формат: фамилия имя отчество
            parts_needed = min(3, len(parts))
            for i, part in enumerate(parts[:parts_needed]):
                if i == 0:
                    pool = self.last_names_pool
                elif i == 1:
                    pool = self.first_names_pool
                else:
                    pool = self.patronymics_pool
                fixed_parts.append(self._fix_single_word(part, pool))
            
            # Добавляем остальные части
            if len(parts) > 3:
                fixed_parts.extend(parts[3:])
        
        result = " ".join(fixed_parts)
        
        return result

fake = Faker('ru_RU')

TRANSLIT_MAP = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo', 'ж': 'zh',
    'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o',
    'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts',
    'ч': 'ch', 'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
}

# Типы частичных дублей
PARTIAL_DUPE_TYPES = [
    "inn",
    "snils",
    "passport",
    "fio_birthdate"
]

def sanitize_text(text):
    if not text or not isinstance(text, str):
        return text
    replacements = {
        '\xab': '"', '\xbb': '"', 
        '\u2013': '-', '\u2014': '-', 
        '\u201c': '"', '\u201d': '"', '№': 'N'
    }
    for char, rep in replacements.items():
        text = text.replace(char, rep)
    return text.encode('iso-8859-5', 'ignore').decode('iso-8859-5')

def transliterate(text):
    if not text: return "user"
    return ''.join(TRANSLIT_MAP.get(c, c) for c in text.lower())

def apply_typo(text):
    if not text or random.random() > TYPO_PCT:
        return text
    text_list = list(str(text))
    if len(text_list) > 1:
        idx = random.randint(0, len(text_list) - 1)
        text_list[idx] = random.choice('абвгдежзийклмнопрстуфхцчшщ')
    return ''.join(text_list)

def apply_missing(val):
    return None if random.random() < MISSING_PCT else val

def format_full_name(ln, fn, pt):
    variant = random.random()
    if variant < 0.6: return f"{ln} {fn} {pt}"
    elif variant < 0.8: return f"ИП {ln} {fn[0]}.{pt[0]}."
    else: return f"{ln} {fn[0]}. {pt[0]}."

def generate_pools():
    ln_pool = set()
    while len(ln_pool) < 100:
        ln_pool.add(fake.last_name())
        
    return {
        "last_names": list(ln_pool),
        "first_names": [fake.first_name() for _ in range(100)],
        "patronymics": [fake.middle_name() for _ in range(100)],
        "birth_places": [fake.city() for _ in range(200)],
        "issued_by": [fake.company() for _ in range(200)]
    }

def insert_batches(cursor, batch_fl, batch_dul, batch_email):
    extras.execute_values(
        cursor, 
        "INSERT INTO fl (guid_agent, full_name) VALUES %s", 
        batch_fl
    )
    
    extras.execute_values(
        cursor,
        """INSERT INTO dul (
            guid_agent, last_name, first_name, patronymic, citizenship, 
            birth_place, dept_code, issued_by, issue_date, doc_number, 
            doc_series, gender, inn, snils, birth_date, doc_type
        ) VALUES %s""",
        batch_dul
    )
    
    extras.execute_values(
        cursor, 
        "INSERT INTO emails (guid_agent, full_name, email_address) VALUES %s", 
        batch_email
    )

def build_record(ln, fn, pt, pools):
    guid = str(uuid.uuid4())
    full_name = sanitize_text(apply_typo(format_full_name(ln, fn, pt)))
    
    b_date = fake.date_of_birth(minimum_age=14, maximum_age=90)
    i_date = b_date + timedelta(days=random.randint(14*365, 20*365))
    doc_num = str(random.randint(100000, 999999))
    doc_ser = str(random.randint(1000, 9999))
    
    dul_row = (
        guid,
        sanitize_text(apply_typo(ln)),
        sanitize_text(apply_typo(fn)),
        sanitize_text(apply_typo(pt)),
        sanitize_text(apply_missing("РФ")),
        sanitize_text(apply_missing(random.choice(pools["birth_places"]))),
        sanitize_text(apply_missing(f"{random.randint(100, 999)}-{random.randint(100, 999)}")),
        sanitize_text(apply_missing(random.choice(pools["issued_by"]))),
        apply_missing(i_date),
        sanitize_text(apply_missing(doc_num)),
        sanitize_text(apply_missing(doc_ser)),
        sanitize_text(apply_missing(random.choice(["Мужской", "Женский"]))),
        sanitize_text(apply_missing(str(random.randint(100000000000, 999999999999)))),
        sanitize_text(apply_missing(f"{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(100,999)} 00")),
        apply_missing(b_date),
        sanitize_text("Паспорт гражданина РФ")
    )
    
    email_prefix = transliterate(ln)
    email = sanitize_text(f"{email_prefix}.{b_date.year}.{doc_num}@example.com")
    email_row = (guid, full_name, apply_missing(email))
    
    return (guid, full_name), dul_row, email_row

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

def run_fio_correction(conn, pools):
    """Очистка опечаток в ФИО после генерации"""

    print("[ОЧИСТКА] Запуск исправления опечаток в ФИО")
    
    corrector = FioCorrector(pools)
    cursor = conn.cursor()

    # Получаем данные до исправления
    cursor.execute("""
        SELECT f.guid_agent, f.full_name, d.last_name, d.first_name, d.patronymic 
        FROM fl f 
        JOIN dul d ON f.guid_agent = d.guid_agent
    """)
    before_records = cursor.fetchall()
    
    if not before_records:
        print("[ОЧИСТКА]: Нет записей для очистки")
        cursor.close()
        return
    
    # СОХРАНЯЕМ ДАННЫЕ ДО ИСПРАВЛЕНИЯ
    before_file = save_before_data(before_records)
    print(f"[ОЧИСТКА]: Данные ДО сохранены в {before_file}")

    # Получаем все записи для исправления
    cursor.execute("SELECT guid_agent, full_name FROM fl")
    records = cursor.fetchall()
    
    print(f"[ОЧИСТКА]: Проверка {len(records)} записей...")
    
    updates = []
    corrections = [] # для хранения деталей исправлений
    
    for guid, full_name in records:
        if not full_name:
            continue
            
        fixed_name = corrector.fix_text(full_name)
        
        if fixed_name and fixed_name != full_name:
            updates.append((guid, fixed_name))
            
            # Сохраняем детали исправления для лога
            corrections.append({
                'guid': guid,
                'before': full_name,
                'after': fixed_name
            })

    if updates:
        print(f"\n[ОЧИСТКА]: Найдено опечаток: {len(updates)}")
        
        # Обновляем записи в базе
        update_query = """
            UPDATE fl AS f 
            SET full_name = v.fixed_name 
            FROM (VALUES %s) AS v(guid, fixed_name) 
            WHERE f.guid_agent = v.guid::uuid
        """
        extras.execute_values(cursor, update_query, updates)
        conn.commit()
        print("[ОЧИСТКА]: База данных успешно обновлена!")
        
        # СОХРАНЯЕМ ЛОГ ИСПРАВЛЕНИЙ
        corrections_file = save_corrections_log(corrections)
        print(f"[ОЧИСТКА]: Лог исправлений сохранен в {corrections_file}")
        
        # Получаем данные после исправления
        cursor.execute("""
            SELECT f.guid_agent, f.full_name, d.last_name, d.first_name, d.patronymic 
            FROM fl f 
            JOIN dul d ON f.guid_agent = d.guid_agent
        """)
        after_records = cursor.fetchall()
        
        # СОХРАНЯЕМ ДАННЫЕ ПОСЛЕ ИСПРАВЛЕНИЯ
        corrected_guids = {c['guid'] for c in corrections}
        after_file = save_after_data(after_records, corrected_guids)
        print(f"[ОЧИСТКА]: Данные ПОСЛЕ сохранены в {after_file}")
        
        print_correction_summary(corrections)
        
    else:
        print("\n[ОЧИСТКА]: Опечаток не найдено, база чиста.")
        
    cursor.close()

def main():
    conn = psycopg2.connect(**DB_PARAMS)
    conn.set_client_encoding('ISO_8859_5')
    cursor = conn.cursor()
    cursor.execute("SET client_encoding TO 'ISO_8859_5';")
    
    print("[ЛОГ]: Подключение установлено")
    
    pools = generate_pools()
    combinations = list(itertools.product(pools["last_names"], pools["first_names"], pools["patronymics"]))
    random.shuffle(combinations)

    batch_fl, batch_dul, batch_email = [], [], []
    count = 0
    all_dul_records = [] # Хранилище всех созданных записей для подстан-ки значений
    exact_dupes_count = 0
    partial_dupes_count = 0
    
    for ln, fn, pt in combinations:
        if count >= TARGET_RECORDS: break
            
        fl_row, dul_row, email_row = build_record(ln, fn, pt, pools)
        
        # Обработка точных дублей
        if random.random() < EXACT_DUPE_PCT:
            dup_guid = str(uuid.uuid4())
            batch_fl.append((dup_guid, fl_row[1]))
            batch_dul.append((dup_guid,) + dul_row[1:])
            batch_email.append((dup_guid,) + email_row[1:])
            count += 1
            exact_dupes_count += 1
            print(f"[ЛОГ]: Создан ТОЧНЫЙ дубль для {fl_row[1]}")
            
        # Обработка частичных дублей
        if random.random() < PARTIAL_DUPE_PCT:
            p_dup_guid = str(uuid.uuid4())
            dupe_type = random.choice(PARTIAL_DUPE_TYPES)
            
            print(f"[ЛОГ]:\nСоздание частичного дубля для {fl_row[1]}, тип: {dupe_type}")
            
            # Создаем дубль в зависимости от выбранного типа
            if dupe_type == "inn":
                mutated_dul = create_partial_dupe_by_inn(dul_row, all_dul_records, p_dup_guid)
                mutated_email = (p_dup_guid, fl_row[1], email_row[2])  # Email не меняем
            
            elif dupe_type == "snils":
                mutated_dul = create_partial_dupe_by_snils(dul_row, all_dul_records, p_dup_guid)
                mutated_email = (p_dup_guid, fl_row[1], email_row[2])
            
            elif dupe_type == "passport":
                mutated_dul = create_partial_dupe_by_passport(dul_row, all_dul_records, p_dup_guid)
                mutated_email = (p_dup_guid, fl_row[1], email_row[2])
            
            else: # fio_birthdate
                mutated_dul = create_partial_dupe_by_fio_birthdate(dul_row, all_dul_records, p_dup_guid, pools)
                # Генерируем новый email на основе измененных данных
                new_email = generate_mutated_email_for_fio_dupe(mutated_dul, fl_row[1])
                mutated_email = (p_dup_guid, fl_row[1], new_email)
            
            batch_fl.append((p_dup_guid, fl_row[1]))
            batch_dul.append(mutated_dul)
            batch_email.append(mutated_email)
            count += 1
            partial_dupes_count += 1

        # Добавляем основную запись
        batch_fl.append(fl_row)
        batch_dul.append(dul_row)
        batch_email.append(email_row)
        count += 1
        
        # Сохраняем запись для след подстановок
        all_dul_records.append(dul_row)

        # Вставка
        if len(batch_fl) >= BATCH_SIZE:
            insert_batches(cursor, batch_fl, batch_dul, batch_email)
            conn.commit()
            print(f"[ЛОГ]:\nЗагружено: {count} записей (Точных дублей: {exact_dupes_count}, Частичных: {partial_dupes_count})\n")
            batch_fl, batch_dul, batch_email = [], [], []

    # Фиксируем остаток
    if batch_fl:
        insert_batches(cursor, batch_fl, batch_dul, batch_email)
        conn.commit()

    cursor.close()

    print(f"\n[ЛОГ]:")
    print(f"> Генерация завершена")
    print(f"> Всего записей в БД: {count}")
    print(f"> Точных дублей создано: {exact_dupes_count}")
    print(f"> Частичных дублей создано: {partial_dupes_count}")

    choice = input("\nХотите запустить очистку опечаток в ФИО? (y/n): ")
    if choice.strip().lower() in ['y', 'yes', 'да', 'д']:
        run_fio_correction(conn, pools)
    else:
        print("Очистка пропущена.")

    # Закрываем соединение в самом конце
    conn.close()

if __name__ == "__main__":
    main()