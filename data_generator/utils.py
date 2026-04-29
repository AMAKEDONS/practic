import json
import csv
import random
from config import *
from datetime import datetime
from pathlib import Path

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

def save_before_data(records, filename=None):
    """
    Сохраняет данные до исправления
    records: список кортежей (guid, full_name, last_name, first_name, patronymic)
    """

    log_dir = Path("data_generator/logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"before_correction_{timestamp}.csv"

    filepath = log_dir / filename
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
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

    log_dir = Path("data_generator/logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"after_correction_{timestamp}.csv"

    filepath = log_dir / filename
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
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

    log_dir = Path("data_generator/logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"corrections_{timestamp}.json"

    filepath = log_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
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