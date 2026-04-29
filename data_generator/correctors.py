import sys
from spellchecker import SpellChecker
from natasha import Segmenter, MorphVocab, NewsEmbedding, NewsMorphTagger, NamesExtractor, Doc
from utils import save_before_data, save_after_data, save_corrections_log, print_correction_summary
from psycopg2 import extras

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
