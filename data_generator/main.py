import itertools
import random
import uuid
from datetime import datetime

import psycopg2
from psycopg2 import extras

from config import *

from generators import generate_pools, build_record
from duplicates import (
    create_partial_dupe_by_inn,
    create_partial_dupe_by_snils,
    create_partial_dupe_by_passport,
    create_partial_dupe_by_fio_birthdate,
    generate_mutated_email_for_fio_dupe
)
from correctors import run_fio_correction

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