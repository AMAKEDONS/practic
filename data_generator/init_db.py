import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import *

def init_database():
    conn = psycopg2.connect(**DB_PARAMS)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Очистка таблиц, если они существуют
    cursor.execute("""
        DROP TABLE IF EXISTS emails CASCADE;
        DROP TABLE IF EXISTS dul CASCADE;
        DROP TABLE IF EXISTS fl CASCADE;
    """)

    # Таблица ФЛ
    cursor.execute("""
        CREATE TABLE fl (
            guid_agent UUID PRIMARY KEY,
            full_name VARCHAR(255) NOT NULL
        );
    """)

    # Таблица ДУЛ
    cursor.execute("""
        CREATE TABLE dul (
            id SERIAL PRIMARY KEY,
            guid_agent UUID REFERENCES fl(guid_agent) ON DELETE CASCADE,
            last_name VARCHAR(100),
            first_name VARCHAR(100),
            patronymic VARCHAR(100),
            citizenship VARCHAR(100),
            birth_place TEXT,
            dept_code VARCHAR(10),
            issued_by TEXT,
            issue_date DATE,
            doc_number VARCHAR(10),
            doc_series VARCHAR(10),
            gender VARCHAR(10),
            inn VARCHAR(12),
            snils VARCHAR(14),
            birth_date DATE,
            doc_type VARCHAR(50)
        );
    """)

    # Таблица майл
    cursor.execute("""
        CREATE TABLE emails (
            id SERIAL PRIMARY KEY,
            guid_agent UUID REFERENCES fl(guid_agent) ON DELETE CASCADE,
            full_name VARCHAR(255),
            email_address VARCHAR(255)
        );
    """)

    cursor.execute("CREATE INDEX idx_dul_guid ON dul(guid_agent);")
    cursor.execute("CREATE INDEX idx_emails_guid ON emails(guid_agent);")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    init_database()