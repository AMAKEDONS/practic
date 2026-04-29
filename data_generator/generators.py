import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
from utils import sanitize_text, apply_typo, apply_missing, transliterate
from config import TYPO_PCT, MISSING_PCT

fake = Faker('ru_RU')

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

def format_full_name(ln, fn, pt):
    variant = random.random()
    if variant < 0.6: return f"{ln} {fn} {pt}"
    elif variant < 0.8: return f"ИП {ln} {fn[0]}.{pt[0]}."
    else: return f"{ln} {fn[0]}. {pt[0]}."

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