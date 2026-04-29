DB_PARAMS = {
    "dbname": "practic",
    "user": "postgres",
    "password": "1234",
    "host": "127.0.0.1",
    "port": "8119"
}

TARGET_RECORDS = 1000
BATCH_SIZE = 100
TYPO_PCT = 0.05
MISSING_PCT = 0.05
EXACT_DUPE_PCT = 0.02
PARTIAL_DUPE_PCT = 0.03

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