import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
from utils import sanitize_text, apply_typo, apply_missing, transliterate
from config import TYPO_PCT, MISSING_PCT