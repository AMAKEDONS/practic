#!/bin/bash

# Создание таблиц
python database/init_db.py

# Запуск генератора
python database/generator.py

echo "Генерация завершена."