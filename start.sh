#!/bin/bash

# Создание таблиц
python data_generator/init_db.py

# Запуск генератора
python data_generator/main.py

echo "Генерация завершена."