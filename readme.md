# База для ETL процессов
Установка зависимостей:

``` bash
pip install poetry
poetry install
poetry shell
dagit -f jobs.py
```
Висит это базовое добро на localhost:3000

Потом разделю на субпроцессы и добавлю докер