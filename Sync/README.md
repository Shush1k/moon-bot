### Синхронизация кучи sqlite баз в огромный монолит
---
Конфигурационный файл `config.ini`:
```
[POSTGRES]
host=192.168.88.122
database=dbname
user=postgres
password=xxxxxxx

[bot]
name = Mb1

[SQLite]
name = Binance.db

[time]
default_time = 20
```
section `POSTGRES`:
- настройки базы Postgres

section `bot`:
- name - имя под которым, будут видны записи в базе postgres

section `SQLite`:
- name - имя файла базы данных sqlite

section `time`:
- default_time - частота синхронизации Sqlite с Postgres
