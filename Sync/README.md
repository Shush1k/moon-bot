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
```
section `POSTGRES`:
- настройки главный базы
  
section `bot`:
- name - имя под которым, будет видны записи в базе postgres

section `SQLite`:
- name - имя файла базы данных sqlite

Файл `current_id.txt`:
- ставим 0 если создаем базу с нуля
- дальше файл не трогаем