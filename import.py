import sqlite3
import csv
import pathlib

IMPORT_DIR = './import'

def create_table(filename: str, db_name: str='./db/db.sqlite3'):
  obj_db = pathlib.Path(db_name)
  obj_file = pathlib.Path(filename)
  table = obj_file.stem
  query_create = f'CREATE TABLE IF NOT EXISTS {table}(name TEXT NOT NULL UNIQUE, status TEXT, changed INTEGER, ttl INTEGER)'
  query_insert = f'INSERT INTO {table}(name) values(?);'
  
  with sqlite3.connect(str(obj_db)) as db:
    print(query_create)
    db.execute(query_create)
    
    with open(obj_file, 'r') as file:
      content = csv.reader(file)
      rows = [(item[0],) for item in content]
      try:
        print(f'Import file {str(obj_file)}')
        db.executemany(query_insert, rows)
        db.commit()
      except sqlite3.IntegrityError as exc:
        print(exc)


if __name__ == "__main__":
  for filename in pathlib.Path(IMPORT_DIR).iterdir():
    if filename.is_file():
      create_table(filename=str(filename))