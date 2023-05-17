import aiosqlite
from sqlite3 import IntegrityError
from datetime import datetime
from .models import *


class Namer:
  def __init__(self, db: str, table: str, ttl: int=0, free_ttl: int=0):
    self.db_name = db
    self.db = None
    self.db_cur = None
    self.table = table.strip()
    self.prelock_ttl = ttl
    self.free_ttl = free_ttl
    
  
  async def connect(self):
    self.db = await aiosqlite.connect(self.db_name)
    self.db_cur = await self.db.cursor()
    
    
  async def disconnect(self):
    await self.db_cur.close()
    await self.db.close()

  async def get_next(self) -> tuple:
    await self.connect()
    
    async with  self.db.execute(f'SELECT id, name FROM {self.table} WHERE (status = ? and changed > ?) or status is NULL ORDER BY id LIMIT 1', ('FREE', self.get_timestamp() + self.free_ttl)) as cursor:
      async for id, name in cursor:
        return id, name

  
  async def prelock(self, id: int, ttl: int=None):
    
    if isinstance(ttl, int):
      _prelock_ttl = ttl
    else:
      _prelock_ttl = self.prelock_ttl
      
    await self.connect()
    print('UPDATE {} SET status = {}, changed = {}, ttl = {} WHERE id = {}'.format(self.table, 'PRELOCK', self.get_timestamp(), _prelock_ttl, id))
    _res = await self.db.execute(f'UPDATE {self.table} SET status = ?, changed = ?, ttl = ? WHERE id = ?', ('PRELOCK', self.get_timestamp() , _prelock_ttl, id))
    await self.db.commit()
    await self.disconnect()


  async def lock(self, id: int):
    await self.connect()
    await self.db.execute(f'UPDATE {self.table} SET status = ?, changed = ? WHERE id = ?', ('LOCK', self.get_timestamp() , id))
    await self.db.commit()
    await self.disconnect()
    

  async def lock(self, name: str):
    await self.connect()
    await self.db.execute(f'UPDATE {self.table} SET status = ?, changed = ? WHERE name = ?', ('LOCK', self.get_timestamp(), name))
    await self.db.commit()
    await self.disconnect()
  
  
  async def unlock(self, id: int):
    await self.connect()
    await self.db.execute(f'UPDATE {self.table} SET status = ?, changed = ? WHERE id = ?', ('FREE', self.get_timestamp() , id))
    await self.db.commit()
    await self.disconnect()


  async def unlock(self, name: str):
    await self.connect()
    await self.db.execute(f'UPDATE {self.table} SET status = ?, changed = ? WHERE name = ?', ('FREE', self.get_timestamp(), name))
    await self.db.commit()
    await self.disconnect()
    

  def get_timestamp(self) -> int:
    return int(datetime.timestamp(datetime.now()))


  async def unlock_expired(self):
    await self.connect()
    await self.db.execute(f'UPDATE {self.table} set status = ?, changed = ? WHERE status = ? changed + timestanp < ?', ('FREE', self.get_timestamp), 'PRELOCK', self.get_timestamp, )
    await self.db.commit()
    await self.disconnect()
    
    
  async def get_status(self) -> dict:
    _res = {}
    await self.connect()
    self.db.row_factory = aiosqlite.Row
    
    async with self.db.execute(f'SELECT status, count(id) as qty from {self.table} GROUP by status') as cursor:
      async for _row in cursor:
        _res[_row['status']] = _row['qty']
    
    await self.disconnect()
    
    return _res
  
  
  async def add_items(self, items: NamerItems):
    _tmp = list()
    
    for item in items:
      _tmp.append(tuple(item.dict().values()))
      
    await self.connect()

    try:
      await self.db.executemany(f'INSERT INTO {self.table} (name, status, changed, ttl) VALUES (?, ?, ?, ?)', _tmp)
      await self.db.commit()
    except aiosqlite.IntegrityError as exc:
      if exc.sqlite_errorcode == 2067:
        return {
          'result': 'FAIL',
          'detail': 'Some item already exists'
        }
      else:
        return {
          'result': 'FAIL',
          'detail': exc
        }
    finally:
      await self.disconnect()
    
    return {
      'result': 'OK',
      'detail': f'{len(_tmp)} items added'
    }

  async def remove_item_by_id(self, id: int):
    await self.connect()
    await self.db.execute(f'UPDATE {self.table} SET status = ? WHERE id = ?', ('REMOVED', id))
    await self.db.commit()
    await self.disconnect()
  

  async def remove_item_by_name(self, name: str):
    await self.connect()
    await self.db.execute(f'UPDATE {self.table} SET status = ? WHERE name = ?', ('REMOVED', name))
    await self.db.commit()
    await self.disconnect()
  

  async def remove_items_by_id(self, items: tuple):
    _tmp = list()
    
    for item in items:
      _tmp.append(tuple('REMOVED',item))
      
    await self.connect()
    await self.db.executemany(f'UPDATE {self.table} SET status = ? WHERE id = ?', _tmp)
    await self.db.commit()
    await self.disconnect()
  

  async def remove_items_by_name(self, items: tuple):
    _tmp = list()
    
    for item in items:
      _tmp.append(tuple('REMOVED',item))
      
    await self.connect()
    await self.db.executemany(f'UPDATE {self.table} SET status = ? WHERE name = ?', _tmp)
    await self.db.commit()
    await self.disconnect()
  
