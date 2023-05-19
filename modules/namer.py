import aiosqlite
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
 
  
  async def get_next(self) -> tuple:
    await self.unlock_expired()
    
    async with aiosqlite.connect(self.db_name) as db:
      async with db.execute(f'SELECT rowid, name FROM {self.table} WHERE (status = ? and changed > ?) or status is NULL ORDER BY rowid LIMIT 1',
                             ('FREE', self.get_timestamp() + self.free_ttl)) as cursor:
        async for id, name in cursor:
          return id, name

  
  async def reserve_next(self, ttl: int=None) -> tuple:
    await self.unlock_expired()
    
    if isinstance(ttl, int):
      _prelock_ttl = ttl
    else:
      _prelock_ttl = self.prelock_ttl

    async with aiosqlite.connect(self.db_name) as db:    
      async with  db.execute(f'SELECT rowid, name FROM {self.table} WHERE (status = ? and changed > ?) or status is NULL ORDER BY rowid LIMIT 1',
                             ('FREE', self.get_timestamp() + self.free_ttl)) as cursor:
        async for id, name in cursor:
          _res = await db.execute(f'UPDATE {self.table} SET status = ?, changed = ?, ttl = ? WHERE rowid = ?',
                                       ('PRELOCK', self.get_timestamp() , _prelock_ttl, id))
          await db.commit()
          return id, name


  async def prelock(self, id: int, ttl: int=None):
    
    if isinstance(ttl, int):
      _prelock_ttl = ttl
    else:
      _prelock_ttl = self.prelock_ttl
      
    async with aiosqlite.connect(self.db_name) as db:
      _res = await db.execute(f'UPDATE {self.table} SET status = ?, changed = ?, ttl = ? WHERE rowid = ?',
                              ('PRELOCK', self.get_timestamp() , _prelock_ttl, id))
      await db.commit()
      return _res


  async def dolock(self, name: str):
    async with aiosqlite.connect(self.db_name) as db:
      try:
        _res = await db.execute(f'UPDATE {self.table} SET status = ?, changed = ? WHERE name = ?',
                                ('LOCK', self.get_timestamp(), name))
        await db.commit()
        return {
          'result': 'OK'
        }
      except Exception as exc:
        return {
          'result': 'FAIL',
          'detail': str(exc)
        }
    
  
  async def unlock(self, name: str):
    async with aiosqlite.connect(self.db_name) as db:
      try:
        _res = await db.execute(f'UPDATE {self.table} SET status = ?, changed = ? WHERE name = ?',
                                ('FREE', self.get_timestamp(), name))
        await db.commit()
        return {
          'result': 'OK'
        }
      except Exception as exc:
        return {
          'result': 'FAIL',
          'detail': str(exc)
        }
    
    

  def get_timestamp(self) -> int:
    return int(datetime.timestamp(datetime.now()))


  async def unlock_expired(self):
    async with aiosqlite.connect(self.db_name) as db:
      try:
        await db.execute(f'UPDATE {self.table} SET status = ?, changed = ? WHERE status = ? AND (changed + ttl) < ?',
                                ('FREE', self.get_timestamp(), 'PRELOCK', self.get_timestamp()))
        await db.commit()
        return True
      except Exception as exc:
        print(exc)
        return False
    
    
  async def get_status(self) -> dict:
    _res = {}
    async with aiosqlite.connect(self.db_name) as db:
      db.row_factory = aiosqlite.Row
    
      async with db.execute(f'SELECT status, count(rowid) as qty from {self.table} GROUP by status') as cursor:
        async for _row in cursor:
          _res[_row['status']] = _row['qty']
          
        return _res
  
  
  async def add_items(self, items: NamerItems):
    _tmp = list()
    
    for item in items:
      _tmp.append(tuple(item.dict().values()))

    async with aiosqlite.connect(self.db_name) as db:
      
      try:
        await db.executemany(f'INSERT INTO {self.table} (name, status, changed, ttl) VALUES (?, ?, ?, ?)', _tmp)
        await db.commit()
        return {
          'result': 'OK',
          'detail': f'{len(_tmp)} items added'
        }
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


  async def remove_item_by_id(self, id: int):
    async with aiosqlite.connect(self.db_name) as db:
      _res = await db.execute(f'UPDATE {self.table} SET status = ? WHERE rowid = ?', ('REMOVED', id))
      await db.commit()
      return _res

  

  async def remove_item_by_name(self, name: str):
    async with aiosqlite.connect(self.db_name) as db:
      _res = await db.execute(f'UPDATE {self.table} SET status = ? WHERE name = ?', ('REMOVED', name))
      await db.commit()
      return _res
  

  async def remove_items_by_ids(self, items: tuple):
    _tmp = list()
    
    for item in items:
      _tmp.append(tuple('REMOVED',item))
      
    async with aiosqlite.connect(self.db_name) as db:
      _res = await db.executemany(f'UPDATE {self.table} SET status = ? WHERE rowid = ?', _tmp)
      await db.commit()
      return _res
  

  async def remove_items_by_names(self, items: tuple):
    _tmp = list()
    
    for item in items:
      _tmp.append(tuple('REMOVED',item))
      
    async with aiosqlite.connect(self.db_name) as db:
      _res = await db.executemany(f'UPDATE {self.table} SET status = ? WHERE name = ?', _tmp)
      await db.commit()
      return _res
  
