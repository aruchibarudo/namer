from os import environ
import settings
from datetime import datetime
from fastapi import FastAPI
from modules.namer import Namer
from modules.models import *

from typing import List, Union
import uvicorn

DB_FILE = environ.get('DB_FILE', settings.DB_FILE)
PRELOCK_TTL = environ.get('PRELOCK_TTL', settings.PRELOCK_TTL)
FREE_TTL = environ.get('FREE_TTL', settings.FREE_TTL)
NAMER_TABLE = environ.get('NAMER_TABLE', settings.NAMER_TABLE)



namer = Namer(db=DB_FILE, table=NAMER_TABLE, ttl=PRELOCK_TTL, free_ttl=FREE_TTL)

api = FastAPI()


@api.get('/status')
async def get_status():
  _data = await namer.get_status()
  
  return {
    'result': 'OK',
    'detail': _data
  }
  

@api.get('/next')
async def get_next():
  _data = await namer.get_next()
  
  return {
    'result': 'OK',
    'detail': {
      'name': _data
    }
  }

@api.post('/next')
async def reserve_next(ttl: int=None):
  id, name = await namer.reserve_next(ttl=ttl)
  
  return {
    'result': 'OK',
    'detail': {
      'id': id,
      'name': name
    }
  }
  

@api.put('/lock/{name}')
async def dolock(name: str):
  return await namer.dolock(name=name)
  

@api.put('/unlock/{name}')
async def unlock(name: str):
  return await namer.unlock(name=name)


@api.post('/items')
async def add_items(items: List[NamerItem]):
  return await namer.add_items(items)
  

@api.delete('/item/{id}')
async def remove_item_by_id(id: int):
  return await namer.remove_item_by_id(id=id)


@api.delete('/item/{name}')
async def remove_item_by_name(name: str):
  return await namer.remove_item_by_name(name=name)


if __name__ == '__main__':
  uvicorn.run(app=api, host='0.0.0.0', port=8000)