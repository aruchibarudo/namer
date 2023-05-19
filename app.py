from os import environ
import settings
from datetime import datetime
from fastapi import FastAPI
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.staticfiles import StaticFiles
from modules.namer import Namer
from modules.models import *

from typing import List, Union
import uvicorn


DB_FILE = environ.get('DB_FILE', settings.DB_FILE)
PRELOCK_TTL = environ.get('PRELOCK_TTL', settings.PRELOCK_TTL)
FREE_TTL = environ.get('FREE_TTL', settings.FREE_TTL)
NAMER_TABLE = environ.get('NAMER_TABLE', settings.NAMER_TABLE)



namer = Namer(db=DB_FILE, table=NAMER_TABLE, ttl=PRELOCK_TTL, free_ttl=FREE_TTL)

api = FastAPI(docs_url=None, redoc_url=None)
api.mount("/static", StaticFiles(directory="static"), name="static")


@api.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
  return get_swagger_ui_html(
    openapi_url=api.openapi_url,
    title=api.title + " - Swagger UI",
    oauth2_redirect_url=api.swagger_ui_oauth2_redirect_url,
    swagger_js_url="/static/swagger-ui-bundle.js",
    swagger_css_url="/static/swagger-ui.css",
  )


@api.get(api.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect():
  return get_swagger_ui_oauth2_redirect_html()


@api.get("/redoc", include_in_schema=False)
async def redoc_html():
  return get_redoc_html(
    openapi_url=api.openapi_url,
    title=api.title + " - ReDoc",
    redoc_js_url="/static/redoc.standalone.js",
  )


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