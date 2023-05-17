from typing import List, Union
from pydantic import BaseModel

class NamerItem(BaseModel):
  name: str
  status: str=None
  ttl: str=None
  changed: str=None


class NamerItems(BaseModel):
  __root__: List[NamerItem]