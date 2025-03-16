# book_tag.py
from pydantic import BaseModel, PositiveInt


class BookTag(BaseModel):
    book_id: PositiveInt
    tag_id: PositiveInt
