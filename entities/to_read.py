from pydantic import BaseModel, PositiveInt


class ToRead(BaseModel):
    user_id: PositiveInt
    book_id: PositiveInt

