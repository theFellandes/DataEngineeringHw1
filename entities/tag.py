from pydantic import BaseModel, PositiveInt


class Tag(BaseModel):
    tag_id: PositiveInt
    tag: str
