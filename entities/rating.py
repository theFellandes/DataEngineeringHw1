from pydantic import BaseModel, PositiveInt, condecimal


class Rating(BaseModel):
    user_id: PositiveInt
    book_id: PositiveInt
    rating: condecimal(max_digits=3, decimal_places=2)
