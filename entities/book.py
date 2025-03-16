from pydantic import BaseModel, PositiveInt, constr, condecimal
from typing import Optional


class Book(BaseModel):
    book_id: PositiveInt
    title: str
    authors: str
    average_rating: Optional[condecimal(max_digits=3, decimal_places=2)]
    isbn: Optional[constr(strip_whitespace=True, min_length=10, max_length=13)]
    isbn13: Optional[constr(strip_whitespace=True, min_length=13, max_length=13)]
    language_code: Optional[constr(strip_whitespace=True, min_length=2, max_length=3)]
    num_pages: Optional[int]
    ratings_count: int
    text_reviews_count: int
    publication_date: Optional[str]
    publisher: Optional[str]
