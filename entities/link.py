from pydantic import BaseModel, AnyUrl, PositiveInt, constr
from typing import Optional


class Link(BaseModel):
    book_id: PositiveInt
    goodreads_book_id: PositiveInt
    best_book_id: PositiveInt
    work_id: PositiveInt
    books_count: PositiveInt
    isbn: Optional[constr(strip_whitespace=True, min_length=10, max_length=13)]
    isbn13: Optional[constr(strip_whitespace=True, min_length=13, max_length=13)]
    authors: str
    original_publication_year: Optional[int]
    original_title: Optional[str]
    title: str
    language_code: Optional[constr(strip_whitespace=True, min_length=2, max_length=3)]
    average_rating: float
    ratings_count: PositiveInt
    image_url: AnyUrl
    small_image_url: AnyUrl
