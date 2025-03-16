from fastapi import FastAPI, HTTPException
from typing import List, Any
from pydantic import BaseModel
from sqlalchemy import create_engine, MetaData, Table, select, func
from sqlalchemy.orm import sessionmaker

app = FastAPI(title="Multi-Database Query API")

# Connection string for PostgreSQL (RDBMS and Data Warehouse)
POSTGRES_CONN_STR = "postgresql://myuser:mypassword@localhost:5432/data-hw1"

def get_relational_engine():
    engine = create_engine(POSTGRES_CONN_STR, pool_pre_ping=True)
    return engine

def get_table(engine, table_name: str) -> Table:
    metadata = MetaData()
    return Table(table_name, metadata, autoload_with=engine)

# ----- Relational Endpoints -----
@app.get("/api/relational/ratings_by_user/{user_id}", response_model=List[Any])
def relational_ratings_by_user(user_id: int):
    engine = get_relational_engine()
    ratings_table = get_table(engine, "ratings")
    with engine.connect() as conn:
        result = conn.execute(select(ratings_table).where(ratings_table.c.user_id == user_id)).fetchall()
    # Use row._mapping to convert to dict
    return [dict(row._mapping) for row in result]

@app.get("/api/relational/users_who_rated/{book_id}", response_model=List[Any])
def relational_users_who_rated(book_id: int):
    engine = get_relational_engine()
    metadata = MetaData()
    ratings_table = Table("ratings", metadata, autoload_with=engine)
    users_table = Table("users", metadata, autoload_with=engine)
    query = select(users_table.c.user_id, users_table.c.user_name)\
        .select_from(ratings_table.join(users_table, ratings_table.c.user_id == users_table.c.user_id))\
        .where(ratings_table.c.book_id == book_id)
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
    return [dict(row._mapping) for row in result]

@app.get("/api/relational/top5_books", response_model=List[Any])
def relational_top5_books():
    engine = get_relational_engine()
    metadata = MetaData()
    ratings_table = Table("ratings", metadata, autoload_with=engine)
    books_table = Table("books", metadata, autoload_with=engine)
    query = select(
                books_table.c.book_id,
                books_table.c.title,
                func.avg(ratings_table.c.rating).label("avg_rating")
            )\
            .select_from(books_table.join(ratings_table, books_table.c.book_id == ratings_table.c.book_id))\
            .group_by(books_table.c.book_id)\
            .order_by(func.avg(ratings_table.c.rating).desc())\
            .limit(5)
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
    return [dict(row._mapping) for row in result]

# ----- Data Warehouse Endpoints -----
@app.get("/api/dw/ratings_over_time", response_model=List[Any])
def dw_ratings_over_time():
    engine = get_relational_engine()
    metadata = MetaData()
    fact_ratings = Table("fact_ratings", metadata, autoload_with=engine)
    dim_time = Table("dim_time", metadata, autoload_with=engine)
    query = select(
                dim_time.c.date,
                func.count(fact_ratings.c.rating).label("total_ratings")
            )\
            .select_from(fact_ratings.join(dim_time, fact_ratings.c.time_id == dim_time.c.time_id))\
            .group_by(dim_time.c.date)
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
    return [dict(row._mapping) for row in result]

@app.get("/api/dw/top10_books", response_model=List[Any])
def dw_top10_books():
    engine = get_relational_engine()
    metadata = MetaData()
    fact_ratings = Table("fact_ratings", metadata, autoload_with=engine)
    dim_books = Table("dim_books", metadata, autoload_with=engine)
    query = select(
                dim_books.c.book_id,
                dim_books.c.title,
                func.avg(fact_ratings.c.rating).label("avg_rating")
            )\
            .select_from(fact_ratings.join(dim_books, fact_ratings.c.book_id == dim_books.c.book_id))\
            .group_by(dim_books.c.book_id)\
            .order_by(func.avg(fact_ratings.c.rating).desc())\
            .limit(10)
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
    return [dict(row._mapping) for row in result]

@app.get("/api/dw/ratings_for_genre/{genre}", response_model=dict)
def dw_ratings_for_genre(genre: str):
    engine = get_relational_engine()
    metadata = MetaData()
    fact_ratings = Table("fact_ratings", metadata, autoload_with=engine)
    dim_books = Table("dim_books", metadata, autoload_with=engine)
    query = select(func.count(fact_ratings.c.rating))\
            .select_from(fact_ratings.join(dim_books, fact_ratings.c.book_id == dim_books.c.book_id))\
            .where(dim_books.c.genre == genre)
    with engine.connect() as conn:
        total = conn.execute(query).scalar()
    return {"genre": genre, "total_ratings": total}
