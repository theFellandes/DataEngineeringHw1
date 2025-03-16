from random import random

from fastapi import FastAPI, HTTPException
from neo4j import GraphDatabase
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import List, Dict

# Configure your database connection
DATABASE_URL = "postgresql://myuser:mypassword@localhost:5432/postgres"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

# MongoDB connection URL and database/collection names
MONGO_URL = "mongodb://root:example@localhost:27017/?authSource=admin"
client = MongoClient(MONGO_URL)
db = client["data-hw1"]
users_collection = db["data-hw1"]

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "your_password"
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

app = FastAPI(title="Goodbooks API")


@app.get("/ratings/{user_id}", response_model=List[Dict])
def get_ratings_by_user(user_id: int):
    """
    Retrieve all ratings given by a specific user.
    """
    session = SessionLocal()
    try:
        query = text("SELECT * FROM ratings WHERE user_id = :user_id")
        result = session.execute(query, {"user_id": user_id})
        ratings = [dict(row) for row in result]
        if not ratings:
            raise HTTPException(status_code=404, detail="No ratings found for this user")
        return ratings
    finally:
        session.close()


@app.get("/users-rated/{book_id}", response_model=List[int])
def get_users_who_rated_book(book_id: int):
    """
    Find all distinct users who have rated a specific book.
    """
    session = SessionLocal()
    try:
        query = text("SELECT DISTINCT user_id FROM ratings WHERE book_id = :book_id")
        result = session.execute(query, {"book_id": book_id})
        users = [row.user_id for row in result]
        if not users:
            raise HTTPException(status_code=404, detail="No users found for this book")
        return users
    finally:
        session.close()


@app.get("/top-books", response_model=List[Dict])
def get_top_5_highest_rated_books():
    """
    Find the top 5 highest-rated books based on the average_rating field.
    """
    session = SessionLocal()
    try:
        query = text("SELECT * FROM books ORDER BY average_rating DESC LIMIT 5")
        result = session.execute(query)
        books = [dict(row) for row in result]
        if not books:
            raise HTTPException(status_code=404, detail="No books found")
        return books
    finally:
        session.close()


@app.post("/mongo/seed")
def seed_users():
    """
    Denormalize the data by embedding book ratings in each user document.
    Inserts 500 sample users with their corresponding book ratings.
    """
    # Only seed if the collection is empty
    if users_collection.count_documents({}) > 0:
        return {"message": "Data already seeded."}

    sample_users = []
    # Create 500 users; each gets a random number of ratings (between 1 and 10)
    for user_id in range(1, 501):
        ratings = []
        num_ratings = random.randint(1, 10)
        for _ in range(num_ratings):
            # Simulate a rating for a book (random book IDs between 1 and 1000)
            book_id = random.randint(1, 1000)
            title = f"Book {book_id}"
            rating = round(random.uniform(1, 5), 2)
            ratings.append({
                "book_id": book_id,  # this key is used by our insertion process
                "title": title,
                "rating": rating
            })
        user_doc = {
            "user_id": user_id,
            "name": f"User {user_id}",
            "ratings": ratings
        }
        sample_users.append(user_doc)

    result = users_collection.insert_many(sample_users)
    return {"message": f"Inserted {len(result.inserted_ids)} user documents."}


@app.get("/mongo/ratings/{user_id}", response_model=Dict)
def get_ratings_by_user(user_id: int):
    """
    Retrieve all ratings embedded in a specific user's document.
    """
    user = users_collection.find_one({"user_id": user_id}, {"_id": 0, "ratings": 1})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@app.get("/mongo/users-rated/{book_id}", response_model=List[int])
def get_users_who_rated_book(book_id: int):
    """
    Find all users who have rated a specific book by querying the embedded ratings.
    """
    # Query for user documents where any rating has the given book_id.
    cursor = users_collection.find({"ratings.book_id": book_id}, {"user_id": 1, "_id": 0})
    user_ids = [doc["user_id"] for doc in cursor]
    if not user_ids:
        raise HTTPException(status_code=404, detail="No users found for this book")
    return user_ids


@app.get("/mongo/top-books", response_model=List[Dict])
def get_top_5_highest_rated_books():
    """
    Aggregate across all users to compute the average rating per book,
    then return the top 5 highest-rated books.
    """
    pipeline = [
        {"$unwind": "$ratings"},
        {"$group": {
            "_id": "$ratings.book_id",
            "title": {"$first": "$ratings.title"},
            "avg_rating": {"$avg": "$ratings.rating"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"avg_rating": -1}},
        {"$limit": 5}
    ]
    top_books = list(users_collection.aggregate(pipeline))
    # Rename _id to book_id in the result for clarity.
    for book in top_books:
        book["book_id"] = book.pop("_id")
    if not top_books:
        raise HTTPException(status_code=404, detail="No books found")
    return top_books


@app.on_event("shutdown")
def shutdown():
    driver.close()


@app.get("/neo4j/ratings/{user_id}", response_model=List[Dict])
def get_ratings_by_user(user_id: int):
    """
    Retrieve all ratings given by a specific user.
    Returns book id, title, and the rating given by the user.
    """
    query = """
    MATCH (u:User {user_id: $user_id})-[r:RATED]->(b:Book)
    RETURN b.book_id AS book_id, b.title AS title, r.rating AS rating
    """
    with driver.session() as session:
        result = session.run(query, user_id=user_id)
        records = [record.data() for record in result]
        if not records:
            raise HTTPException(status_code=404, detail="No ratings found for this user")
        return records


@app.get("/neo4j/users-rated/{book_id}", response_model=List[int])
def get_users_who_rated_book(book_id: int):
    """
    Find users who have rated a specific book.
    Returns a list of user IDs.
    """
    query = """
    MATCH (u:User)-[r:RATED]->(b:Book {book_id: $book_id})
    RETURN DISTINCT u.user_id AS user_id
    """
    with driver.session() as session:
        result = session.run(query, book_id=book_id)
        user_ids = [record["user_id"] for record in result]
        if not user_ids:
            raise HTTPException(status_code=404, detail="No users found for this book")
        return user_ids


@app.get("/neo4j/top-books", response_model=List[Dict])
def get_top_5_highest_rated_books():
    """
    Find the top 5 highest-rated books.
    Computes the average rating across all RATED relationships.
    Returns book id, title, and average rating.
    """
    query = """
    MATCH (u:User)-[r:RATED]->(b:Book)
    WITH b, avg(r.rating) AS avg_rating
    RETURN b.book_id AS book_id, b.title AS title, avg_rating
    ORDER BY avg_rating DESC
    LIMIT 5
    """
    with driver.session() as session:
        result = session.run(query)
        top_books = [record.data() for record in result]
        if not top_books:
            raise HTTPException(status_code=404, detail="No books found")
        return top_books


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
