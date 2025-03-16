import os
import time
import pandas as pd
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, ForeignKey, select, func, insert


# -----------------------------------------------------
# Data Loading with Kaggle API
# -----------------------------------------------------
def download_dataset_if_needed():
    dataset_folder = 'data'
    books_csv = os.path.join(dataset_folder, 'books.csv')
    ratings_csv = os.path.join(dataset_folder, 'ratings.csv')

    if not os.path.exists(books_csv) or not os.path.exists(ratings_csv):
        print("Downloading Goodbooks-10k-updated dataset from Kaggle...")
        os.makedirs(dataset_folder, exist_ok=True)
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files('alexanderfrosati/goodbooks-10k-updated', path=dataset_folder, unzip=True)
        print("Download complete.")


def load_goodbooks_data():
    download_dataset_if_needed()
    dataset_folder = 'data'
    books_path = os.path.join(dataset_folder, 'books.csv')
    ratings_path = os.path.join(dataset_folder, 'ratings.csv')

    # Optional additional CSVs (if needed later)
    tags_path = os.path.join(dataset_folder, 'tags.csv')
    book_tags_path = os.path.join(dataset_folder, 'book_tags.csv')

    books_df = pd.read_csv(books_path)
    ratings_df = pd.read_csv(ratings_path)
    tags_df = pd.read_csv(tags_path) if os.path.exists(tags_path) else None
    book_tags_df = pd.read_csv(book_tags_path) if os.path.exists(book_tags_path) else None

    # Derive users from ratings.
    user_ids = ratings_df['user_id'].unique()
    users_df = pd.DataFrame({'user_id': user_ids})
    users_df['user_name'] = users_df['user_id'].apply(lambda x: f'User{x}')
    users_df['email'] = users_df['user_id'].apply(lambda x: f'user{x}@example.com')

    return {
        'users': users_df.to_dict(orient='records'),
        'books': books_df.to_dict(orient='records'),
        'ratings': ratings_df.to_dict(orient='records'),
        'tags': tags_df.to_dict(orient='records') if tags_df is not None else [],
        'book_tags': book_tags_df.to_dict(orient='records') if book_tags_df is not None else []
    }


# -----------------------------------------------------
# RelationalDBSetup
# -----------------------------------------------------
class RelationalDBSetup:
    def __init__(self, connection_string):
        # Use pool_pre_ping=True to ensure persistent connections
        self.engine = create_engine(connection_string, pool_pre_ping=True)
        self.metadata = MetaData()
        # Users table
        self.users_table = Table('users', self.metadata,
                                 Column('user_id', Integer, primary_key=True),
                                 Column('user_name', String),
                                 Column('email', String))
        # Books table
        self.books_table = Table('books', self.metadata,
                                 Column('book_id', Integer, primary_key=True),
                                 Column('title', String),
                                 Column('authors', String),
                                 Column('average_rating', Float),
                                 Column('isbn', String),
                                 Column('isbn13', String),
                                 Column('language_code', String),
                                 Column('num_pages', Integer),
                                 Column('ratings_count', Integer),
                                 Column('text_reviews_count', Integer),
                                 Column('publication_date', String),
                                 Column('publisher', String))
        # Ratings table with foreign keys
        self.ratings_table = Table('ratings', self.metadata,
                                   Column('rating_id', Integer, primary_key=True, autoincrement=True),
                                   Column('user_id', Integer, ForeignKey('users.user_id')),
                                   Column('book_id', Integer, ForeignKey('books.book_id')),
                                   Column('rating', Float))

    def create_schema(self):
        # Drop and re-create schema to match our design.
        self.metadata.drop_all(self.engine, checkfirst=True)
        self.metadata.create_all(self.engine)
        print("Relational schema (Users, Books, Ratings) created.")

    def insert_sample_data(self, users, books, ratings):
        with self.engine.begin() as connection:
            connection.execute(self.users_table.insert(), users)
            connection.execute(self.books_table.insert(), books)
            connection.execute(self.ratings_table.insert(), ratings)
        print("Sample data inserted into relational database.")

    def run_queries(self, specific_user_id, specific_book_id):
        conn = self.engine.connect()
        try:
            # Query a: Retrieve all ratings given by a specific user.
            query_a = select(self.ratings_table).where(self.ratings_table.c.user_id == specific_user_id)
            results_a = conn.execute(query_a).fetchall()
            print("Relational Query - Ratings by user", specific_user_id, ":", results_a)

            # Query b: Find users who have rated a specific book.
            # Instead of selecting only user_id from ratings, join with the users table to get more details.
            query_b = select(self.users_table.c.user_id, self.users_table.c.user_name) \
                .select_from(self.ratings_table.join(self.users_table,
                                                     self.ratings_table.c.user_id == self.users_table.c.user_id)) \
                .where(self.ratings_table.c.book_id == specific_book_id)
            results_b = conn.execute(query_b).fetchall()
            print("Relational Query - Users who rated book", specific_book_id, ":", results_b)

            # Query c: Top 5 highest-rated books by average rating.
            query_c = select(self.books_table.c.book_id,
                             self.books_table.c.title,
                             func.avg(self.ratings_table.c.rating).label('avg_rating')) \
                .select_from(self.books_table.join(self.ratings_table,
                                                   self.books_table.c.book_id == self.ratings_table.c.book_id)) \
                .group_by(self.books_table.c.book_id) \
                .order_by(func.avg(self.ratings_table.c.rating).desc()) \
                .limit(5)
            results_c = conn.execute(query_c).fetchall()
            print("Relational Query - Top 5 highest-rated books:", results_c)
        finally:
            conn.close()

    def close(self):
        self.engine.dispose()
        print("Closed Relational DB connection.")


# -----------------------------------------------------
# DocumentDBSetup (MongoDB)
# -----------------------------------------------------
from pymongo import MongoClient, errors as mongo_errors


class DocumentDBSetup:
    def __init__(self, uri, database_name='data-hw1', collection_name='users'):
        self.uri = uri
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.database_name]
            print("Connected to MongoDB")
        except mongo_errors.PyMongoError as e:
            print("Error connecting to MongoDB:", e)

    def insert_sample_data(self, data):
        self.connect()
        try:
            result = self.db[self.collection_name].insert_many(data)
            print(f"Inserted {len(result.inserted_ids)} documents into MongoDB")
        except mongo_errors.PyMongoError as e:
            print("Error inserting into MongoDB:", e)

    def run_queries(self, specific_user_id, specific_book_id):
        result_a = self.db[self.collection_name].find_one({'user_id': specific_user_id}, {'ratings': 1})
        print("Document Query - Ratings for user", specific_user_id, ":", result_a.get('ratings') if result_a else None)
        result_b = self.db[self.collection_name].find({'ratings.book_id': specific_book_id}, {'user_id': 1})
        users_for_book = [doc['user_id'] for doc in result_b]
        print("Document Query - Users who rated book", specific_book_id, ":", users_for_book)
        pipeline = [
            {'$unwind': '$ratings'},
            {'$group': {'_id': '$ratings.book_id', 'avg_rating': {'$avg': '$ratings.rating'}}},
            {'$sort': {'avg_rating': -1}},
            {'$limit': 5}
        ]
        result_c = list(self.db[self.collection_name].aggregate(pipeline))
        print("Document Query - Top 5 highest-rated books:", result_c)

    def close(self):
        if self.client:
            self.client.close()
        print("Closed MongoDB connection.")


# -----------------------------------------------------
# GraphDBSetup (Neo4j)
# -----------------------------------------------------
from neo4j import GraphDatabase


class GraphDBSetup:
    def __init__(self, uri, user, password, retry=5, delay=5):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = None
        for attempt in range(retry):
            try:
                self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
                with self.driver.session() as session:
                    session.run("RETURN 1")
                print("Connected to Neo4j")
                break
            except Exception as e:
                print(f"Attempt {attempt + 1} - Failed to connect to Neo4j, retrying in {delay} seconds...", e)
                time.sleep(delay)
        if self.driver is None:
            raise Exception("Could not connect to Neo4j after several attempts")

    def insert_sample_data(self, users, books, ratings):
        def create_user(tx, user):
            query = "MERGE (u:User {user_id: $user_id}) SET u.user_name = $user_name"
            tx.run(query, **user)

        def create_book(tx, book):
            query = "MERGE (b:Book {book_id: $book_id}) SET b.title = $title"
            tx.run(query, **book)

        def create_rating(tx, rating):
            query = """
            MATCH (u:User {user_id: $user_id}), (b:Book {book_id: $book_id})
            MERGE (u)-[r:RATED]->(b)
            SET r.rating = $rating
            """
            tx.run(query, **rating)

        with self.driver.session() as session:
            for user in users:
                session.write_transaction(create_user, user)
            for book in books:
                session.write_transaction(create_book, book)
            for rating in ratings:
                session.write_transaction(create_rating, rating)
        print("Graph data inserted into Neo4j.")

    def run_queries(self, specific_user_id, specific_book_id):
        with self.driver.session() as session:
            query_a = """
            MATCH (u:User {user_id: $user_id})-[r:RATED]->(b:Book)
            RETURN b.title AS book, r.rating AS rating
            """
            result_a = session.run(query_a, user_id=specific_user_id)
            print("Graph Query - Ratings by user", specific_user_id, ":", list(result_a))

            query_b = """
            MATCH (u:User)-[r:RATED]->(b:Book {book_id: $book_id})
            RETURN u.user_id AS user_id, u.user_name AS user_name
            """
            result_b = session.run(query_b, book_id=specific_book_id)
            print("Graph Query - Users who rated book", specific_book_id, ":", list(result_b))

            query_c = """
            MATCH (:User)-[r:RATED]->(b:Book)
            WITH b, avg(r.rating) AS avgRating
            RETURN b.book_id AS book_id, b.title AS title, avgRating
            ORDER BY avgRating DESC
            LIMIT 5
            """
            result_c = session.run(query_c)
            print("Graph Query - Top 5 highest-rated books:", list(result_c))

    def close(self):
        if self.driver:
            self.driver.close()
        print("Closed Neo4j connection.")


# -----------------------------------------------------
# DataWarehouseSetup (Star Schema)
# -----------------------------------------------------
class DataWarehouseSetup:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string, pool_pre_ping=True)
        self.metadata = MetaData()
        self.dim_users = Table('dim_users', self.metadata,
                               Column('user_id', Integer, primary_key=True),
                               Column('user_name', String))
        self.dim_books = Table('dim_books', self.metadata,
                               Column('book_id', Integer, primary_key=True),
                               Column('title', String),
                               Column('genre', String))
        self.dim_time = Table('dim_time', self.metadata,
                              Column('time_id', Integer, primary_key=True),
                              Column('date', String))
        self.fact_ratings = Table('fact_ratings', self.metadata,
                                  Column('rating_id', Integer, primary_key=True, autoincrement=True),
                                  Column('user_id', Integer),
                                  Column('book_id', Integer),
                                  Column('time_id', Integer),
                                  Column('rating', Float))

    def create_schema(self):
        self.metadata.drop_all(self.engine, checkfirst=True)
        self.metadata.create_all(self.engine)
        print("Data Warehouse (star schema) created.")

    def insert_sample_data(self, users, books, time_dim, ratings):
        with self.engine.begin() as conn:
            conn.execute(self.dim_users.insert(), users)
            conn.execute(self.dim_books.insert(), books)
            conn.execute(self.dim_time.insert(), time_dim)
            conn.execute(self.fact_ratings.insert(), ratings)
        print("Data Warehouse sample data inserted.")

    def run_queries(self, genre_filter):
        conn = self.engine.connect()
        try:
            query_a = select(self.dim_time.c.date, func.count(self.fact_ratings.c.rating).label('total_ratings')) \
                .select_from(
                self.fact_ratings.join(self.dim_time, self.fact_ratings.c.time_id == self.dim_time.c.time_id)) \
                .group_by(self.dim_time.c.date)
            results_a = conn.execute(query_a).fetchall()
            print("DW Query - Total ratings over time:", results_a)

            query_b = select(self.dim_books.c.book_id, self.dim_books.c.title,
                             func.avg(self.fact_ratings.c.rating).label('avg_rating')) \
                .select_from(
                self.fact_ratings.join(self.dim_books, self.fact_ratings.c.book_id == self.dim_books.c.book_id)) \
                .group_by(self.dim_books.c.book_id) \
                .order_by(func.avg(self.fact_ratings.c.rating).desc()) \
                .limit(10)
            results_b = conn.execute(query_b).fetchall()
            print("DW Query - Top 10 highest-rated books:", results_b)

            query_c = select(func.count(self.fact_ratings.c.rating)) \
                .select_from(
                self.fact_ratings.join(self.dim_books, self.fact_ratings.c.book_id == self.dim_books.c.book_id)) \
                .where(self.dim_books.c.genre == genre_filter)
            result_c = conn.execute(query_c).scalar()
            print(f"DW Query - Total ratings for genre '{genre_filter}':", result_c)
        finally:
            conn.close()

    def close(self):
        self.engine.dispose()
        print("Closed Data Warehouse connection.")


# -----------------------------------------------------
# Task functions (each will run concurrently)
# -----------------------------------------------------
def run_relational_task(users_sample, books_sample, ratings_sample):
    relational_conn_str = 'postgresql://myuser:mypassword@localhost:5432/data-hw1'
    relational_db = RelationalDBSetup(relational_conn_str)
    relational_db.create_schema()
    relational_db.insert_sample_data(users_sample, books_sample, ratings_sample)
    relational_db.run_queries(specific_user_id=users_sample[0]['user_id'],
                              specific_book_id=books_sample[0]['book_id'])
    relational_db.close()


def run_document_task(users_sample, books_sample, ratings_sample):
    mongo_uri = 'mongodb://root:example@localhost:27017'
    document_db = DocumentDBSetup(mongo_uri, database_name='data-hw1', collection_name='users')
    books_map = {book['book_id']: book for book in books_sample}
    user_documents = []
    user_ratings = defaultdict(list)
    for r in ratings_sample:
        user_ratings[r['user_id']].append(r)
    for user in users_sample:
        uid = user['user_id']
        ratings_embedded = []
        for rating in user_ratings.get(uid, []):
            rating_copy = rating.copy()
            rating_copy['title'] = books_map.get(rating['book_id'], {}).get('title', '')
            ratings_embedded.append(rating_copy)
        user_doc = user.copy()
        user_doc['ratings'] = ratings_embedded
        user_documents.append(user_doc)
    document_db.insert_sample_data(user_documents)
    document_db.run_queries(specific_user_id=users_sample[0]['user_id'],
                            specific_book_id=books_sample[0]['book_id'])
    document_db.close()


def run_graph_task(users_sample, books_sample, ratings_sample):
    try:
        neo4j_db = GraphDBSetup('bolt://localhost:7687', 'neo4j', 'your_password')
    except Exception as e:
        print("Neo4j connection failed:", e)
        return
    neo4j_db.insert_sample_data(users_sample, books_sample, ratings_sample)
    neo4j_db.run_queries(specific_user_id=users_sample[0]['user_id'],
                         specific_book_id=books_sample[0]['book_id'])
    neo4j_db.close()


def run_dw_task(users_sample, books_sample, ratings_sample):
    dw_conn_str = 'postgresql://myuser:mypassword@localhost:5432/data-hw1'
    dw_setup = DataWarehouseSetup(dw_conn_str)
    dw_setup.create_schema()
    dw_users_sample = users_sample
    dw_books_sample = [{'book_id': b['book_id'], 'title': b['title'], 'genre': 'Fiction'} for b in books_sample]
    dw_time_sample = [{'time_id': i + 1, 'date': '2020-01-01'} for i in range(10)]
    dw_ratings_sample = []
    for i, rating in enumerate(ratings_sample):
        time_id = (i % len(dw_time_sample)) + 1
        rating_record = rating.copy()
        rating_record['time_id'] = time_id
        dw_ratings_sample.append(rating_record)
    dw_setup.insert_sample_data(dw_users_sample, dw_books_sample, dw_time_sample, dw_ratings_sample)
    dw_setup.run_queries(genre_filter='Fiction')
    dw_setup.close()


# -----------------------------------------------------
# Main function using multithreading
# -----------------------------------------------------
def main():
    data = load_goodbooks_data()
    users_all = data['users']
    books_all = data['books']
    ratings_all = data['ratings']

    # Sampling and Filtering:
    books_sample = books_all[:500]
    selected_book_ids = set(book['book_id'] for book in books_sample)
    ratings_filtered = [r for r in ratings_all if r['book_id'] in selected_book_ids]
    ratings_sample = ratings_filtered[:2000]
    selected_user_ids = set(r['user_id'] for r in ratings_sample)
    users_sample = [u for u in users_all if u['user_id'] in selected_user_ids]

    # Run all tasks concurrently.
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        futures.append(executor.submit(run_relational_task, users_sample, books_sample, ratings_sample))
        futures.append(executor.submit(run_document_task, users_sample, books_sample, ratings_sample))
        futures.append(executor.submit(run_graph_task, users_sample, books_sample, ratings_sample))
        futures.append(executor.submit(run_dw_task, users_sample, books_sample, ratings_sample))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print("Task encountered an error:", e)


if __name__ == '__main__':
    main()
