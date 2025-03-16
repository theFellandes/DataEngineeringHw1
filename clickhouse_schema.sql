-- ClickHouse Schema for Goodbooks 10K Dataset

DROP TABLE IF EXISTS book_tags;
DROP TABLE IF EXISTS to_read;
DROP TABLE IF EXISTS links;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS books;

-- Create the books table with "goodreads_book_id"
CREATE TABLE books (
    goodreads_book_id Int32,
    title String,
    authors String,
    average_rating Float32,
    isbn Nullable(String),
    isbn13 Nullable(String),
    language_code Nullable(String),
    num_pages Nullable(Int32),
    ratings_count Int32,
    text_reviews_count Int32,
    publication_date Nullable(String),
    publisher Nullable(String)
) ENGINE = MergeTree()
ORDER BY goodreads_book_id;

-- Create the ratings table
CREATE TABLE ratings (
    user_id Int32,
    goodreads_book_id Int32,
    rating Float32
) ENGINE = MergeTree()
ORDER BY (user_id, goodreads_book_id);

-- Create the tags table
CREATE TABLE tags (
    tag_id Int32,
    tag String
) ENGINE = MergeTree()
ORDER BY tag_id;

-- Create the book_tags linking table
CREATE TABLE book_tags (
    goodreads_book_id Int32,
    tag_id Int32
) ENGINE = MergeTree()
ORDER BY (goodreads_book_id, tag_id);

-- Create the to_read table
CREATE TABLE to_read (
    user_id Int32,
    goodreads_book_id Int32
) ENGINE = MergeTree()
ORDER BY (user_id, goodreads_book_id);

-- Create the links table
CREATE TABLE links (
    goodreads_book_id Int32,
    link String
) ENGINE = MergeTree()
ORDER BY goodreads_book_id;
