-- PostgreSQL Schema for Goodbooks 10K

-- Drop tables in order to avoid dependency issues
DROP TABLE IF EXISTS book_tags;
DROP TABLE IF EXISTS to_read;
DROP TABLE IF EXISTS links;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS books;

-- Create the books table using "goodreads_book_id" as the primary key
CREATE TABLE books (
    goodreads_book_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    authors TEXT NOT NULL,
    average_rating NUMERIC(3,2),
    isbn VARCHAR(20),        -- Increased from 13 to 20 characters
    isbn13 VARCHAR(20),      -- Increased from 13 to 20 characters
    language_code VARCHAR(10), -- Increased to allow longer language codes if necessary
    num_pages INTEGER,
    ratings_count INTEGER,
    text_reviews_count INTEGER,
    publication_date TEXT,
    publisher TEXT
);

-- Create the ratings table
CREATE TABLE ratings (
    user_id INTEGER,
    goodreads_book_id INTEGER,
    rating NUMERIC(3,2),
    PRIMARY KEY (user_id, goodreads_book_id),
    FOREIGN KEY (goodreads_book_id) REFERENCES books(goodreads_book_id)
);

-- Create the tags table
CREATE TABLE tags (
    tag_id INTEGER PRIMARY KEY,
    tag TEXT NOT NULL
);

-- Create the book_tags linking table
CREATE TABLE book_tags (
    goodreads_book_id INTEGER,
    tag_id INTEGER,
    PRIMARY KEY (goodreads_book_id, tag_id),
    FOREIGN KEY (goodreads_book_id) REFERENCES books(goodreads_book_id),
    FOREIGN KEY (tag_id) REFERENCES tags(tag_id)
);

-- Create the to_read table
CREATE TABLE to_read (
    user_id INTEGER,
    goodreads_book_id INTEGER,
    PRIMARY KEY (user_id, goodreads_book_id),
    FOREIGN KEY (goodreads_book_id) REFERENCES books(goodreads_book_id)
);

-- Create the links table
CREATE TABLE links (
    goodreads_book_id INTEGER PRIMARY KEY,
    link TEXT NOT NULL,
    FOREIGN KEY (goodreads_book_id) REFERENCES books(goodreads_book_id)
);
