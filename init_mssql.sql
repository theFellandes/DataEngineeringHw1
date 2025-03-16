-- Drop tables if they exist (in proper order due to foreign keys)
IF OBJECT_ID('dbo.book_tags', 'U') IS NOT NULL DROP TABLE dbo.book_tags;
IF OBJECT_ID('dbo.to_read', 'U') IS NOT NULL DROP TABLE dbo.to_read;
IF OBJECT_ID('dbo.links', 'U') IS NOT NULL DROP TABLE dbo.links;
IF OBJECT_ID('dbo.ratings', 'U') IS NOT NULL DROP TABLE dbo.ratings;
IF OBJECT_ID('dbo.tags', 'U') IS NOT NULL DROP TABLE dbo.tags;
IF OBJECT_ID('dbo.books', 'U') IS NOT NULL DROP TABLE dbo.books;

-- Create the books table
CREATE TABLE dbo.books (
    book_id INT PRIMARY KEY,
    title NVARCHAR(255) NOT NULL,
    authors NVARCHAR(255) NOT NULL,
    average_rating DECIMAL(3,2),
    isbn NVARCHAR(13),
    isbn13 NVARCHAR(13),
    language_code NVARCHAR(3),
    num_pages INT,
    ratings_count INT,
    text_reviews_count INT,
    publication_date NVARCHAR(50),
    publisher NVARCHAR(255)
);

-- Create the ratings table
CREATE TABLE dbo.ratings (
    user_id INT NOT NULL,
    book_id INT NOT NULL,
    rating DECIMAL(3,2),
    CONSTRAINT PK_ratings PRIMARY KEY (user_id, book_id),
    CONSTRAINT FK_ratings_books FOREIGN KEY (book_id) REFERENCES dbo.books(book_id)
);

-- Create the tags table
CREATE TABLE dbo.tags (
    tag_id INT PRIMARY KEY,
    tag NVARCHAR(255) NOT NULL
);

-- Create the book_tags linking table
CREATE TABLE dbo.book_tags (
    book_id INT NOT NULL,
    tag_id INT NOT NULL,
    CONSTRAINT PK_book_tags PRIMARY KEY (book_id, tag_id),
    CONSTRAINT FK_book_tags_books FOREIGN KEY (book_id) REFERENCES dbo.books(book_id),
    CONSTRAINT FK_book_tags_tags FOREIGN KEY (tag_id) REFERENCES dbo.tags(tag_id)
);

-- Create the to_read table
CREATE TABLE dbo.to_read (
    user_id INT NOT NULL,
    book_id INT NOT NULL,
    CONSTRAINT PK_to_read PRIMARY KEY (user_id, book_id),
    CONSTRAINT FK_to_read_books FOREIGN KEY (book_id) REFERENCES dbo.books(book_id)
);

-- Create the links table
CREATE TABLE dbo.links (
    book_id INT PRIMARY KEY,
    link NVARCHAR(1024) NOT NULL,
    CONSTRAINT FK_links_books FOREIGN KEY (book_id) REFERENCES dbo.books(book_id)
);
