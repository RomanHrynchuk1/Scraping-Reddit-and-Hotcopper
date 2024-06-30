"""
This contains all definitions and functions related to the database.
"""

import os
from dotenv import load_dotenv

from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    MetaData,
    DateTime,
    Text,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

# Load environment variables
load_dotenv()

# Create database engine
engine = create_engine(os.getenv("DATABASE_URL"))
metadata = MetaData()

# Define tables
StockTable = Table(
    "stock",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("stk", String(10)),  # Specify length for String columns
)

PostsTable = Table(
    "posts",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("type", String(10)),  # Specify length for String columns
    Column("url", String(255)),  # Specify length for String columns
    Column("post_id", String(20)),  # Specify length for String columns
    Column("title", String(50)),  # Specify length for String columns
    Column("description", Text),
    Column("stockid", Integer),
    Column("stockname", String(10)),  # Specify length for String columns
    Column("timestamp", DateTime, server_default=func.now()),
)

CommentsTable = Table(
    "comments",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("text", Text),
    Column("author", String(20)),  # Specify length for String columns
    Column("postid", Integer),  # Specify length for String columns
    Column("stockid", Integer),
    Column("stockname", String(8)),  # Specify length for String columns
)

# Create tables in the database
metadata.create_all(engine)

# Create a configured "Session" class
Session = sessionmaker(bind=engine)


# Functions
def get_stock_values() -> list[dict]:
    """Fetch all stock values from the database and return them as a list of dictionaries."""
    session = Session()
    stock_values = []
    try:
        stocks = session.query(StockTable).all()
        for stock in stocks:
            stock_values.append({"id": stock.id, "stk": stock.stk})
    except Exception as e:
        print(f"Error fetching stock values: {e}")
    finally:
        session.close()
    return stock_values


def insert_stock_values(stocks: list[str]) -> bool:
    """Insert a list of stock values into the database."""
    session = Session()
    try:
        for stock in stocks:
            session.execute(StockTable.insert().values(stk=stock))
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        print(f"Error setting stock values: {e}")
        return False
    finally:
        session.close()


def insert_into_posts(
    type: str,
    url: str,
    post_id: str,
    title: str,
    description: str,
    stockid: int,
    stockname: str,
) -> int:
    """Insert a post into the posts table and return the id of the inserted post."""
    session = Session()
    try:
        # Truncate strings to fit the column definitions
        type = type[:10]
        url = url[:255]
        post_id = post_id[:20]
        title = title[:50]
        stockname = stockname[:10]

        result = session.execute(
            PostsTable.insert().values(
                type=type,
                url=url,
                post_id=post_id,
                title=title,
                description=description,
                stockid=stockid,
                stockname=stockname,
            )
        )
        session.commit()
        post_id = result.inserted_primary_key[0]
        return post_id
    except Exception as e:
        session.rollback()
        print(f"Error inserting post: {e}")
        return -1
    finally:
        session.close()


def get_posts_url() -> list[str]:
    """Fetch all URLs from the posts table and return them as a list of strings."""
    session = Session()
    urls = []
    try:
        # Use query().values() to fetch only the 'url' column
        urls = [url for (url,) in session.query(PostsTable.c.url)]
    except Exception as e:
        print(f"Error fetching posts URLs: {e}")
    finally:
        session.close()
    return urls


def insert_into_comments(
    text: str, author: str, postid: int, stockid: int, stockname: str
) -> int:
    """Insert a comment into the comments table and return the id of the inserted comment."""
    session = Session()
    try:
        # Truncate strings to fit the column definitions
        author = author[:20]
        stockname = stockname[:8]

        result = session.execute(
            CommentsTable.insert().values(
                text=text,
                author=author,
                postid=postid,
                stockid=stockid,
                stockname=stockname,
            )
        )
        session.commit()
        comment_id = result.inserted_primary_key[0]
        return comment_id
    except Exception as e:
        session.rollback()
        print(f"Error inserting comment: {e}")
        return -1
    finally:
        session.close()
