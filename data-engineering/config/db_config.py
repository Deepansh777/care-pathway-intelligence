"""Database configuration and connection utilities."""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'care_pathway')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres123')

# Connection string
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_engine():
    """Create and return a SQLAlchemy engine."""
    return create_engine(DATABASE_URL, echo=False)


def get_session():
    """Create and return a SQLAlchemy session."""
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def test_connection():
    """Test the database connection."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            print(f"✓ Successfully connected to PostgreSQL")
            print(f"  Version: {version}")
            return True
    except Exception as e:
        print(f"✗ Failed to connect to database")
        print(f"  Error: {str(e)}")
        return False


if __name__ == "__main__":
    test_connection()
