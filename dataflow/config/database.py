from sqlmodel import create_engine, Session, SQLModel
import os

# Database file path
DB_PATH = "dataflow_config.db"
sqlite_url = f"sqlite:///{DB_PATH}"

# Create engine
engine = create_engine(sqlite_url, echo=False)

def init_db():
    """Initialize the database and create tables."""
    SQLModel.metadata.create_all(engine)

def get_session():
    """Utility to get a basic session."""
    return Session(engine)
