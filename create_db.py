# create_db.py
import os
from sqlalchemy import create_engine

# Replace "sqlite:///absolute/path/to/your/database.db" with your actual connection string
db_filename = "database.db"
db_path = os.path.expanduser(f"~/Desktop/DataEngineering/{db_filename}")
db_connection = f"sqlite:///{db_path}"

engine = create_engine(db_connection)
engine.connect()
