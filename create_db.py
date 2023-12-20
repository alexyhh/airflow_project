# create_db.py
from sqlalchemy import create_engine

# Replace "sqlite:///absolute/path/to/your/database.db" with your actual connection string
db_connection = "sqlite:///C:\Users\alexl\DataEngineering\airflow_project\database.db"

engine = create_engine(db_connection)
engine.connect()
