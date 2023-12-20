# scripts/load_data.py
from sqlalchemy import create_engine

def load_data(data, db_connection):
    engine = create_engine(db_connection)

    # Replace "your_table" with the actual table name
    data.to_sql("your_table", con=engine, index=False, if_exists="replace")

if __name__ == "__main__":
    # Assuming you've already cleaned data in the previous step
    # Replace "path/to/your/data_cleaned.csv" with the actual path
    cleaned_data = pd.read_csv("C:\Users\alexl\DataEngineering\airflow_project\Data in csv\cleaned_data.csv")

    # Replace "sqlite:///your_database.db" with your actual database connection string
    db_connection = "sqlite:///C:\Users\alexl\DataEngineering\airflow_project\database.db"

    load_data(cleaned_data, db_connection)
