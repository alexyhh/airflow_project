# scripts/load_data.py
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, MetaData
from airflow.exceptions import AirflowException

def load_data(**kwargs):
    # Access cleaned data from XCom
    ti = kwargs['ti']
    cleaned_data = ti.xcom_pull(task_ids='clean_data', key='return_value')

    if not isinstance(cleaned_data, pd.DataFrame):
        raise AirflowException("XCom returned an unexpected data type. Expected DataFrame.")

    # Your database connection string
    db_connection = "sqlite:////home/alex/Desktop/DataEngineering/database.db"

    # Your code to create table and load data
    engine = create_engine(db_connection)
    metadata = MetaData()

    # Define the structure of your table
    data_table = Table(
        'data_table',
        metadata,
        Column('column1', Integer),
        Column('column2', Integer),
        Column('column3', Integer),
        Column('column4', Integer),
        # Add more columns as needed
    )

    # Create the table if it doesn't exist
    metadata.create_all(engine)

    # Replace "your_table" with the actual table name
    cleaned_data.to_sql("data_table", con=engine, index=False, if_exists="replace")

if __name__ == "__main__":
    # Assuming you've already cleaned data in the previous step
    # Replace "path/to/your/data_cleaned.csv" with the actual path
    # cleaned_data = pd.read_csv("/home/alex/Desktop/DataEngineering/Data in csv/cleaned_data.csv")
    ti = kwargs['ti']
    cleaned_data = ti.xcom_pull(task_ids='clean_data', key='return_value')

    # Replace "sqlite:///your_database.db" with your actual database connection string
    db_connection = "sqlite:////home/alex/Desktop/DataEngineering/database.db"

    load_data(cleaned_data, db_connection=db_connection)
