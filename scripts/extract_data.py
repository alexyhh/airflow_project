# scripts/extract_data.py
import pandas as pd

def extract_data(csv_path):
    return pd.read_csv(csv_path)

if __name__ == "__main__":
    data = extract_data("C:\Users\alexl\DataEngineering\airflow_project\Data in csv\source_data.csv")
    print(data.head())
