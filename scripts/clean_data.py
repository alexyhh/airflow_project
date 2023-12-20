# scripts/clean_data.py
import pandas as pd

def clean_data(data):
    # Add your data cleaning logic here
    cleaned_data = data.dropna()  # Placeholder example

    # Save cleaned data to a CSV file
    cleaned_data.to_csv("C:\Users\alexl\DataEngineering\airflow_project\Data in csv\cleaned_data.csv", index=False)

    return cleaned_data

if __name__ == "__main__":
    # Assuming you've already extracted data in the previous step
    # Replace "path/to/your/data.csv" with the actual path
    data = pd.read_csv("C:\Users\alexl\DataEngineering\airflow_project\Data in csv\source_data.csv")
    
    cleaned_data = clean_data(data)
    print(cleaned_data.head())

