# scripts/clean_data.py
import pandas as pd


def clean_data(csv_path):
    data =  pd.read_csv(csv_path)
    # Perform cleaning
    cleaned_data = data.dropna()
    cleaned_data.to_csv("/home/alex/Desktop/DataEngineering/Data in csv/cleaned_data.csv", index=False)

    return cleaned_data


if __name__ == "__main__":
    # Assuming you've already extracted data in the previous step
    # Replace "path/to/your/data.csv" with the actual path
    cleaned_data = clean_data("/home/alex/Desktop/DataEngineering/Data in csv/source_data.csv")

    print(cleaned_data.head())
