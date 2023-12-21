This is a program that demo how Airflow can be used to orchestrate a simple ETL.

We start with uncleaned data "source_data.csv". First we extract the csv to dataframe, next we perform simple transformation to drop NA values and save it to cleaned_data.csv
Lastly we load the data to database.

This makes up a total of 3 steps which are in 3 separate python files clean_data, extract_data and load_data. All 3 files are in "scripts".

All csv files are located in "Data in csv"

Preparation:
To prepare for this ETL job a database is being created using the create_db.py

We also need to install Airflow using "pip install apache airflow"
We also need to navigate to the Airflow.cfg file to add in our directory paths containing my_dag.py

Running Airflow:
Using terminal we need to
1) start airflow with "airflow db init"
2) start the webserver with "airflow webserver --port 8080"
3) start airflow scheduler with "airflow scheduler"

We can now navigate to the browser to view the airflow UI with http://localhost:8080