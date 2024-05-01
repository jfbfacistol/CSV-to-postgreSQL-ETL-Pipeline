# Data Engineering Series Challenge: ETL Pipeline with Python and PostgreSQL

Hello! Welcome to my 1st project for the Data Engineering Series Challenge. In this project, I've developed a simple ETL (Extract, Transform, Load) pipeline using Python. The purpose of this pipeline is to efficiently extract data from a CSV file, transform it as needed, and load it into a PostgreSQL database using pgAdmin4.

## Overview

The ETL process involves three main steps:

1. **Extract**: Data is extracted from a CSV file.
2. **Transform**: The extracted data is cleaned and transformed into a suitable format.
3. **Load**: The transformed data is loaded into a PostgreSQL database.

For this project, I've chosen to work with a CSV file named `spotify-2023.csv` as our data source.

## Usage

To use this ETL pipeline:

1. Make sure you have Python installed on your system.
2. Install the required dependencies by running:
    pip install pandas chardet sqlalchemy psycopg2 
3. Ensure you have PostgreSQL installed and running, with pgAdmin4 as the administration platform.
4. Clone this repository or download the `spotifydatapipeline.py` file. 
5. Replace the `spotify-2023.csv` file with your desired CSV file, or rename your CSV file to match. 
6. Open `spotifydatapipeline.py` and adjust any configuration settings as necessary (such as database connection details). Make sure to use and modify the template `config.py` to your own postgreSQL settings. 
7. Run the script:
    python spotifydatapipeline.py


## File Structure

- `spotifydatapipeline.py`: Python script containing the ETL pipeline.
- `spotify-2023.csv`: Sample CSV file used as the data source.

## Detailed Process

1. **Extracting Data**: The pipeline reads data from the specified CSV file using the `pandas` and `chardet` libraries in Python.
2. **Transforming Data**: After extraction, the data undergoes cleaning and transformation processes. This may include removing duplicates, handling missing values, and formatting data according to database requirements. The `sqlalchemy` library is utilized for database interactions and transformations during this phase.
3. **Loading Data**: Once transformed, the data is loaded into a PostgreSQL database. This requires establishing a connection to the database using the `psycopg2` library and executing SQL queries to insert the data into appropriate tables.

## Conclusion

This ETL pipeline provides a basic framework for efficiently managing data from CSV files and storing it in a PostgreSQL database. It can be further customized and expanded to handle larger datasets, additional data sources, and more complex transformations as needed. Feel free to explore and adapt the code to suit your specific requirements.

For any questions or suggestions, please feel free to reach out. Happy data engineering!

Created by JaeBytes 2024