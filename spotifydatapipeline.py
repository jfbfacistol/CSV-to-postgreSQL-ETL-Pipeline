import os
import pandas as pd
import chardet as ch
import sqlalchemy as sa
import psycopg2
import credentials

# Data Extraction
# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Specify the relative path to your CSV file
relative_path = "Data\\spotify-2023.csv"

# Construct the full path to the CSV file
file_path = os.path.join(script_dir, relative_path)

# Detect the encoding of the CSV file
with open(file_path, 'rb') as f:
    raw_data = f.read()
    encoding_result = ch.detect(raw_data)
    file_encoding = encoding_result['encoding']

# Read the CSV file using the detected encoding
def extract_data(file_path, file_encoding):
    data = pd.read_csv(file_path, encoding=file_encoding)
    print("Data extracted successfully.")
    return data

def transform_data(data):
    ## Perform data cleaning here and manipulation, such as removing invalid characters

    df = pd.DataFrame(data)
    # Remove invalid characters from the 'track_name' and 'artist_names' columns
    df[['track_name', 'artist_names']] = df[['track_name', 'artist_names']].apply(lambda x: x.str.replace('[^\x00-\x7F]+', '', regex=True))

    # Remove leading and trailing whitespaces from the 'track_name' and 'artist(s)_name' columns
    df[['track_name', 'artist_names']] = df[['track_name', 'artist_names']].apply(lambda x: x.str.strip())
    
    # Fill missing values using forward fill
    df.ffill(inplace=True)

    # Fill missing values using backward fill
    df.bfill(inplace=True)
    
    # Remove duplicate rows
    df.drop_duplicates(inplace=True)
    
    # Convert 'streams' column to numeric, coerce non-numeric values to NaN
    df['streams'] = pd.to_numeric(df['streams'], errors='coerce')

    # Remove rows where 'streams' column is NaN
    df = df.dropna(subset=['streams'])

    # Convert 'streams' column to integer and make integers always positive
    df['streams'] = df['streams'].astype(int).abs()
    
    # Save the cleaned DataFrame back to a CSV file 
    output_file_path = os.path.join(script_dir, "Data\\cleaned_spotify_data.csv")
    df.to_csv(output_file_path, index=False)
    
    print("Data transformed successfully.")
    return df, output_file_path
    
def load(df, connection, spotify):
    conn = None 
    cur = None
    try:
        rows_imported = 0
        engine = sa.create_engine(f'postgresql://{credentials.username}:{credentials.pwd}@{credentials.hostname}:{credentials.port_id}/{credentials.database}')
        df.to_sql(spotify, engine, if_exists='append', index=False)
        rows_imported += len(df)
        print(f"{rows_imported} rows have been imported into the database.")
    except Exception as error: 
        print("File Upload Data load error: ", f"Data extract error: File location: {error}")
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

# Main code
if __name__ == "__main__":
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Specify the relative path to your CSV file
    relative_path = "Data\\spotify-2023.csv"

    # Construct the full path to the CSV file
    file_path = os.path.join(script_dir, relative_path)

    # Detect the encoding of the CSV file
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        encoding_result = ch.detect(raw_data)
        file_encoding = encoding_result['encoding']
        
    # Read the CSV file using the detected encoding
    data = extract_data(file_path, file_encoding)
    transformed_data, output_file_path = transform_data(data)

    conn = None 
    cur = None

    try:
        # Define connection details and file paths
        conn = psycopg2.connect(
                host=credentials.hostname,
                dbname=credentials.database,
                user=credentials.username,
                password=credentials.pwd,
                port=credentials.port_id
                )

        cur = conn.cursor()  # Create a cursor object

        load(transformed_data, conn, "spotify")

    except Exception as error: 
        print("Error while connecting to PostgreSQL:", error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
