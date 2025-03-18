import csv
import psycopg2
import os
import pandas as pd  # Import the pandas library

# Database connection details (assuming these are defined globally as in your provided code)
DB_NAME = "test_db"
DB_USER = "admin"
DB_PASSWORD = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"

# Connect to PostgreSQL (assuming this function is defined globally)
def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

# Process consumer data from CSV and prepare data for batch insert
def process_consumer_data_for_batch():
    csv_file_path = os.path.join("data", "consumer_data.csv")
    data_to_insert = []
    with open(csv_file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row

        for row in reader:
            if row:
                consumer_data_tuple = (
                    row[0],  # Country
                    row[1],  # Model
                    row[2],  # Type
                    row[3],  # Year
                    row[4],  # Review Score
                    row[5] if len(row) > 5 else None  # Sales Volume
                )
                data_to_insert.append(consumer_data_tuple)
    return data_to_insert

# Insert consumer data using execute_batch
def insert_consumer_data_batch(data_list):
    conn = connect_db()
    cur = conn.cursor()
    try:
        sql = "INSERT INTO consumers (country, model, type, year, review_score, sales_volume) VALUES (%s, %s, %s, %s, %s, %s)"
        cur.executemany(sql, data_list)
        conn.commit()
        print(f"{len(data_list)} consumer records inserted successfully!")
    except psycopg2.Error as e:
        print(f"Database error during batch insertion: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# Function to clean columns (for car data) - keeping it as is
def clean_car_columns(df):
    df.columns = [col.split(" ")[0] for col in df.columns]
    df.columns = df.columns.str.replace(r"[^\w\s]", "", regex=True).str.strip()
    return df

# Load and clean car data - keeping it as is
car_df = pd.read_csv(os.path.join("data", "car_data.csv"))
car_df = clean_car_columns(car_df)

# Insert car data into PostgreSQL - keeping it as is
def insert_car_data():
    conn = connect_db()
    cur = conn.cursor()
    for _, row in car_df.iterrows():
        cur.execute(
            "INSERT INTO cars (make, model, production_year, price, engine_type) VALUES (%s, %s, %s, %s, %s)",
            (row["Make"], row["Model"], row["Production"], row["Price"], row["Engine"]),
        )
    conn.commit()
    cur.close()
    conn.close()
    print("Car data inserted successfully!")

if __name__ == "__main__":
    insert_car_data()
    consumer_data_to_insert = process_consumer_data_for_batch()
    insert_consumer_data_batch(consumer_data_to_insert)