import csv
import psycopg2
import os
import pandas as pd  # Import the pandas library

# Database connection details
DB_NAME = "test_db"
DB_USER = "admin"
DB_PASSWORD = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"

# Connect to PostgreSQL
def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

# Insert consumer data from dictionary
def insert_consumer_data_from_dict(data):
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO consumers (country, model, type, year, review_score, sales_volume) VALUES (%s, %s, %s, %s, %s, %s)",
            (data["Country"], data["Model"], data["Type"], data["Year"], data["Review Score"], data["Sales Volume"]),
        )
        conn.commit()
    except psycopg2.Error as e:
        print(f"Database error during insertion: {e}")
        print("Problematic data:", data)
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# Process consumer data from CSV and insert
def process_consumer_data():
    csv_file_path = os.path.join("data", "consumer_data.csv")
    with open(csv_file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row

        for row in reader:
            if row:  # Ensure the row is not empty
                consumer_data = {
                    "Country": row[0],
                    "Model": row[1],
                    "Type": row[2],       # Data in the 'Year' column is Type
                    "Year": row[3],        # Data in the 'Review Score' column is Year
                    "Review Score": row[4], # Data in the 'Sales Volume' column is Review Score
                    "Sales Volume": row[5] if len(row) > 5 else None # Data in the missing header column is Sales Volume
                }
                insert_consumer_data_from_dict(consumer_data)
    print("Consumer data inserted successfully!")

# Function to clean columns (for car data)
def clean_car_columns(df):
    df.columns = [col.split(" ")[0] for col in df.columns]
    df.columns = df.columns.str.replace(r"[^\w\s]", "", regex=True).str.strip()
    return df

# Load and clean car data
car_df = pd.read_csv(os.path.join("data", "car_data.csv"))
car_df = clean_car_columns(car_df)

# Insert car data into PostgreSQL
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
    process_consumer_data()