import pandas as pd
import psycopg2
import os

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

# Function to clean column names
def clean_columns(df):
    df.columns = [col.split(" ")[0] for col in df.columns]  # Remove trailing numbers
    df.columns = df.columns.str.replace(r"[^\w\s]", "", regex=True).str.strip()
    return df

# Load and clean car data
car_df = pd.read_csv(os.path.join("data", "car_data.csv"))
car_df = clean_columns(car_df)

# Load and clean consumer data
consumer_df = pd.read_csv(os.path.join("data", "consumer_data.csv"))
consumer_df = clean_columns(consumer_df)

# Insert data into PostgreSQL
def insert_data():
    conn = connect_db()
    cur = conn.cursor()

    # Insert car data
    for _, row in car_df.iterrows():
        cur.execute(
            "INSERT INTO cars (make, model, production_year, price, engine_type) VALUES (%s, %s, %s, %s, %s)",
            (row["Make"], row["Model"], row["Production"], row["Price"], row["Engine"]),
        )
    # Insert consumer data
    for _, row in consumer_df.iterrows():
        cur.execute(
            "INSERT INTO consumers (country, model, year, review_score, sales_volume) VALUES (%s, %s, %s, %s, %s)",
            (row["Country"], row["Model"], row["Year"], row["Review Score"], row["Sales Volume"]),
        )

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    insert_data()
    print("Data inserted successfully!")
