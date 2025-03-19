import csv
import psycopg2
import pandas as pd  
import time  # Import time module for execution time measurement
from psycopg2.extras import execute_values  

# Database connection 
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

# Function to clean and remove duplicate car data
def clean_car_data():
    car_df = pd.read_csv("car_data.csv")
    car_df.columns = [col.split(" ")[0] for col in car_df.columns]  # Remove spaces
    car_df.columns = car_df.columns.str.replace(r"[^\w\s]", "", regex=True).str.strip()
    
    car_df = car_df.drop_duplicates(subset=["Make", "Model", "Production"])  # Remove duplicate cars
    return car_df

# Batch insert for cars using execute_values (fast)
def insert_car_data():
    car_df = clean_car_data()
    conn = connect_db()
    cur = conn.cursor()

    data_to_insert = [
        (row["Make"], row["Model"], row["Production"], row["Price"], row["Engine"])
        for _, row in car_df.iterrows()
    ]
    
    try:
        sql = """
        INSERT INTO cars (make, model, production_year, price, engine_type)
        VALUES %s
        ON CONFLICT (make, model, production_year) DO NOTHING
        """
        
        start_time = time.time()
        execute_values(cur, sql, data_to_insert)
        conn.commit()
        end_time = time.time()

        print(f"{len(data_to_insert)} car records processed successfully!")
        print(f"Car Data Execution Time: {end_time - start_time:.2f} seconds")
    
    except psycopg2.Error as e:
        print(f"Database error (cars): {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# Function to process consumer data from CSV, removing duplicates
def process_consumer_data_for_batch():
    csv_file_path = "consumer_data.csv"
    seen = set()
    data_to_insert = []

    with open(csv_file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header
        
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
                
                if consumer_data_tuple not in seen:
                    seen.add(consumer_data_tuple)
                    data_to_insert.append(consumer_data_tuple)

    return data_to_insert

# Batch insert for consumers using execute_values (fast)
def insert_consumer_data_batch(data_list):
    conn = connect_db()
    cur = conn.cursor()
    
    try:
        sql = """
        INSERT INTO consumers (country, model, type, year, review_score, sales_volume)
        VALUES %s
        ON CONFLICT (country, model, type, year) DO NOTHING
        """
        
        start_time = time.time()
        execute_values(cur, sql, data_list)
        conn.commit()
        end_time = time.time()

        print(f"{len(data_list)} consumer records processed successfully!")
        print(f"Consumer Data Execution Time: {end_time - start_time:.2f} seconds")
        
    except psycopg2.Error as e:
        print(f"Database error (consumers): {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    start_total_time = time.time()  

    insert_car_data()
    
    consumer_data_to_insert = process_consumer_data_for_batch()
    insert_consumer_data_batch(consumer_data_to_insert)

    end_total_time = time.time() # execution time
    print(f"Total Execution Time: {end_total_time - start_total_time:.2f} seconds") 
