import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

# Database connection 
DB_NAME = "test_db"
DB_USER = "admin"
DB_PASSWORD = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"

def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

def fetch_car_sales_data():
    """Fetches car sales data from the cars table."""
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT production_year, engine_type, COUNT(*) AS volume, SUM(price) AS value
            FROM cars
            GROUP BY production_year, engine_type
            ORDER BY production_year, engine_type;
        """)
        data = cur.fetchall()
        columns = ["production_year", "engine_type", "volume", "value"]
        df = pd.DataFrame(data, columns=columns)

        # Convert data types
        df["production_year"] = pd.to_numeric(df["production_year"], errors="coerce").astype("Int64")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
        df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)

        return df
    except psycopg2.Error as e:
        print(f"Error fetching data from the database: {e}")
        return pd.DataFrame()
    finally:
        cur.close()
        conn.close()

def generate_sales_graphs(df):
    """Generates and displays graphs of car sales data from the cars table."""
    if df.empty:
        print("No car sales data found in the cars table to generate graphs.")
        return

    # Group data by year and engine type
    grouped = df.groupby(["production_year", "engine_type"]).agg({"volume": "sum", "value": "sum"}).reset_index()

    # Pivot the table for easier plotting
    volume_pivot = grouped.pivot(index="production_year", columns="engine_type", values="volume").fillna(0)
    value_pivot = grouped.pivot(index="production_year", columns="engine_type", values="value").fillna(0)

    print("Volume Pivot Table:\n", volume_pivot.head())  
    print("Value Pivot Table:\n", value_pivot.head())  

    # Engine types
    all_engine_types = volume_pivot.columns.tolist()
    electric_engines = [etype for etype in all_engine_types if "electric" in etype.lower()]
    thermal_engines = [etype for etype in all_engine_types if etype not in electric_engines]

    volume_pivot["Electric"] = volume_pivot[electric_engines].sum(axis=1) if electric_engines else 0
    volume_pivot["Thermal"] = volume_pivot[thermal_engines].sum(axis=1) if thermal_engines else 0

    value_pivot["Electric"] = value_pivot[electric_engines].sum(axis=1) if electric_engines else 0
    value_pivot["Thermal"] = value_pivot[thermal_engines].sum(axis=1) if thermal_engines else 0

    # --- Volume Graph ---
    plt.figure(figsize=(10, 6))
    plt.plot(volume_pivot.index, volume_pivot["Electric"], label="Electric", marker="o", color="blue")
    plt.plot(volume_pivot.index, volume_pivot["Thermal"], label="Thermal", marker="o", color="red")
    plt.xlabel("Year")
    plt.ylabel("Number of Cars Sold")
    plt.title("Volume of Electric vs. Thermal Cars Sold Per Year")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # --- Value Graph ---
    plt.figure(figsize=(10, 6))
    plt.plot(value_pivot.index, value_pivot["Electric"], label="Electric", marker="o", color="blue")
    plt.plot(value_pivot.index, value_pivot["Thermal"], label="Thermal", marker="o", color="red")
    plt.xlabel("Year")
    plt.ylabel("Total Value of Cars Sold")
    plt.title("Value of Electric vs. Thermal Cars Sold Per Year")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    car_sales_data = fetch_car_sales_data()
    generate_sales_graphs(car_sales_data)