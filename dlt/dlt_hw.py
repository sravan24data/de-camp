import duckdb

# Connect to the DuckDB database
con = duckdb.connect('taxi_pipeline.duckdb')

# 1️⃣ Start and end date
start_end = con.execute("""
SELECT 
    MIN(trip_pickup_date_time) AS start_date, 
    MAX(trip_dropoff_date_time) AS end_date
FROM taxi_data.taxi_trips
""").fetchone()

# 2️⃣ Credit card proportion (strings now)
credit_card = con.execute("""
SELECT
    100.0 * SUM(CASE WHEN payment_type = 'Credit' THEN 1 ELSE 0 END) / COUNT(*) AS credit_card_percentage
FROM taxi_data.taxi_trips
""").fetchone()

# 3️⃣ Total tips
total_tips = con.execute("""
SELECT SUM(tip_amt) AS total_tips
FROM taxi_data.taxi_trips
""").fetchone()

# ✅ Print all answers
print("✅ Homework Answers")
print("-------------------")
print(f"1. Start date: {start_end[0]}, End date: {start_end[1]}")
print(f"2. Credit card proportion: {credit_card[0]:.2f}%")
print(f"3. Total tips: ${total_tips[0]:.2f}") 