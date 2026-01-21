import pandas as pd

# -----------------------------
# Load data
# -----------------------------
GREEN_FILE = "green_tripdata_2025-11.parquet"
ZONES_FILE = "taxi_zone_lookup.csv"

green = pd.read_parquet(GREEN_FILE)
zones = pd.read_csv(ZONES_FILE)

# Ensure datetime
green["lpep_pickup_datetime"] = pd.to_datetime(green["lpep_pickup_datetime"])

# Filter November 2025
nov = green[
    (green["lpep_pickup_datetime"] >= "2025-11-01") &
    (green["lpep_pickup_datetime"] < "2025-12-01")
].copy()

print(f"Total trips in Nov 2025: {len(nov)}")
print("-" * 50)

# -----------------------------
# Question 3
# Counting short trips (<= 1 mile)
# -----------------------------
q3 = (nov["trip_distance"] <= 1).sum()
print(f"Q3 - Trips with distance <= 1 mile: {q3}")

# -----------------------------
# Question 4
# Longest trip for each day (distance < 100)
# -----------------------------
nov_valid = nov[nov["trip_distance"] < 100].copy()
nov_valid["pickup_date"] = nov_valid["lpep_pickup_datetime"].dt.date

longest_by_day = nov_valid.groupby("pickup_date")["trip_distance"].max()
q4 = longest_by_day.idxmax()

print(f"Q4 - Pickup day with longest trip (<100 miles): {q4}")

# -----------------------------
# Question 5
# Biggest pickup zone by total_amount on Nov 18, 2025
# -----------------------------
nov18 = nov[nov["lpep_pickup_datetime"].dt.date == pd.to_datetime("2025-11-18").date()]

nov18_zones = nov18.merge(
    zones,
    left_on="PULocationID",
    right_on="LocationID",
    how="left"
)

q5 = (
    nov18_zones
    .groupby("Zone")["total_amount"]
    .sum()
    .idxmax()
)

print(f"Q5 - Biggest pickup zone on 2025-11-18 by total_amount: {q5}")

# -----------------------------
# Question 6
# Largest tip for pickups in East Harlem North
# -----------------------------
ehn_id = zones.loc[zones["Zone"] == "East Harlem North", "LocationID"].iloc[0]

ehn_trips = nov[nov["PULocationID"] == ehn_id]

ehn_drop = ehn_trips.merge(
    zones,
    left_on="DOLocationID",
    right_on="LocationID",
    how="left"
)

q6 = (
    ehn_drop
    .groupby("Zone")["tip_amount"]
    .sum()
    .idxmax()
)

print(f"Q6 - Dropoff zone with largest total tip (PU = East Harlem North): {q6}")

print("-" * 50)
print("Done.")
