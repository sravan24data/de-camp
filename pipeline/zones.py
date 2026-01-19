from sqlalchemy import create_engine
import pandas as pd

# Load CSV
# url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
zones_df = pd.read_csv(url)

# Connect to Postgres
engine = create_engine("postgresql://root:root@localhost:5433/ny_taxi")


# Load into table
zones_df.to_sql(name="zones", con=engine, if_exists="replace", index=False)
print("Zones table created successfully!")
