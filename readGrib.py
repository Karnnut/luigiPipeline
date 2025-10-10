import xarray as xr
import pandas as pd

# 1️⃣ Open only the GRIB subset with numberOfPoints = 2600
ds = xr.open_dataset(
    "data/raw/pressure_levels_1_hour.grib",
    engine="cfgrib",
    filter_by_keys={'numberOfPoints': 2600}
)

# 2️⃣ Convert dataset to pandas DataFrame
df = ds.to_dataframe().reset_index()

# 3️⃣ Split datetime column
if 'time' in df.columns:
    df['time'] = pd.to_datetime(df['time'])
    df['date'] = df['time'].dt.date
    df['time'] = df['time'].dt.time  # 👈 keep the column name 'time'
    # Move date and time columns to the front
    df = df[['date', 'time'] + [c for c in df.columns if c not in ['date', 'time']]]

# 4️⃣ Save to CSV
df.to_csv("Land_September_2024.csv", index=False)

print("✅ Done! Saved filtered GRIB (numberOfPoints=2600) to data_2600.csv")
