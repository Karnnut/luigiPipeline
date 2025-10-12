# import xarray as xr
# import pandas as pd

# # 1️⃣ Open the correct dataset (no filter_by_keys)
# ds = xr.open_dataset(
#     "data/raw/Land_October_2024.grib",
#     engine="cfgrib"
# )

# # 2️⃣ Convert to DataFrame
# df = ds.to_dataframe().reset_index()

# # 3️⃣ Use 'valid_time' if available (it's the real forecast datetime)
# if 'valid_time' in df.columns:
#     df['valid_time'] = pd.to_datetime(df['valid_time'])
#     df['date'] = df['valid_time'].dt.date
#     df['time'] = df['valid_time'].dt.time
# elif 'time' in df.columns:
#     df['time'] = pd.to_datetime(df['time'])
#     df['date'] = df['time'].dt.date
#     df['time'] = df['time'].dt.time

# # 4️⃣ Reorder columns (date/time first)
# df = df[['date', 'time'] + [c for c in df.columns if c not in ['date', 'time']]]

# # 5️⃣ Save to CSV
# df.to_csv("precipitation.csv", index=False)

# print("✅ Done! Saved to data/precipitation.csv")

import pandas as pd
df = pd.read_csv("data/processed/pressure_levels.csv")
print(df.columns)
