import pandas as pd
import polars as pl

'''
print('PANDAS')
df = pd.read_csv('data/processed/pressure_levels.csv')
df.head()
print(df.describe())
print(df['time'].value_counts())
print(df['isobaricInhPa'].value_counts())
print(df['w'].value_counts())
print(df['vo'].value_counts())
print(df['date'].value_counts())
print(df['tp'].value_counts())
'''
print('---\nPOLARS')
df = pl.read_parquet('data/processed/pressure_levels.parquet')
# Assuming 'df' is your Polars DataFrame with 24 columns.
# The 'with pl.Config(tbl_cols=-1)' block ensures all columns are printed.
# df.head() will display the first 5 rows by default.
with pl.Config(tbl_cols=-1):
    print(df.head())
'''
df = pl.read_parquet('data/processed/pressure_levels.parquet')

print(df.describe())
print(df['tp'].value_counts())
'''
