import luigi
import pandas as pd
import polars as pl
import xarray as xr
import os
import glob
import time
import sqlalchemy
from sqlalchemy import types

PRESSURE_FILE = "data/raw/pressure_levels_1_hour.grib"
OUTPUT_DIR = "data/processed"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "pressure_levels.csv")
OUTPUT_PARQUET = os.path.join(OUTPUT_DIR, "pressure_levels.parquet")

DB_URL = "postgresql+psycopg2://luigi:luigi@db:5432/weather_data"


# =========================
# Extract Pressure Levels
# =========================
class ExtractPressureLevels(luigi.Task):
    input_path = luigi.Parameter(default=PRESSURE_FILE)
    output_path = luigi.Parameter(default=OUTPUT_CSV)

    def output(self):
        return luigi.LocalTarget(self.output_path)

    def run(self):
        print(f"📘 Loading pressure dataset: {self.input_path}")

        try:
            ds = xr.open_dataset(self.input_path, engine="cfgrib", filter_by_keys={"numberOfPoints": 2600})
        except Exception as e:
            raise RuntimeError(f"Failed to open {self.input_path}: {e}")

        # Convert xarray dataset to pandas first, then to polars
        pandas_df = ds.to_dataframe().reset_index()
        df = pl.from_pandas(pandas_df)

        del ds
        del pandas_df

        # Drop step if exists
        if 'step' in df.columns:
            df = df.drop('step')

        # Extract date from valid_time
        if 'valid_time' in df.columns:
            df = df.with_columns([
                pl.col('valid_time').dt.date().alias('date'),
                pl.col('valid_time').dt.time().cast(pl.Utf8).alias('time')
            ]).drop('valid_time')
        elif 'time' in df.columns:
            df = df.with_columns([
                pl.col('time').dt.date().alias('date'),
                pl.col('time').dt.time().cast(pl.Utf8).alias('time')
            ])

        df = df.fill_null(0)

        if 'latitude' in df.columns and 'longitude' in df.columns:
            df = df.with_columns([
                pl.col('latitude').round(4),
                pl.col('longitude').round(4)
            ])

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df.write_csv(self.output_path)
        print(f"Saved CSV: {self.output_path}")
        print(f" Pressure file shape: {df.shape}")
        print(f" Pressure columns: {df.columns}")
        print(f" Sample dates: {df.select('date').unique().head(3).to_series().to_list()}")
        if 'time' in df.columns:
            print(f" Sample times: {df.select('time').unique().head(3).to_series().to_list()}")
        del df


# =========================
# Extract All Land TP Files
# =========================
class ExtractAllLandTP(luigi.Task):
    input_dir = luigi.Parameter(default="data/raw")
    output_dir = luigi.Parameter(default=os.path.join(OUTPUT_DIR, "land_csv"))

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, "done.txt"))

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)
        all_files = sorted(glob.glob(os.path.join(self.input_dir, "Land_*.grib")))

        if not all_files:
            raise ValueError(f"No GRIB files found in {self.input_dir}")

        for file in all_files:
            csv_file = os.path.join(self.output_dir, os.path.basename(file).replace(".grib", ".csv"))

            if os.path.exists(csv_file):
                print(f"Skipping {file}: CSV already exists")
                continue

            print(f"📘 Processing {file}")
            try:
                ds = xr.open_dataset(file, engine="cfgrib", filter_by_keys={"typeOfLevel": "surface"})
            except Exception as e:
                print(f"Failed to open {file}: {e}")
                continue

            pandas_df = ds.to_dataframe().reset_index()
            df = pl.from_pandas(pandas_df)
            del pandas_df
            
            if 'step' in df.columns:
                df = df.drop('step')

            if 'tp' not in df.columns:
                print(f"Skipping {file}: missing 'tp'")
                continue

            if 'valid_time' in df.columns:
                df = df.with_columns([
                    pl.col('valid_time').dt.date().alias('date'),
                    pl.col('valid_time').dt.time().cast(pl.Utf8).alias('time')
                ]).drop('valid_time')
            elif 'time' in df.columns:
                df = df.with_columns([
                    pl.col('time').dt.date().alias('date'),
                    pl.col('time').dt.time().cast(pl.Utf8).alias('time')
                ])

            df = df.select(['date', 'time', 'latitude', 'longitude', 'tp'])
            df = df.with_columns([
                pl.col('latitude').round(4),
                pl.col('longitude').round(4),
                pl.col('tp').fill_null(0)
            ])

            df.write_csv(csv_file)
            print(f"Saved Land CSV: {csv_file}")

            time.sleep(0.5)
        del df
        with self.output().open("w") as f:
            f.write("Land TP CSV extraction done\n")


# =========================
# Merge Pressure + Land TP
# =========================
class ExtractAndMergeLandTP(luigi.Task):
    input_dir = luigi.Parameter(default="data/raw")
    pressure_csv = luigi.Parameter(default=OUTPUT_CSV)
    parquet_path = luigi.Parameter(default=OUTPUT_PARQUET)

    def requires(self):
        return ExtractPressureLevels()

    def output(self):
        return {
            "csv": luigi.LocalTarget(self.pressure_csv),
            "parquet": luigi.LocalTarget(self.parquet_path),
            "done": luigi.LocalTarget(os.path.join(OUTPUT_DIR, "done.txt"))
        }

    def run(self):
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Load pressure data
        pressure_df = pl.read_csv(self.pressure_csv)
        if 'step' in pressure_df.columns:
            pressure_df = pressure_df.drop('step')

        # Ensure date is datetime
        pressure_df = pressure_df.with_columns(
            pl.col('date').str.to_date()
        )
        
        # Initialize tp column with null if it doesn't exist
        if 'tp' not in pressure_df.columns:
            pressure_df = pressure_df.with_columns(
                pl.lit(None).cast(pl.Float64).alias('tp')
            )
        
        print(f" Pressure DataFrame initial shape: {pressure_df.shape}")
        print(f" Pressure columns: {pressure_df.columns}")
        print(f" Pressure date type: {pressure_df.schema['date']}")
        print(f" Sample pressure dates: {pressure_df.select('date').unique().head(3).to_series().to_list()}")
        
        if 'time' in pressure_df.columns:
            print(f" Pressure time type: {pressure_df.schema['time']}")
            print(f" Sample pressure times: {pressure_df.select('time').unique().head(5).to_series().to_list()}")
        
        print(f" Pressure lat range: {pressure_df['latitude'].min()} to {pressure_df['latitude'].max()}")
        print(f" Pressure lon range: {pressure_df['longitude'].min()} to {pressure_df['longitude'].max()}")

        # Find all Land GRIB files
        all_files = sorted(glob.glob(os.path.join(self.input_dir, "Land_*.grib")))
        if not all_files:
            raise ValueError(f"No GRIB files found in {self.input_dir}")

        total_matches = 0
        
        # Process each Land file
        for file_idx, file in enumerate(all_files):
            print(f"\n📘 Merging {file} ({file_idx + 1}/{len(all_files)})")
            try:
                ds = xr.open_dataset(file, engine="cfgrib", filter_by_keys={"typeOfLevel": "surface"})
            except Exception as e:
                print(f"Failed to open {file}: {e}")
                continue

            pandas_df = ds.to_dataframe().reset_index()
            df = pl.from_pandas(pandas_df)
            
            if 'step' in df.columns:
                df = df.drop('step')
                
            if 'tp' not in df.columns:
                print(f"Skipping {file}: missing 'tp' column")
                continue

            # Process datetime columns
            if 'valid_time' in df.columns:
                df = df.with_columns([
                    pl.col('valid_time').dt.date().alias('date'),
                    pl.col('valid_time').dt.time().cast(pl.Utf8).alias('time')
                ]).drop('valid_time')
            elif 'time' in df.columns:
                df = df.with_columns([
                    pl.col('time').dt.date().alias('date'),
                    pl.col('time').dt.time().cast(pl.Utf8).alias('time')
                ])

            # Select and prepare columns
            df = df.select(['date', 'time', 'latitude', 'longitude', 'tp'])
            df = df.with_columns([
                pl.col('latitude').round(4),
                pl.col('longitude').round(4),
                pl.col('tp').fill_null(0)
            ])

            print(f" Land file shape: {df.shape}")
            print(f" Land date type: {df.schema['date']}")
            print(f" Sample land dates: {df.select('date').unique().head(3).to_series().to_list()}")
            print(f" Land time type: {df.schema['time']}")
            print(f" Sample land times: {df.select('time').unique().head(5).to_series().to_list()}")
            print(f" Land tp range: {df['tp'].min()} to {df['tp'].max()}")
            print(f" Non-zero tp count: {df.filter(pl.col('tp') > 0).height}")

            # Determine merge keys based on available columns
            merge_keys = ['date', 'latitude', 'longitude']
            if 'time' in pressure_df.columns and 'time' in df.columns:
                merge_keys.append('time')
                print(f" Merging on: {merge_keys}")
            else:
                print(f" Merging on: {merge_keys} (no time column)")

            # Perform merge
            before_merge_size = pressure_df.height
            pressure_df = pressure_df.join(
                df,
                on=merge_keys,
                how='left',
                suffix='_new'
            )
            after_merge_size = pressure_df.height
            
            print(f" Rows before merge: {before_merge_size}")
            print(f" Rows after merge: {after_merge_size}")

            # Update tp values where we have new data
            if 'tp_new' in pressure_df.columns:
                matches = pressure_df.filter(pl.col('tp_new').is_not_null()).height
                total_matches += matches
                print(f" Merge matches found: {matches}")
                
                if matches > 0:
                    tp_new_min = pressure_df.filter(pl.col('tp_new').is_not_null())['tp_new'].min()
                    tp_new_max = pressure_df.filter(pl.col('tp_new').is_not_null())['tp_new'].max()
                    print(f" tp_new range: {tp_new_min} to {tp_new_max}")
                    
                    # Update tp only where we have actual new data
                    pressure_df = pressure_df.with_columns(
                        pl.when(pl.col('tp_new').is_not_null())
                          .then(pl.col('tp_new'))
                          .otherwise(pl.col('tp'))
                          .alias('tp')
                    )
                    
                    print(f"Updated {matches} tp values")
                else:
                    print(f"No matches to update!")
                
                pressure_df = pressure_df.drop('tp_new')
            else:
                print(f"No tp_new column created - merge may have failed!")

        print(f"\nTotal merge matches across all files: {total_matches}")
        
        # Fill remaining NaN with 0 only at the END
        pressure_df = pressure_df.with_columns(
            pl.col('tp').fill_null(0)
        )
        
        print(f"\n Final Statistics:")
        print(f" Final shape: {pressure_df.shape}")
        print(f" Final tp range: {pressure_df['tp'].min()} to {pressure_df['tp'].max()}")
        print(f" Non-zero tp count: {pressure_df.filter(pl.col('tp') > 0).height}")
        print(f" Zero tp count: {pressure_df.filter(pl.col('tp') == 0).height}")

        # Save CSV and Parquet
        pressure_df.write_csv(self.pressure_csv)
        pressure_df.write_parquet(self.parquet_path)
        print(f"Saved merged CSV: {self.pressure_csv}")
        print(f"Saved merged Parquet: {self.parquet_path}")

        with self.output()["done"].open("w") as f:
            f.write("Merge complete\n")


# =========================
#  Upload to PostgreSQL
# =========================
class UploadToDatabase(luigi.Task):
    csv_path = luigi.Parameter(default=OUTPUT_CSV)
    parquet_path = luigi.Parameter(default=OUTPUT_PARQUET)
    db_url = luigi.Parameter(default=DB_URL)

    def requires(self):
        return ExtractAndMergeLandTP()

    def output(self):
        return luigi.LocalTarget(os.path.join(OUTPUT_DIR, "uploaded.txt"))

    def run(self):
        print(f"📦 Uploading data from {self.parquet_path} to PostgreSQL...")
    
        engine = sqlalchemy.create_engine(self.db_url)
    
        # Drop table once
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS weather_data"))
    
        # Read and upload in chunks
        chunk_size = 10000  # Adjust based on your RAM
        df = pl.read_parquet(self.parquet_path)
        total_rows = df.height
    
        print(f"Total rows: {total_rows}, uploading in chunks of {chunk_size}")
    
        for i in range(0, total_rows, chunk_size):
            chunk = df.slice(i, chunk_size)
            chunk.to_pandas().to_sql(
                "weather_data",
                engine,
                if_exists="append",
                index=False
            )
            print(f"✅ Uploaded {min(i + chunk_size, total_rows)}/{total_rows} rows")
    
        print("✅ All data uploaded")
        with self.output().open("w") as f:
            f.write("Upload complete\n")


if __name__ == "__main__":
    luigi.build([UploadToDatabase()], local_scheduler=True)
