import luigi
import pandas as pd
import xarray as xr
import os
import glob
import time
import sqlalchemy

PRESSURE_FILE = "data/raw/pressure_levels_1_hour.grib"
OUTPUT_DIR = "data/processed"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "pressure_levels.csv")
OUTPUT_PARQUET = os.path.join(OUTPUT_DIR, "pressure_levels.parquet")

DB_URL = "postgresql+psycopg2://luigi:luigi@db:5432/weather"


# =========================
# 1️⃣ Extract Pressure Levels
# =========================
class ExtractPressureLevels(luigi.Task):
    input_path = luigi.Parameter(default=PRESSURE_FILE)
    output_path = luigi.Parameter(default=OUTPUT_CSV)

    def output(self):
        return luigi.LocalTarget(self.output_path)

    def run(self):
        print(f"📘 Loading pressure dataset: {self.input_path}")

        try:
            ds = xr.open_dataset(self.input_path, engine="cfgrib")
        except Exception as e:
            raise RuntimeError(f"❌ Failed to open {self.input_path}: {e}")

        df = ds.to_dataframe().reset_index()

        # Drop step if exists
        if 'step' in df.columns:
            df.drop(columns=['step'], inplace=True)

        # Extract date from valid_time
        if 'valid_time' in df.columns:
            df['date'] = pd.to_datetime(df['valid_time']).dt.date
            df.drop(columns=['valid_time'], inplace=True)
        elif 'time' in df.columns:
            df['date'] = pd.to_datetime(df['time']).dt.date
            df.drop(columns=['time'], inplace=True)

        df = df.fillna(0)

        if 'latitude' in df.columns and 'longitude' in df.columns:
            df['latitude'] = df['latitude'].round(4)
            df['longitude'] = df['longitude'].round(4)

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df.to_csv(self.output_path, index=False)
        print(f"✅ Saved CSV: {self.output_path}")


# =========================
# 2️⃣ Extract All Land TP Files
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
            raise ValueError(f"❌ No GRIB files found in {self.input_dir}")

        for file in all_files:
            csv_file = os.path.join(self.output_dir, os.path.basename(file).replace(".grib", ".csv"))

            if os.path.exists(csv_file):
                print(f"⏩ Skipping {file}: CSV already exists")
                continue

            print(f"📘 Processing {file}")
            try:
                ds = xr.open_dataset(file, engine="cfgrib", filter_by_keys={"typeOfLevel": "surface"})
            except Exception as e:
                print(f"⚠️ Failed to open {file}: {e}")
                continue

            df = ds.to_dataframe().reset_index()
            if 'step' in df.columns:
                df.drop(columns=['step'], inplace=True)

            if 'tp' not in df.columns:
                print(f"⚠️ Skipping {file}: missing 'tp'")
                continue

            if 'valid_time' in df.columns:
                df['valid_time'] = pd.to_datetime(df['valid_time'])
                df['date'] = df['valid_time'].dt.date
                df['time'] = df['valid_time'].dt.time
            elif 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                df['date'] = df['time'].dt.date
                df['time'] = df['time'].dt.time

            df = df[['date', 'time', 'latitude', 'longitude', 'tp']]
            df['latitude'] = df['latitude'].round(4)
            df['longitude'] = df['longitude'].round(4)
            df['tp'] = df['tp'].fillna(0)

            df.to_csv(csv_file, index=False)
            print(f"✅ Saved Land CSV: {csv_file}")

            time.sleep(0.5)

        with self.output().open("w") as f:
            f.write("✅ Land TP CSV extraction done\n")


# =========================
# 3️⃣ Merge Pressure + Land TP
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

        pressure_df = pd.read_csv(self.pressure_csv)
        if 'step' in pressure_df.columns:
            pressure_df.drop(columns=['step'], inplace=True)

        pressure_df['date'] = pd.to_datetime(pressure_df['date']).dt.date
        if 'time' not in pressure_df.columns:
            pressure_df['time'] = ""

        pressure_df['tp'] = 0

        all_files = sorted(glob.glob(os.path.join(self.input_dir, "Land_*.grib")))
        if not all_files:
            raise ValueError(f"❌ No GRIB files found in {self.input_dir}")

        for file in all_files:
            print(f"📘 Merging {file}")
            try:
                ds = xr.open_dataset(file, engine="cfgrib", filter_by_keys={"typeOfLevel": "surface"})
            except Exception as e:
                print(f"⚠️ Failed to open {file}: {e}")
                continue

            df = ds.to_dataframe().reset_index()
            if 'step' in df.columns:
                df.drop(columns=['step'], inplace=True)
            if 'tp' not in df.columns:
                continue

            if 'valid_time' in df.columns:
                df['valid_time'] = pd.to_datetime(df['valid_time'])
                df['date'] = df['valid_time'].dt.date
                df['time'] = df['valid_time'].dt.time.astype(str)
            elif 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                df['date'] = df['time'].dt.date
                df['time'] = df['time'].dt.time.astype(str)

            df = df[['date', 'time', 'latitude', 'longitude', 'tp']]
            df['latitude'] = df['latitude'].round(4)
            df['longitude'] = df['longitude'].round(4)

            pressure_df = pressure_df.merge(
                df,
                on=['date', 'time', 'latitude', 'longitude'],
                how='left',
                suffixes=('', '_new')
            )

            pressure_df['tp'] = pressure_df['tp_new'].fillna(pressure_df['tp'])
            pressure_df.drop(columns=['tp_new'], inplace=True)

        # Save CSV and Parquet
        pressure_df.to_csv(self.pressure_csv, index=False)
        pressure_df.to_parquet(self.parquet_path, index=False)
        print(f"✅ Saved merged CSV: {self.pressure_csv}")
        print(f"✅ Saved merged Parquet: {self.parquet_path}")

        with self.output()["done"].open("w") as f:
            f.write("✅ Merge complete\n")


# =========================
# 4️⃣ Upload to PostgreSQL
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
        print(f"📦 Uploading data from {self.csv_path} to PostgreSQL...")

        df = pd.read_csv(self.csv_path)
        if 'step' in df.columns:
            df.drop(columns=['step'], inplace=True)

        engine = sqlalchemy.create_engine(self.db_url)
        df.to_sql("weather_data", engine, if_exists="append", index=False)
        print("✅ Data successfully uploaded to weather_data")

        with self.output().open("w") as f:
            f.write("✅ Upload complete\n")


if __name__ == "__main__":
    luigi.build([UploadToDatabase()], local_scheduler=True)
