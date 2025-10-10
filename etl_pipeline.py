import luigi
import pandas as pd
import xarray as xr
from sqlalchemy import create_engine, text
import os
import glob

DB_URI = "postgresql+psycopg2://luigi:luigi@db:5432/weather"


# -----------------------
# 1. Extract Pressure Level Data
# -----------------------
class ExtractPressureLevels(luigi.Task):
    input_path = luigi.Parameter(default="data/raw/pressure_levels_1_hour.grib")
    output_path = luigi.Parameter(default="data/processed/pressure_levels.parquet")

    def output(self):
        return luigi.LocalTarget(self.output_path)

    def run(self):
        print(f"Loading {self.input_path} ...")
        ds = xr.open_dataset(self.input_path, engine="cfgrib", filter_by_keys={"numberOfPoints": 2600})

        if ds.dims:
            df = ds.to_dataframe().reset_index()
        else:
            df = pd.DataFrame([{var: float(ds[var].values) for var in ds.data_vars}])

        # Convert datetime fields
        if 'valid_time' in df.columns:
            df['valid_time'] = pd.to_datetime(df['valid_time'])
            df['date'] = df['valid_time'].dt.date
            df['time'] = df['valid_time'].dt.time
        elif 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
            df['date'] = df['time'].dt.date
            df['time'] = df['time'].dt.time

        df = df[['date', 'time'] + [c for c in df.columns if c not in ['date', 'time']]]
        df = df.fillna(0)

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df.to_parquet(self.output_path, index=False)
        print(f"✅ Saved pressure_levels parquet to {self.output_path}")


# -----------------------
# 2. Extract Land TP Data (loop over all Land_{month}_{year}.grib)
# -----------------------
class ExtractAllLandTP(luigi.Task):
    input_dir = luigi.Parameter(default="data/raw")
    output_path = luigi.Parameter(default="data/processed/all_land_tp.parquet")

    def output(self):
        return luigi.LocalTarget(self.output_path)

    def run(self):
        all_files = sorted(glob.glob(os.path.join(self.input_dir, "Land_*.grib")))
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

        if not all_files:
            raise ValueError(f"No GRIB files found in {self.input_dir}")

        all_dataframes = []

        for file in all_files:
            print(f"📘 Processing file: {file}")
            try:
                ds = xr.open_dataset(file, engine="cfgrib", filter_by_keys={"typeOfLevel": "surface"})

                if ds.dims:
                    df = ds.to_dataframe().reset_index()
                else:
                    df = pd.DataFrame([{var: float(ds[var].values) for var in ds.data_vars}])

                if 'tp' not in df.columns:
                    print(f"⚠️ Skipping {file}: missing 'tp' column")
                    continue

                # Handle datetime
                if 'valid_time' in df.columns:
                    df['valid_time'] = pd.to_datetime(df['valid_time'])
                    df['date'] = df['valid_time'].dt.date
                    df['time'] = df['valid_time'].dt.time
                elif 'time' in df.columns:
                    df['time'] = pd.to_datetime(df['time'])
                    df['date'] = df['time'].dt.date
                    df['time'] = df['time'].dt.time

                keep_cols = ['date', 'time', 'latitude', 'longitude', 'tp']
                df = df[[c for c in keep_cols if c in df.columns]]

                df['latitude'] = df['latitude'].round(4)
                df['longitude'] = df['longitude'].round(4)
                df['tp'] = df['tp'].fillna(0)

                all_dataframes.append(df)

            except Exception as e:
                print(f"❌ Failed to process {file}: {e}")

        if not all_dataframes:
            raise ValueError("No valid TP data found in any Land_*.grib files!")

        final_df = pd.concat(all_dataframes, ignore_index=True)
        final_df.to_parquet(self.output_path, index=False)
        print(f"✅ Saved all Land TP data to {self.output_path}")


# -----------------------
# 3. Load to Database
# -----------------------
class LoadAllTPToDatabase(luigi.Task):
    def requires(self):
        return ExtractAllLandTP()

    def output(self):
        return luigi.LocalTarget("data/load_all_tp_done.txt")

    def run(self):
        engine = create_engine(DB_URI)
        tp_df = pd.read_parquet(self.input().path)

        # Ensure columns
        tp_df['latitude'] = tp_df['latitude'].round(4)
        tp_df['longitude'] = tp_df['longitude'].round(4)
        tp_df['time'] = tp_df['time'].astype(str)

        # Create table if not exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS weather_precision_data (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            time TIME NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            tp DOUBLE PRECISION DEFAULT 0
        );
        """
        with engine.begin() as conn:
            conn.execute(text(create_table_query))

        # Write to DB
        tp_df.to_sql(
            "weather_precision_data",
            con=engine,
            if_exists="append",  # append each batch instead of replace
            index=False,
            chunksize=5000
        )

        with self.output().open("w") as f:
            f.write("All Land TP data loaded successfully\n")
        print("✅ All Land TP data loaded to PostgreSQL (table: weather_precision_data)")


# -----------------------
# 4. Run Luigi Pipeline
# -----------------------
if __name__ == "__main__":
    luigi.build([LoadAllTPToDatabase()], local_scheduler=True)
