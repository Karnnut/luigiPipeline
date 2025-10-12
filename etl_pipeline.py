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

# Database connection (edit this for your setup)
DB_URL = "postgresql+psycopg2://username:password@localhost:5432/weather"


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
            raise RuntimeError(f"❌ Failed to open {self.input_path}: {e}")

        # Convert to DataFrame
        if ds.dims:
            df = ds.to_dataframe().reset_index()
        else:
            df = pd.DataFrame([{var: float(ds[var].values) for var in ds.data_vars}])

        # Convert datetime columns
        if 'valid_time' in df.columns:
            df['valid_time'] = pd.to_datetime(df['valid_time'])
            df['date'] = df['valid_time'].dt.date
            df['time'] = df['valid_time'].dt.time
        elif 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
            df['date'] = df['time'].dt.date
            df['time'] = df['time'].dt.time

        # Clean and format data
        df = df.fillna(0)
        df['latitude'] = df['latitude'].round(4)
        df['longitude'] = df['longitude'].round(4)

        # Ensure output directory exists
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

        # Save as CSV
        df.to_csv(self.output_path, index=False)
        print(f"✅ Saved Pressure CSV: {self.output_path}")



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

            # ✅ Skip if CSV already exists
            if os.path.exists(csv_file):
                print(f"⏩ Skipping {file}: CSV already exists ({csv_file})")
                continue

            print(f"📘 Processing {file}")
            idx_file = f"{file}.idx"

            # If .idx missing or cannot be created, handle it
            if not os.path.exists(idx_file):
                try:
                    open(idx_file, "w").close()
                    print(f"🆕 Created missing index file: {idx_file}")
                except Exception as e:
                    print(f"⚠️ Could not create index file {idx_file}: {e}")
                    continue  # skip this file but continue others

            try:
                ds = xr.open_dataset(file, engine="cfgrib", filter_by_keys={"typeOfLevel": "surface"})
            except OSError as e:
                print(f"❌ Disk/Index Error for {file}: {e}")
                continue
            except Exception as e:
                print(f"⚠️ Failed to open {file}: {e}")
                continue

            if ds.dims:
                df = ds.to_dataframe().reset_index()
            else:
                df = pd.DataFrame([{var: float(ds[var].values) for var in ds.data_vars}])

            if 'tp' not in df.columns:
                print(f"⚠️ Skipping {file}: missing 'tp' column")
                continue

            # Convert datetime
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

            try:
                df.to_csv(csv_file, index=False)
                print(f"✅ Saved Land CSV: {csv_file}")
            except OSError as e:
                print(f"❌ Disk Full while saving {csv_file}: {e}")
                continue
            except Exception as e:
                print(f"⚠️ Error saving {csv_file}: {e}")

            # Avoid overloading disk writes
            time.sleep(0.5)

        with self.output().open("w") as f:
            f.write("✅ Land TP CSV extraction done\n")


# =========================
# 3️⃣ Merge Pressure + Land TP
# =========================
class ExtractAndMergeLandTP(luigi.Task):
    input_dir = luigi.Parameter(default="data/raw")
    pressure_csv = luigi.Parameter(default=OUTPUT_CSV)

    def requires(self):
        return ExtractPressureLevels()  # ensures pressure CSV exists

    def output(self):
        return luigi.LocalTarget(os.path.join(OUTPUT_DIR, "done.txt"))

    def run(self):
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Load base pressure CSV
        pressure_df = pd.read_csv(self.pressure_csv)
        pressure_df['date'] = pd.to_datetime(pressure_df['date']).dt.date
        pressure_df['time'] = pressure_df['time'].astype(str)

        # Initialize TP column to 0
        pressure_df['tp'] = 0

        all_files = sorted(glob.glob(os.path.join(self.input_dir, "Land_*.grib")))
        if not all_files:
            raise ValueError(f"❌ No GRIB files found in {self.input_dir}")

        for file in all_files:
            print(f"📘 Processing {file}")
            try:
                ds = xr.open_dataset(file, engine="cfgrib", filter_by_keys={"typeOfLevel": "surface"})
            except Exception as e:
                print(f"⚠️ Failed to open {file}: {e}")
                continue

            df = ds.to_dataframe().reset_index()
            if 'tp' not in df.columns:
                print(f"⚠️ Skipping {file}: missing 'tp' column")
                continue

            # Convert datetime
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
            df['tp'] = df['tp'].fillna(0)

            # Merge tp values into pressure_df
            pressure_df = pressure_df.merge(
                df,
                on=['date', 'time', 'latitude', 'longitude'],
                how='left',
                suffixes=('', '_new')
            )

            # Replace 0 where no match, otherwise take the new tp
            pressure_df['tp'] = pressure_df['tp_new'].fillna(pressure_df['tp'])
            pressure_df.drop(columns=['tp_new'], inplace=True)

        # Save final merged CSV
        pressure_df.to_csv(self.pressure_csv, index=False)
        print(f"✅ All Land TP files merged into {self.pressure_csv}")

        with self.output().open("w") as f:
            f.write("✅ Merge complete\n")


# if __name__ == "__main__":
#     luigi.build([ExtractPressureLevels()], local_scheduler=True)

if __name__ == "__main__":
    # Run the final task; Luigi will run dependencies first
    luigi.build(
        [ExtractAndMergeLandTP()],  # final task
        local_scheduler=True
    )


