import luigi
import pandas as pd
import xarray as xr
from sqlalchemy import create_engine
import os
import glob
import re
import gc
from datetime import datetime
import warnings

# Optimize pandas for memory usage
pd.set_option('mode.chained_assignment', None)
warnings.filterwarnings('ignore', category=UserWarning)

DB_URI = "postgresql+psycopg2://luigi:luigi@db:5432/weather"


class CheckDataFiles(luigi.Task):
    """Debug task to check what files are available"""
    data_path = luigi.Parameter(default="data/raw")
    
    def output(self):
        return luigi.LocalTarget("data/file_check.txt")
    
    def run(self):
        print(f"Checking files in {self.data_path}...")
        
        if not os.path.exists(self.data_path):
            print(f"ERROR: Directory {self.data_path} does not exist!")
            return
        
        all_files = os.listdir(self.data_path)
        grib_files = [f for f in all_files if f.endswith('.grib')]
        land_files = [f for f in all_files if f.startswith('Land_') and f.endswith('.grib')]
        pressure_files = [f for f in all_files if 'pressure' in f.lower()]
        
        print(f"All files in {self.data_path}: {all_files}")
        print(f"GRIB files: {grib_files}")
        print(f"Land files: {land_files}")
        print(f"Pressure files: {pressure_files}")
        
        with self.output().open('w') as f:
            f.write(f"File check completed at {datetime.now()}\n")
            f.write(f"Directory: {self.data_path}\n")
            f.write(f"All files: {all_files}\n")
            f.write(f"GRIB files: {grib_files}\n")
            f.write(f"Land files: {land_files}\n")
            f.write(f"Pressure files: {pressure_files}\n")


class ExtractPressureLevels(luigi.Task):
    input_path = luigi.Parameter(default="data/raw/pressure_levels_1_hour.grib")
    output_path = luigi.Parameter(default="data/processed/pressure_levels.parquet")

    def output(self):
        return luigi.LocalTarget(self.output_path)

    def run(self):
        print(f"Loading {self.input_path} ...")
        
        # Check if input file exists
        if not os.path.exists(self.input_path):
            print(f"ERROR: Input file {self.input_path} does not exist!")
            print(f"Current directory contents: {os.listdir('data/raw') if os.path.exists('data/raw') else 'data/raw does not exist'}")
            raise FileNotFoundError(f"Input file {self.input_path} not found")
        
        # Load with minimal memory footprint
        try:
            with xr.open_dataset(self.input_path, engine="cfgrib", 
                               filter_by_keys={"numberOfPoints": 2600}) as ds:
                
                if ds.dims:
                    # Process in smaller chunks if dataset is large
                    df = ds.to_dataframe().reset_index()
                else:
                    df = pd.DataFrame([{var: float(ds[var].values) for var in ds.data_vars}])
        except Exception as e:
            print(f"ERROR: Failed to load {self.input_path}: {e}")
            raise

        # Standardize datetime handling
        df = self._standardize_datetime(df)
        
        # Standardize coordinates
        df = self._standardize_coordinates(df)
        
        # Reorder columns
        base_cols = ['date', 'time', 'latitude', 'longitude']
        other_cols = [c for c in df.columns if c not in base_cols]
        df = df[base_cols + other_cols]
        df = df.fillna(0)

        # Create output directory
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        
        # Save with compression
        df.to_parquet(self.output_path, index=False, compression='snappy')
        print(f"Saved pressure_levels parquet to {self.output_path}")
        print(f"Records: {len(df)}, Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

    def _standardize_datetime(self, df):
        """Standardize datetime columns to date and time"""
        if 'valid_time' in df.columns:
            df['valid_time'] = pd.to_datetime(df['valid_time'])
            df['date'] = df['valid_time'].dt.date
            df['time'] = df['valid_time'].dt.time
            df = df.drop('valid_time', axis=1)
        elif 'time' in df.columns and not pd.api.types.is_object_dtype(df['time']):
            df['time'] = pd.to_datetime(df['time'])
            df['date'] = df['time'].dt.date
            df['time'] = df['time'].dt.time
        
        return df

    def _standardize_coordinates(self, df):
        """Standardize latitude and longitude to 4 decimal places"""
        if 'latitude' in df.columns:
            df['latitude'] = df['latitude'].round(4).astype('float32')  # Use float32 to save memory
        if 'longitude' in df.columns:
            df['longitude'] = df['longitude'].round(4).astype('float32')
        return df


class ProcessSingleLandFile(luigi.Task):
    file_path = luigi.Parameter()
    output_dir = luigi.Parameter(default="data/processed/temp_tp")

    def output(self):
        filename = os.path.basename(self.file_path).replace('.grib', '.parquet')
        return luigi.LocalTarget(os.path.join(self.output_dir, filename))

    def run(self):
        import traceback
        
        print(f"=== Processing single file: {self.file_path} ===")
        
        try:
            # Extract month and year from filename for validation
            filename = os.path.basename(self.file_path)
            # Updated regex to match Land_{month_name}_{year}.grib pattern
            match = re.match(r'Land_([A-Za-z]+)_(\d+)\.grib', filename)
            if match:
                month_name, year = match.groups()
                print(f"  -> Processing data for {month_name}/{year}")
            else:
                print(f"  -> Warning: Filename {filename} doesn't match expected pattern")

            # Check if file exists
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"File {self.file_path} does not exist")

            print(f"  -> File size: {os.path.getsize(self.file_path) / (1024*1024):.1f} MB")

            # Load the GRIB file with memory optimization
            print(f"  -> Opening GRIB file...")
            with xr.open_dataset(self.file_path, engine="cfgrib", 
                               filter_by_keys={"typeOfLevel": "surface"}) as ds:
                
                print(f"  -> Successfully opened GRIB file")
                print(f"  -> Dimensions: {dict(ds.dims)}")
                print(f"  -> Variables: {list(ds.data_vars)}")
                
                if ds.dims:
                    df = ds.to_dataframe().reset_index()
                    print(f"  -> Converted to DataFrame: {df.shape}")
                else:
                    df = pd.DataFrame([{var: float(ds[var].values) for var in ds.data_vars}])
                    print(f"  -> Created single-row DataFrame: {df.shape}")

            # Check if tp column exists
            if 'tp' not in df.columns:
                print(f"  -> Skipping {self.file_path}: missing 'tp' column")
                print(f"  -> Available columns: {list(df.columns)}")
                # Create empty file to mark as processed
                os.makedirs(self.output_dir, exist_ok=True)
                pd.DataFrame().to_parquet(self.output().path, index=False)
                return

            print(f"  -> Found 'tp' column with {len(df)} records")

            # Optimize data types for memory
            for col in df.columns:
                if df[col].dtype == 'float64':
                    df[col] = df[col].astype('float32')

            # Standardize datetime
            print(f"  -> Standardizing datetime...")
            df = self._standardize_datetime(df)
            
            # Standardize coordinates  
            print(f"  -> Standardizing coordinates...")
            df = self._standardize_coordinates(df)

            # Keep only relevant columns for TP data
            keep_cols = ['date', 'time', 'latitude', 'longitude', 'tp']
            available_cols = [c for c in keep_cols if c in df.columns]
            print(f"  -> Keeping columns: {available_cols}")
            df = df[available_cols]

            # Fill missing TP values with 0
            df['tp'] = df['tp'].fillna(0).astype('float32')
            print(f"  -> Filled missing TP values")

            # Create output directory
            os.makedirs(self.output_dir, exist_ok=True)
            
            # Save with compression
            df.to_parquet(self.output().path, index=False, compression='snappy')
            print(f"  -> Saved {len(df)} records, {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
            print(f"  -> Output saved to: {self.output().path}")
            
        except Exception as e:
            print(f"  -> Failed to process {self.file_path}: {e}")
            print(f"  -> Error type: {type(e).__name__}")
            print("  -> Full traceback:")
            traceback.print_exc()
            # Create empty file to mark as processed
            os.makedirs(self.output_dir, exist_ok=True)
            pd.DataFrame().to_parquet(self.output().path, index=False)
        
        finally:
            # Force garbage collection
            gc.collect()
            print(f"=== Completed processing: {os.path.basename(self.file_path)} ===")

    def _standardize_datetime(self, df):
        """Standardize datetime columns to date and time"""
        if 'valid_time' in df.columns:
            df['valid_time'] = pd.to_datetime(df['valid_time'])
            df['date'] = df['valid_time'].dt.date
            df['time'] = df['valid_time'].dt.time
            df = df.drop('valid_time', axis=1)
        elif 'time' in df.columns and not pd.api.types.is_object_dtype(df['time']):
            df['time'] = pd.to_datetime(df['time'])
            df['date'] = df['time'].dt.date
            df['time'] = df['time'].dt.time
        
        return df

    def _standardize_coordinates(self, df):
        """Standardize latitude and longitude to 4 decimal places"""
        if 'latitude' in df.columns:
            df['latitude'] = df['latitude'].round(4).astype('float32')
        if 'longitude' in df.columns:
            df['longitude'] = df['longitude'].round(4).astype('float32')
        return df


class ExtractLandTP(luigi.Task):
    input_path = luigi.Parameter(default="data/raw")
    output_path = luigi.Parameter(default="data/processed/land_tp.parquet")
    temp_dir = luigi.Parameter(default="data/processed/temp_tp")

    def requires(self):
        # Find all Land_{month_name}_{year}.grib files
        pattern = os.path.join(self.input_path, "Land_*_*.grib")
        all_files = glob.glob(pattern)
        all_files.sort()
        
        print(f"Looking for files with pattern: {pattern}")
        print(f"Found {len(all_files)} Land_*.grib files to process")
        
        if all_files:
            print(f"Files found: {[os.path.basename(f) for f in all_files]}")
        else:
            print(f"No Land_*.grib files found in {self.input_path}")
            print(f"Directory contents: {os.listdir(self.input_path) if os.path.exists(self.input_path) else 'Directory does not exist'}")
        
        # Create individual tasks for each file (return empty list if no files)
        return [ProcessSingleLandFile(file_path=file_path, output_dir=self.temp_dir) 
                for file_path in all_files]

    def output(self):
        return luigi.LocalTarget(self.output_path)

    def run(self):
        import traceback
        
        print("=== Starting ExtractLandTP.run() ===")
        
        try:
            print("Combining all processed TP files...")
            
            # Check if we have any requirements (files to process)
            requirements = self.requires()
            print(f"Number of requirements (files to process): {len(requirements)}")
            
            if not requirements:
                print("No Land_*.grib files found. Creating empty TP dataset...")
                # Create an empty parquet file with the expected structure
                empty_df = pd.DataFrame(columns=['date', 'time', 'latitude', 'longitude', 'tp'])
                os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
                empty_df.to_parquet(self.output_path, index=False, compression='snappy')
                print(f"Created empty TP parquet at {self.output_path}")
                return
            
            print(f"Processing {len(requirements)} files...")
            
            # Get all processed files
            processed_files = [task.output().path for task in requirements]
            print(f"Expected processed files: {[os.path.basename(p) for p in processed_files[:3]]}...")
            
            # Check which files actually exist
            existing_files = [p for p in processed_files if os.path.exists(p)]
            print(f"Actually existing files: {len(existing_files)}/{len(processed_files)}")
            
            # Process files in small batches to avoid memory issues
            batch_size = 3  # Process 3 files at a time
            final_chunks = []
            
            print(f"Processing {len(existing_files)} files in batches of {batch_size}...")
            
            for i in range(0, len(existing_files), batch_size):
                batch_files = existing_files[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(existing_files) + batch_size - 1) // batch_size
                
                print(f"Processing batch {batch_num}/{total_batches} ({len(batch_files)} files)...")
                
                # Load and combine current batch
                batch_dfs = []
                for file_path in batch_files:
                    try:
                        df = pd.read_parquet(file_path)
                        if not df.empty:
                            batch_dfs.append(df)
                            print(f"  -> Loaded {len(df)} records from {os.path.basename(file_path)}")
                        else:
                            print(f"  -> Skipped empty file: {os.path.basename(file_path)}")
                    except Exception as e:
                        print(f"  -> Error reading {file_path}: {e}")
                
                if batch_dfs:
                    # Combine current batch
                    batch_combined = pd.concat(batch_dfs, ignore_index=True)
                    
                    # Remove duplicates within batch
                    batch_combined = batch_combined.drop_duplicates(
                        subset=['date', 'time', 'latitude', 'longitude'], keep='first'
                    )
                    
                    # Save batch to temporary file
                    temp_batch_file = f"data/processed/batch_{batch_num}.parquet"
                    os.makedirs(os.path.dirname(temp_batch_file), exist_ok=True)
                    batch_combined.to_parquet(temp_batch_file, index=False, compression='snappy')
                    final_chunks.append(temp_batch_file)
                    
                    print(f"  -> Saved batch {batch_num}: {len(batch_combined)} records")
                    
                    # Free memory
                    del batch_dfs, batch_combined
                    gc.collect()

            if not final_chunks:
                print("No valid TP data found. Creating empty TP dataset...")
                empty_df = pd.DataFrame(columns=['date', 'time', 'latitude', 'longitude', 'tp'])
                os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
                empty_df.to_parquet(self.output_path, index=False, compression='snappy')
                print(f"Created empty TP parquet at {self.output_path}")
                return

            # Combine all batch files
            print(f"Combining {len(final_chunks)} batch files...")
            final_dfs = []
            for batch_file in final_chunks:
                df = pd.read_parquet(batch_file)
                final_dfs.append(df)
                print(f"  -> Loaded batch: {len(df)} records")
                
                # Clean up batch file
                os.remove(batch_file)
            
            final_df = pd.concat(final_dfs, ignore_index=True)
            del final_dfs
            gc.collect()
            
            # Remove duplicates
            print("Removing duplicates...")
            initial_count = len(final_df)
            final_df = final_df.drop_duplicates(subset=['date', 'time', 'latitude', 'longitude'], keep='first')
            print(f"Removed {initial_count - len(final_df)} duplicate records")
            
            # Sort for consistency
            print("Sorting data...")
            final_df = final_df.sort_values(['date', 'time', 'latitude', 'longitude']).reset_index(drop=True)
            
            # Create output directory
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            
            # Save final result
            final_df.to_parquet(self.output_path, index=False, compression='snappy')
            print(f"Saved combined TP parquet to {self.output_path}")
            print(f"Total records: {len(final_df)}")
            
            # Clean up temporary files
            print("Cleaning up temporary files...")
            for file_path in processed_files:
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except:
                    pass
            try:
                if os.path.exists(self.temp_dir):
                    os.rmdir(self.temp_dir)
            except:
                pass
            
            print("=== ExtractLandTP.run() completed successfully ===")
            
        except Exception as e:
            print(f"=== ERROR in ExtractLandTP.run(): {e} ===")
            print("Full traceback:")
            traceback.print_exc()
            raise


class LoadToDatabase(luigi.Task):
    table_name = luigi.Parameter(default="weather_data")

    def requires(self):
        return [ExtractPressureLevels(), ExtractLandTP()]

    def output(self):
        return luigi.LocalTarget("data/load_done.txt")

    def run(self):
        print("Loading data to PostgreSQL database...")
        engine = create_engine(DB_URI, pool_pre_ping=True, pool_recycle=3600)

        try:
            # Load parquet files with memory optimization
            print("Loading pressure levels data...")
            pressure_df = pd.read_parquet(self.input()[0].path)
            print(f"Loaded {len(pressure_df)} pressure level records")

            print("Loading TP data...")
            tp_df = pd.read_parquet(self.input()[1].path)
            print(f"Loaded {len(tp_df)} TP records")
            
            # Ensure coordinates are standardized for pressure data
            pressure_df['latitude'] = pressure_df['latitude'].round(4).astype('float32')
            pressure_df['longitude'] = pressure_df['longitude'].round(4).astype('float32')
            pressure_df['time'] = pressure_df['time'].astype(str)
            
            # Handle case where TP data might be empty
            if len(tp_df) == 0:
                print("WARNING: TP dataset is empty. Proceeding without TP data.")
                # Create dummy TP merge dataframe
                tp_merge_df = pd.DataFrame(columns=['date', 'time', 'latitude', 'longitude', 'tp'])
            else:
                # Ensure coordinates are standardized
                tp_df['latitude'] = tp_df['latitude'].round(4).astype('float32')
                tp_df['longitude'] = tp_df['longitude'].round(4).astype('float32')
                # Convert time to string for consistent merging
                tp_df['time'] = tp_df['time'].astype(str)
                # Optimize TP dataframe before merge
                tp_merge_df = tp_df[['date', 'time', 'latitude', 'longitude', 'tp']].copy()
                del tp_df
                gc.collect()

            # Perform merge in chunks if dataset is large
            chunk_size = 50000  # Process in smaller chunks
            
            if len(tp_merge_df) == 0:
                # No TP data to merge, just add empty TP column
                print("No TP data available - adding empty TP column...")
                merged_df = pressure_df.copy()
                merged_df['tp'] = 0.0
            elif len(pressure_df) > chunk_size:
                print(f"Processing merge in chunks of {chunk_size} records...")
                merged_chunks = []
                
                for i in range(0, len(pressure_df), chunk_size):
                    chunk = pressure_df.iloc[i:i + chunk_size]
                    merged_chunk = pd.merge(chunk, tp_merge_df, 
                                          on=['date', 'time', 'latitude', 'longitude'], 
                                          how='left')
                    merged_chunks.append(merged_chunk)
                    print(f"  -> Processed chunk {len(merged_chunks)}")
                
                merged_df = pd.concat(merged_chunks, ignore_index=True)
                del merged_chunks
            else:
                merged_df = pd.merge(pressure_df, tp_merge_df,
                                   on=['date', 'time', 'latitude', 'longitude'],
                                   how='left')

            # Free memory
            del pressure_df, tp_merge_df
            gc.collect()

            # Fill missing TP values
            merged_df['tp'] = merged_df['tp'].fillna(0).astype('float32')

            print(f"Merged dataset contains {len(merged_df)} records")
            print(f"Records with TP data: {(merged_df['tp'] > 0).sum()}")

            # Write to database in very small chunks
            chunk_size = 1000
            total_chunks = (len(merged_df) + chunk_size - 1) // chunk_size
            
            print(f"Writing to PostgreSQL table: {self.table_name} in {total_chunks} chunks")
            
            for i in range(0, len(merged_df), chunk_size):
                chunk = merged_df.iloc[i:i + chunk_size]
                chunk_num = (i // chunk_size) + 1
                
                print(f"Writing chunk {chunk_num}/{total_chunks} ({len(chunk)} records)...")
                
                chunk.to_sql(
                    self.table_name,
                    con=engine,
                    if_exists="replace" if i == 0 else "append",
                    index=False,
                    method='multi'
                )
                
                # Force cleanup every 10 chunks
                if chunk_num % 10 == 0:
                    gc.collect()

            # Create indexes
            with engine.connect() as conn:
                print("Creating database indexes...")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_date_time ON {self.table_name} (date, time);")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_coords ON {self.table_name} (latitude, longitude);")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_spatial ON {self.table_name} (date, time, latitude, longitude);")
                conn.commit()

            # Write completion marker
            with self.output().open("w") as f:
                f.write(f"Data loaded successfully at {datetime.now()}\n")
                f.write(f"Records processed: {len(merged_df)}\n")
                f.write(f"Table: {self.table_name}\n")
            
            print("Data loaded to PostgreSQL successfully!")
            
        except Exception as e:
            print(f"Error during database loading: {e}")
            raise


if __name__ == "__main__":
    import sys
    import traceback
    
    try:
        # Check command line arguments for debugging
        if len(sys.argv) > 1 and sys.argv[1] == "--check-files":
            print("Running file check...")
            luigi.build([CheckDataFiles()])
        elif len(sys.argv) > 1 and sys.argv[1] == "--local-scheduler":
            print("Running with local scheduler...")
            luigi.build([LoadToDatabase()], local_scheduler=True)
        else:
            print("Running full ETL pipeline...")
            luigi.build([LoadToDatabase()])
    except Exception as e:
        print(f"PIPELINE ERROR: {e}")
        print("Full traceback:")
        traceback.print_exc()
        sys.exit(1)
