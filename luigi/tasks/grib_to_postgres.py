import os
import luigi
import xarray as xr
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('grib_pipeline')

# Database connection parameters
DB_PARAMS = {
    'host': os.environ.get('POSTGRES_HOST', 'postgres'),
    'port': os.environ.get('POSTGRES_PORT', '5432'),
    'user': os.environ.get('POSTGRES_USER', 'weather_user'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'weather_password'),
    'database': os.environ.get('POSTGRES_DB', 'weather_data')
}

class ExtractGribData(luigi.Task):
    """
    Task to extract data from a GRIB file and prepare it for loading
    """
    grib_file = luigi.Parameter()
    variables = luigi.ListParameter(default=['t', 'u', 'v', 'r', 'z'])  # Select variables to extract
    
    def output(self):
        # Output will be a parquet file with processed data
        filename = os.path.basename(self.grib_file).replace('.grib', '')
        return luigi.LocalTarget(f"data/processed/{filename}_processed.parquet")
    
    def run(self):
        logger.info(f"Processing GRIB file: {self.grib_file}")
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        
        try:
            # Try to open the GRIB file with xarray
            # First, try without filters
            try:
                ds = xr.open_dataset(self.grib_file, engine='cfgrib')
                logger.info(f"Successfully opened GRIB file: {self.grib_file}")
            except Exception as e:
                # If that fails, try with different numberOfPoints filters
                logger.warning(f"Failed to open GRIB file without filters: {e}")
                logger.info("Trying to open with numberOfPoints filter...")
                
                # Try the first numberOfPoints value
                try:
                    ds = xr.open_dataset(self.grib_file, engine='cfgrib', 
                                       backend_kwargs={'filter_by_keys': {'numberOfPoints': 2600}})
                    logger.info(f"Successfully opened GRIB file with numberOfPoints=2600")
                except Exception:
                    # Try the second numberOfPoints value
                    ds = xr.open_dataset(self.grib_file, engine='cfgrib', 
                                       backend_kwargs={'filter_by_keys': {'numberOfPoints': 660}})
                    logger.info(f"Successfully opened GRIB file with numberOfPoints=660")
            
            # Select variables of interest
            variables = [var for var in self.variables if var in ds.data_vars]
            if not variables:
                raise ValueError(f"None of the requested variables {self.variables} found in the dataset")
            
            # Process each variable
            processed_data = []
            
            for var in variables:
                logger.info(f"Processing variable: {var}")
                
                # Extract the variable data
                var_data = ds[var]
                
                # Create a DataFrame from the data
                # Flattening the multi-dimensional data
                for time_idx, time_val in enumerate(var_data.time.values):
                    for pressure_idx, pressure_val in enumerate(var_data.isobaricInhPa.values):
                        # Extract 2D slice (latitude x longitude)
                        if 'number' in var_data.dims:
                            # For ensemble forecast data (Dataset 2)
                            for number_idx, number_val in enumerate(var_data.number.values):
                                slice_data = var_data.isel(time=time_idx, isobaricInhPa=pressure_idx, number=number_idx)
                                
                                # Convert to pandas DataFrame
                                df = slice_data.to_dataframe().reset_index()
                                
                                # Add metadata columns
                                df['variable'] = var
                                df['forecast_number'] = number_val
                                df['timestamp'] = pd.to_datetime(time_val)
                                df['pressure_level'] = pressure_val
                                
                                processed_data.append(df)
                        else:
                            # For deterministic forecast data (Dataset 1)
                            slice_data = var_data.isel(time=time_idx, isobaricInhPa=pressure_idx)
                            
                            # Convert to pandas DataFrame
                            df = slice_data.to_dataframe().reset_index()
                            
                            # Add metadata columns
                            df['variable'] = var
                            df['forecast_number'] = 0  # Not an ensemble forecast
                            df['timestamp'] = pd.to_datetime(time_val)
                            df['pressure_level'] = pressure_val
                            
                            processed_data.append(df)
            
            # Combine all processed data
            if processed_data:
                combined_df = pd.concat(processed_data, ignore_index=True)
                
                # Save to parquet
                combined_df.to_parquet(self.output().path, index=False)
                logger.info(f"Successfully processed data and saved to: {self.output().path}")
            else:
                raise ValueError("No data was processed")
            
        except Exception as e:
            logger.error(f"Error processing GRIB file: {e}")
            raise

class LoadToPostgres(luigi.Task):
    """
    Task to load processed data into PostgreSQL
    """
    grib_file = luigi.Parameter()
    chunk_size = luigi.IntParameter(default=10000)  # Number of rows to insert at once
    
    def requires(self):
        return ExtractGribData(grib_file=self.grib_file)
    
    def output(self):
        # Marker file to indicate the data has been loaded
        filename = os.path.basename(self.grib_file).replace('.grib', '')
        return luigi.LocalTarget(f"data/loaded/{filename}_loaded.txt")
    
    def run(self):
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        
        input_file = self.input().path
        logger.info(f"Loading data from: {input_file}")
        
        try:
            # Read the processed data
            df = pd.read_parquet(input_file)
            
            # Connect to the PostgreSQL database
            conn = psycopg2.connect(**DB_PARAMS)
            cursor = conn.cursor()
            
            # Create table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                variable VARCHAR(10),
                forecast_number INT,
                timestamp TIMESTAMP,
                pressure_level FLOAT,
                latitude FLOAT,
                longitude FLOAT,
                value FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_table_query)
            
            # Create indexes for better query performance
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_weather_data_timestamp ON weather_data (timestamp);
            CREATE INDEX IF NOT EXISTS idx_weather_data_variable ON weather_data (variable);
            CREATE INDEX IF NOT EXISTS idx_weather_data_coords ON weather_data (latitude, longitude);
            """)
            
            # Process data in chunks to avoid memory issues
            total_rows = len(df)
            chunks = (total_rows + self.chunk_size - 1) // self.chunk_size
            
            logger.info(f"Starting to load {total_rows} rows in {chunks} chunks")
            
            # Rename the value column based on the variable
            for i in range(chunks):
                start_idx = i * self.chunk_size
                end_idx = min((i + 1) * self.chunk_size, total_rows)
                chunk_df = df.iloc[start_idx:end_idx]
                
                # Prepare data for insertion
                values_list = []
                
                for _, row in chunk_df.iterrows():
                    variable = row['variable']
                    value = row[variable]  # The actual value is in the column named after the variable
                    
                    values_list.append((
                        row['variable'],
                        int(row['forecast_number']),
                        row['timestamp'],
                        float(row['pressure_level']),
                        float(row['latitude']),
                        float(row['longitude']),
                        float(value) if not pd.isna(value) else None
                    ))
                
                # Insert the data
                insert_query = """
                INSERT INTO weather_data (variable, forecast_number, timestamp, pressure_level, latitude, longitude, value)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(insert_query, values_list)
                conn.commit()
                
                logger.info(f"Inserted chunk {i+1}/{chunks} ({len(values_list)} rows)")
            
            # Close the database connection
            cursor.close()
            conn.close()
            
            # Create the output marker file
            with self.output().open('w') as f:
                f.write(f"Data loaded at {datetime.now()}")
            
            logger.info(f"Successfully loaded data into PostgreSQL")
            
        except Exception as e:
            logger.error(f"Error loading data to PostgreSQL: {e}")
            raise

class ProcessAllGribFiles(luigi.WrapperTask):
    """
    Process all GRIB files in the data directory
    """
    data_dir = luigi.Parameter(default="data/raw")
    
    def requires(self):
        # Find all GRIB files in the data directory
        grib_files = []
        for root, _, files in os.walk(self.data_dir):
            for file in files:
                if file.endswith('.grib'):
                    grib_files.append(os.path.join(root, file))
        
        logger.info(f"Found {len(grib_files)} GRIB files to process")
        return [LoadToPostgres(grib_file=grib_file) for grib_file in grib_files]

if __name__ == "__main__":
    luigi.run(local_scheduler=True)