import sqlalchemy
import polars as pl
import os

# Database connection
DB_URL = "postgresql://luigi:luigi@localhost:5433/weather_data"

# Output file
OUTPUT_FILE = "data/exports/weather_data.parquet"

def export_to_parquet():
    """Export weather_data table from PostgreSQL to Parquet"""
    
    print("📦 Connecting to database...")
    engine = sqlalchemy.create_engine(DB_URL)
    
    # Get row count
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text("SELECT COUNT(*) FROM weather_data"))
        total_rows = result.scalar()
        print(f"📊 Total rows to export: {total_rows:,}")
    
    print("📥 Reading data from PostgreSQL...")
    # Read directly with Polars (faster than pandas)
    query = "SELECT * FROM weather_data ORDER BY date, time"
    df = pl.read_database_uri(query, uri=DB_URL)
    
    print(f"✅ Loaded {df.height:,} rows with {len(df.columns)} columns")
    print(f"📋 Columns: {df.columns}")
    
    # Create output directory
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    # Write to Parquet
    print(f"💾 Writing to {OUTPUT_FILE}...")
    df.write_parquet(OUTPUT_FILE, compression="snappy")
    
    # Get file size
    file_size = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)  # MB
    print(f"✅ Export complete! File size: {file_size:.2f} MB")
    
    # Show sample
    print("\n📄 Sample data:")
    print(df.head(3))
    
    engine.dispose()

if __name__ == "__main__":
    export_to_parquet()
