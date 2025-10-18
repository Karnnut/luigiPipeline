from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import sqlalchemy
from sqlalchemy import create_engine, text
import os
from typing import Optional

# Create FastAPI app
app = FastAPI(title="Weather Data API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
DB_USER = os.getenv("DB_USER", "luigi")
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "weather_data")
DB_PASSWORD = os.getenv("DB_PASSWORD", "luigi")
DB_PORT = os.getenv("DB_PORT", "5432")

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create engine
engine = create_engine(DB_URL, pool_pre_ping=True)


@app.on_event("startup")
async def startup():
    """Test database connection on startup"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT NOW()"))
            print(f"Database connected successfully at: {result.scalar()}")
    except Exception as e:
        print(f"Database connection error: {e}")


@app.on_event("shutdown")
async def shutdown():
    """Close database connection on shutdown"""
    engine.dispose()


# Root endpoint
@app.get("/")
def root():
    """Get API status and database statistics"""
    try:
        with engine.connect() as conn:
            # Get basic stats
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) AS total_records,
                    MIN(date) AS earliest_date,
                    MAX(date) AS latest_date,
                    COUNT(DISTINCT date) AS unique_dates
                FROM weather_data
            """))
            db_stats = result.fetchone()._asdict()
            
            # Get column stats
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'weather_data' 
                AND column_name NOT IN ('date', 'time', 'latitude', 'longitude', 'isobaricinhpa')
                ORDER BY column_name
            """))
            columns = [row[0] for row in result]
            
            return {
                "status": "Weather Data API is running",
                "databaseStats": db_stats,
                "availableColumns": columns,
                "endpoints": {
                    "root": "/",
                    "columns": "/api/columns",
                    "dataByLocation": "/api/data",
                    "timeSeriesData": "/api/timeseries",
                    "spatialData": "/api/spatial"
                }
            }
    except Exception as e:
        print(f"Error fetching database stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Get available columns
@app.get("/api/columns")
def get_columns():
    """Get list of all available data columns"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'weather_data'
                ORDER BY column_name
            """))
            return [row[0] for row in result]
    except Exception as e:
        print(f"Error fetching columns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Get data by location and date
@app.get("/api/data")
def get_data(
    date: Optional[str] = Query(None),
    lat: Optional[float] = Query(None),
    lon: Optional[float] = Query(None),
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0)
):
    """Get weather data by location and/or date"""
    try:
        query = "SELECT * FROM weather_data WHERE 1=1"
        params = {}
        
        if date:
            query += " AND date = :date"
            params['date'] = date
        
        if lat is not None and lon is not None:
            query += " AND latitude BETWEEN :lat - 0.25 AND :lat + 0.25"
            query += " AND longitude BETWEEN :lon - 0.25 AND :lon + 0.25"
            params['lat'] = lat
            params['lon'] = lon
        
        query += " ORDER BY date, time LIMIT :limit OFFSET :offset"
        params['limit'] = limit
        params['offset'] = offset
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            return [dict(row._mapping) for row in result]
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Get time series data for a location
@app.get("/api/timeseries")
def get_timeseries(
    lat: Optional[float] = Query(None),
    lon: Optional[float] = Query(None),
    column: str = Query("tp", description="Column name to retrieve")
):
    """Get time series data for a specific location"""
    if lat is None or lon is None:
        raise HTTPException(
            status_code=400, 
            detail="Latitude and longitude are required"
        )
    
    try:
        with engine.connect() as conn:
            # Check if column exists
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'weather_data' AND column_name = :column
            """), {"column": column})
            
            if not result.fetchone():
                raise HTTPException(status_code=400, detail=f"Column '{column}' does not exist")
            
            # Use text() with proper parameter binding
            query = text(f"""
                SELECT 
                    date,
                    time,
                    {column} as value
                FROM weather_data 
                WHERE 
                    latitude BETWEEN :lat - 0.25 AND :lat + 0.25 AND 
                    longitude BETWEEN :lon - 0.25 AND :lon + 0.25 
                ORDER BY date, time
            """)
            
            result = conn.execute(query, {"lat": lat, "lon": lon})
            return [dict(row._mapping) for row in result]
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching time series data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Get spatial data for a specific time
@app.get("/api/spatial")
def get_spatial_data(
    date: Optional[str] = Query(None),
    time: Optional[str] = Query(None),
    column: str = Query("tp", description="Column name to retrieve")
):
    """Get spatial data for a specific date and time"""
    if date is None:
        raise HTTPException(status_code=400, detail="Date is required")
    
    try:
        with engine.connect() as conn:
            # Check if column exists
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'weather_data' AND column_name = :column
            """), {"column": column})
            
            if not result.fetchone():
                raise HTTPException(status_code=400, detail=f"Column '{column}' does not exist")
            
            query = f"""
                SELECT 
                    latitude, 
                    longitude, 
                    {column} as value
                FROM weather_data 
                WHERE date = :date
            """
            params = {"date": date}
            
            if time:
                query += " AND time = :time"
                params["time"] = time
            
            query += " ORDER BY latitude, longitude"
            
            result = conn.execute(text(query), params)
            return [dict(row._mapping) for row in result]
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching spatial data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 3000))
    uvicorn.run(app, host="0.0.0.0", port=port)
