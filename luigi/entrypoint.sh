#!/bin/bash

# Wait for the PostgreSQL database to be ready
echo "Waiting for PostgreSQL to start..."
until PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\q'; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 5
done

echo "PostgreSQL is up - continuing"

# Run the Luigi pipeline
echo "Starting Luigi pipeline..."
python -m tasks.grib_to_postgres ProcessAllGribFiles --data-dir /app/data/raw --local-scheduler 

# Keep the container running
tail -f /dev/null