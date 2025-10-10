FROM python:3.10-slim

WORKDIR /app

# Install system dependencies required for GRIB/CFGRIB and PostgreSQL client
RUN apt-get update && apt-get install -y \
    build-essential \
    libeccodes-dev \
    libpq-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p data/raw data/processed data/loaded tasks

# Copy Luigi configuration
COPY luigi.cfg .

# Copy the entry point script
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Set the entry point
ENTRYPOINT ["./entrypoint.sh"]