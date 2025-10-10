FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for GRIB files and PostgreSQL
RUN apt-get update && apt-get install -y \
    libeccodes-dev \
    libpq-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directories
RUN mkdir -p data/raw data/processed

# Default command
CMD ["python", "etl_pipeline.py"]