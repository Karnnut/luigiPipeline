const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');

// Create Express app
const app = express();
const port = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

// Database configuration
const pool = new Pool({
  user: process.env.DB_USER || 'weather_user',
  host: process.env.DB_HOST || 'postgres',
  database: process.env.DB_NAME || 'weather_data',
  password: process.env.DB_PASSWORD || 'weather_password',
  port: process.env.DB_PORT || 5432,
});

// Test database connection
pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('Database connection error:', err.stack);
  } else {
    console.log('Database connected successfully at:', res.rows[0].now);
  }
});

// Root endpoint
app.get('/', async (req, res) => {
  try {
    // Get basic stats about the database
    const dbStats = await pool.query(`
      SELECT 
        (SELECT COUNT(*) FROM weather_data) AS total_records,
        (SELECT COUNT(DISTINCT variable) FROM weather_data) AS total_variables,
        (SELECT MIN(timestamp) FROM weather_data) AS earliest_record,
        (SELECT MAX(timestamp) FROM weather_data) AS latest_record,
        (SELECT COUNT(DISTINCT pressure_level) FROM weather_data) AS pressure_levels
    `);

    const variableStats = await pool.query(`
      SELECT 
        variable, 
        COUNT(*) AS record_count,
        MIN(value) AS min_value,
        MAX(value) AS max_value,
        AVG(value) AS avg_value
      FROM weather_data
      GROUP BY variable
      ORDER BY variable
    `);

    res.json({
      status: 'Weather Data API is running',
      databaseStats: dbStats.rows[0],
      variableStats: variableStats.rows,
      endpoints: {
        root: '/',
        variables: '/api/variables',
        dataByVariable: '/api/data/:variable',
        timeSeriesData: '/api/timeseries/:variable',
        spatialData: '/api/spatial/:variable'
      }
    });
  } catch (error) {
    console.error('Error fetching database stats:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get available variables
app.get('/api/variables', async (req, res) => {
  try {
    const result = await pool.query('SELECT DISTINCT variable FROM weather_data');
    res.json(result.rows.map(row => row.variable));
  } catch (error) {
    console.error('Error fetching variables:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get data for a specific variable
app.get('/api/data/:variable', async (req, res) => {
  const { variable } = req.params;
  const { limit = 100, offset = 0, pressure = 500 } = req.query;
  
  try {
    const result = await pool.query(
      'SELECT * FROM weather_data WHERE variable = $1 AND pressure_level = $2 ORDER BY timestamp LIMIT $3 OFFSET $4',
      [variable, pressure, limit, offset]
    );
    
    res.json(result.rows);
  } catch (error) {
    console.error(`Error fetching data for variable ${variable}:`, error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get time series data for a specific variable
app.get('/api/timeseries/:variable', async (req, res) => {
  const { variable } = req.params;
  const { lat, lon, pressure = 500 } = req.query;
  
  if (!lat || !lon) {
    return res.status(400).json({ error: 'Latitude and longitude are required' });
  }
  
  try {
    const result = await pool.query(
      `SELECT 
        timestamp, 
        value 
      FROM weather_data 
      WHERE 
        variable = $1 AND 
        pressure_level = $2 AND
        latitude BETWEEN $3 - 0.25 AND $3 + 0.25 AND 
        longitude BETWEEN $4 - 0.25 AND $4 + 0.25 
      ORDER BY timestamp`,
      [variable, pressure, parseFloat(lat), parseFloat(lon)]
    );
    
    res.json(result.rows);
  } catch (error) {
    console.error(`Error fetching time series data for variable ${variable}:`, error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get spatial data for a specific variable at a specific time
app.get('/api/spatial/:variable', async (req, res) => {
  const { variable } = req.params;
  const { timestamp, pressure = 500 } = req.query;
  
  if (!timestamp) {
    return res.status(400).json({ error: 'Timestamp is required' });
  }
  
  try {
    const result = await pool.query(
      `SELECT 
        latitude, 
        longitude, 
        value 
      FROM weather_data 
      WHERE 
        variable = $1 AND 
        pressure_level = $2 AND
        timestamp = $3
      ORDER BY latitude, longitude`,
      [variable, pressure, timestamp]
    );
    
    res.json(result.rows);
  } catch (error) {
    console.error(`Error fetching spatial data for variable ${variable}:`, error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Start the server
app.listen(port, () => {
  console.log(`Weather Data API running on port ${port}`);
});

module.exports = app; // For testing