# Database Queries Documentation

This directory contains both SQL queries and their Python equivalents for analyzing and managing worker data in the PostgreSQL database.

## File Structure

### SQL Files
1. `worker_queries.sql`
   - Basic worker information queries
   - Contact information queries
   - Address information queries
   - Assignment information queries
   - Complete worker profile queries

2. `analytics_queries.sql`
   - Demographic analysis
   - Assignment statistics
   - Contact method analysis
   - Geographic distribution
   - Age distribution by department
   - Assignment duration analysis
   - Nationality distribution
   - Contact type distribution

3. `data_validation_queries.sql`
   - Missing data checks
   - Date range validation
   - Duplicate record checks
   - Orphaned record checks
   - Data consistency validation
   - Reference data validation
   - Primary contact flag validation

### Python Files
1. `db_connection.py`
   - Database connection utilities
   - Query execution functions
   - DataFrame conversion utilities

2. `worker_queries.py`
   - Python implementation of worker queries
   - Returns pandas DataFrames
   - Includes documentation and examples

3. `analytics_queries.py`
   - Python implementation of analytics queries
   - Includes data visualization
   - Statistical analysis functions

## Usage

### SQL Queries
Execute SQL queries directly in PostgreSQL using:
```bash
docker exec -i postgres-db psql -U postgres -d postgres -f query_file.sql
```

### Python Scripts
1. Install required packages:
```bash
pip install -r requirements.txt
```

2. Run Python scripts:
```bash
python worker_queries.py
python analytics_queries.py
```

3. Convert to Jupyter notebooks:
   - Open in VS Code with Jupyter extension
   - Use "Convert Python Script to Jupyter Notebook" command
   - Save as .ipynb file

## Query Categories

### Worker Information
- Basic demographic data
- Contact information
- Address details
- Assignment history
- Complete worker profiles

### Analytics
- Workforce demographics
- Assignment patterns
- Geographic distribution
- Contact preferences
- Department statistics

### Data Validation
- Data quality checks
- Consistency validation
- Reference data validation
- Relationship validation
- Date range validation

## Best Practices
1. Always use parameterized queries in Python to prevent SQL injection
2. Include appropriate indexes for frequently queried columns
3. Use EXPLAIN ANALYZE to optimize query performance
4. Consider data volume when running complex analytics queries
5. Validate data quality regularly using validation queries

## Converting Python Scripts to Notebooks
The Python scripts use the #%% cell notation for easy conversion to Jupyter notebooks:
1. #%% [markdown] - Creates a markdown cell
2. #%% - Creates a code cell
3. Regular comments become part of the code cells

## Requirements
- PostgreSQL 17.4 or higher
- Python 3.8 or higher
- Required Python packages listed in requirements.txt
- Docker for running PostgreSQL container 