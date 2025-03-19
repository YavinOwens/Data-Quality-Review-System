# Oracle HR Tables Project

This repository contains two main components:

1. **PL/SQL Validation Scripts** - A suite of PL/SQL scripts for validating and standardizing Oracle HR table data
2. **HR Table Schema Scraper** - A Python tool for scraping Oracle HR table definitions and generating CSV documentation

## PL/SQL Validation Scripts

### Oracle HR Table Validation Scripts

1. **validations.sql** - Main validations for people table
2. **name_field_validation.sql** - Validations for name fields
3. **email_validation.sql** - Validations for email fields
4. **address_validation.sql** - Validations for address fields
5. **job_history_validation.sql** - Validations for job history
6. **national_insurance_validation.sql** - Validations for UK National Insurance numbers
7. **date_of_birth_validation.sql** - Validations for date of birth fields
8. **data_quality_review.sql** - Data quality review summary

### Execution Scripts

1. **executions.sql** - Master execution script that runs all validations in sequence

## HR Table Schema Scraper

A Python-based tool for scraping Oracle HR table definitions from web pages and generating structured CSV output.

### Requirements

- Python 3.9+ (currently using Python 3.9.18)
- Required packages:
  - requests
  - beautifulsoup4
  - pandas
  - lxml
  - flask (for web viewer)

### Setup

1. Set up a Python virtual environment:

   ```bash
   # Install the virtual environment
   pyenv install 3.9.18
   pyenv virtualenv 3.9.18 hr-schema-scraper
   pyenv local hr-schema-scraper
   
   # Install required packages
   pip install requests beautifulsoup4 pandas lxml flask
   ```

2. Configure the URLs to scrape:
   - Edit the `oracle_hr_urls.csv` file to add or modify table URLs

### Usage

1. Run the scraper:

   ```bash
   python oracle_table_scraper.py --urls oracle_hr_urls.csv --output oracle_tables
   ```

2. Run the web viewer to browse scraped schemas and ERD diagrams:

   ```bash
   python web_viewer.py
   ```

   Then open your browser to [http://localhost:5001](http://localhost:5001)

3. View ERD diagrams for tables:
   - Each table has an associated ERD button if a diagram is available
   - Diagrams are mapped to tables based on naming patterns and explicit mappings
   - You can also view all available ERD diagrams in the dedicated section on the home page

### Files

- **oracle_table_scraper.py** - Main scraper script
- **web_viewer.py** - Flask web application for viewing scraped schemas and ERD diagrams
- **oracle_hr_urls.csv** - CSV file containing the URLs to scrape
- **oracle_hr_erd.csv** - CSV file containing the URLs for ERD diagrams

## Usage Examples

### Running PL/SQL Validations

To run all validations:

```sql
@executions.sql
```

To run specific validations, use the relevant script and call the procedures:

```sql
-- Example for address validations
@address_validation.sql
EXEC create_address_clean_copy;
EXEC standardize_uk_counties;
EXEC validate_uk_postcodes;
EXEC clean_address_spaces;
EXEC identify_incomplete_addresses;
```

### Scraping Oracle HR Tables

```bash
# Run the scraper with default settings
python oracle_table_scraper.py

# Run with custom URL file and output directory
python oracle_table_scraper.py --urls custom_urls.csv --output output_dir
```
