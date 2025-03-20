# HR Analytics and Data Validation Project

This project combines HR data analytics capabilities with comprehensive data validation using both Python and PL/SQL.

## Project Structure

```
hr_analytics_with_validations/
├── notebooks/              # Jupyter notebooks for HR data analysis
│   └── hr_analytics/      # HR analytics notebooks
├── scripts/               # Utility scripts
│   ├── notebook_converter.py  # Convert between Python and Jupyter notebooks
│   └── validation_runner.py   # Run PL-SQL validations
└── validations/           # PL-SQL validation scripts
    ├── address_validation.sql
    ├── data_quality_review.sql
    ├── date_of_birth_validation.sql
    ├── email_validation.sql
    ├── job_history_validation.sql
    ├── name_field_validation.sql
    ├── national_insurance_validation.sql
    └── validations.sql
```

## Setup Instructions

1. Ensure you have Python 3.8+ installed
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and update the connection parameters:
   ```bash
   cp .env.example .env
   ```
4. Make sure the Oracle Docker container is running with the HR schema
5. Start Jupyter Notebook:
   ```bash
   jupyter notebook
   ```

## Features

### HR Analytics
- Interactive data analysis using Jupyter notebooks
- Oracle database connection and data extraction
- Data visualization with matplotlib and seaborn
- Statistical analysis capabilities

### Data Validation
- Comprehensive PL-SQL validation scripts
- Data quality checks
- Field-level validations
- Cross-table relationship validations

## Notebook Conversion Utility

The project includes a utility script for converting between Python files and Jupyter notebooks:

### Converting Python to Notebook
```bash
python scripts/notebook_converter.py py2nb <python_file> [output_dir]
```

### Converting Notebook to Python
```bash
python scripts/notebook_converter.py nb2py <notebook_file> [output_dir]
```

## Running Validations

1. Connect to the Oracle database
2. Execute validation scripts in the following order:
   ```sql
   @validations/validations.sql
   @validations/data_quality_review.sql
   ```

## Dependencies

- Jupyter Notebook
- cx_Oracle for Oracle database connection
- pandas for data manipulation
- matplotlib and seaborn for visualization
- python-dotenv for environment variable management
- ipynb-py-convert for notebook conversion

## Notes

- Make sure the Oracle Docker container is running before executing the notebooks or validations
- The HR schema must be properly set up in the Oracle database
- Keep your `.env` file secure and never commit it to version control
- Use the notebook conversion utility if you need to convert between Python files and Jupyter notebooks
- Run validations regularly to ensure data quality
