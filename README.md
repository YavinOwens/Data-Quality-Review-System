# HR Core Data Validation

A Python-based data validation framework for HR Core data, providing comprehensive data quality checks, profiling, and visualization.

## Features

- Data validation using Pandera schemas
- Data profiling with ydata-profiling
- Interactive visualization with Plotly
- Entity-Relationship diagram generation
- Comprehensive HTML reports
- Support for multiple data types (Workers, Assignments, Addresses, Communications)

## Project Structure

```
hr_core_validation/
├── data_sources/          # Source CSV files
│   ├── workers.csv
│   ├── assignments.csv
│   ├── addresses.csv
│   └── communications.csv
├── documentation/         # Generated reports and diagrams
│   ├── er_diagram.png
│   ├── validation_summary_*.html
│   └── *_profile_*.html
└── tests/                # Validation scripts and tests
    └── test_validation.py
```

## Requirements

- Python 3.8+
- Dependencies listed in requirements.txt

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd hr_core_validation
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Place your CSV files in the `data_sources` directory
2. Run the validation script:
```bash
python hr_core_validation/tests/test_validation.py
```
3. View the results in the `documentation` directory

## Data Validation Rules

### Workers
- Unique ID validation
- Required fields: First Name, Last Name
- Date format validation for Birth Date and Effective Dates
- Nationality code length check (2-3 characters)

### Addresses
- Unique ID validation
- Required fields: Worker ID, Address Type
- Address Type enumeration (HOME, WORK, OTHER)
- Postal code format validation
- Date format validation for Effective Dates

### Communications
- Unique ID validation
- Required fields: Worker ID, Contact Type, Contact Value
- Contact Type enumeration (EMAIL, PHONE)
- Date format validation for Effective Dates

## Output

The validation process generates:
1. Validation summary report (HTML)
2. Data profile reports for each table
3. Entity-Relationship diagram
4. Success rate visualizations
5. Table-level quality analysis

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your License Here]
