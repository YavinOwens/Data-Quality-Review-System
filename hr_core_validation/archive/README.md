# Code Archive

This directory contains archived versions of the HR Core Validation code. Each version is stored in its own subdirectory and represents a significant milestone or change in the codebase.

## Directory Structure

```
archive/
├── v1/                     # Original version
│   └── test_validation.py  # Original validation script
├── v2/                     # Current version (in main directory)
│   └── ...
├── version_history.md      # Detailed version history
└── README.md              # This file
```

## Usage

To revert to a previous version:
1. Copy the desired version from the archive directory
2. Replace the current version in the main codebase
3. Update requirements.txt if needed (check version_history.md for dependencies)

## Version Notes

### v1 (Original)
- Basic validation functionality
- HTML report generation embedded in Python code
- No templating system
- Dependencies: pandas, pandera, plotly, numpy, sadisplay

### v2 (Current)
- Jinja2 templating system
- Separated HTML template
- Enhanced report styling
- Additional dependencies: Jinja2

## Best Practices
1. Always archive the current version before making significant changes
2. Update version_history.md with detailed change notes
3. Include all necessary files for each version
4. Document any dependency changes 