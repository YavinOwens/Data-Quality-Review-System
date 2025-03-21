from setuptools import setup, find_packages

setup(
    name="hr_core_validation",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        # Testing and Development
        "pytest>=8.3.5",
        "mypy>=1.15.0",
        "pylint>=3.3.6",
        "black>=25.1.0",
        "rope>=1.13.0",

        # Database Connectivity
        "psycopg2-binary>=2.9.10",
        "SQLAlchemy>=2.0.39",

        # Data Processing and Analysis
        "numpy>=2.2.4",
        "pandas>=2.2.3",
        "polars>=1.25.2",
        "python-dateutil>=2.9.0.post0",
        "pytz>=2025.1",

        # Data Validation and Quality
        "pydantic>=2.10.6",
        "pydantic-core>=2.27.2",

        # Visualization and Documentation
        "matplotlib>=3.10.1",
        "seaborn>=0.13.2",
        "Jinja2>=3.1.6",
        "Pillow>=11.1.0",

        # Jupyter Environment
        "jupyter>=1.1.1",
        "notebook>=7.3.3",
        "ipynb-py-convert>=0.4.6",
        "ipython>=9.0.2",
        "jupyterlab>=4.3.6",

        # Additional Dependencies
        "python-dotenv>=1.0.1",
        "PyYAML>=6.0.2",
        "requests>=2.32.3",
        "typing_extensions>=4.12.2",
        "toml>=0.10.2"
    ],
    python_requires=">=3.10",
) 