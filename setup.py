from setuptools import setup, find_packages

setup(
    name="python_offline",
    version="0.1",
    packages=find_packages(include=['main', 'main.*']),
    install_requires=[
        "streamlit>=1.31.0",
        "pandas>=2.2.0",
        "numpy>=1.26.0",
        "psycopg2-binary>=2.9.9",
        "requests>=2.31.0",
        "sqlalchemy>=2.0.0",
        "streamlit-option-menu==0.3.12",
        "graphviz==0.20.1",
        "streamlit-aggrid==0.3.4"
    ],
    author="Yavin Owens",
    author_email="your.email@example.com",
    description="A package for data engineering and analytics with validation",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/YavinOwens/Python_offline",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.10",
) 