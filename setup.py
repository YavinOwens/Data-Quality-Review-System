from setuptools import setup, find_packages

setup(
    name="hr_core_validation",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "jinja2",
        "pandas",
        "numpy",
        "ydata-profiling",
        "graphviz"
    ],
) 