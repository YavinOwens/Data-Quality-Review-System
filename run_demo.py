#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Oracle HR Table Schema Scraper Demo

This script runs the scraper on the sample URLs and launches the web viewer 
to demonstrate the functionality.
"""

import os
import subprocess
import time
import webbrowser
import threading
from oracle_table_scraper import OracleTableScraper

def run_scraper():
    """Run the scraper on the sample URLs."""
    print("Running Oracle HR Table Schema Scraper...")
    
    # Create output directory if it doesn't exist
    if not os.path.exists('oracle_tables'):
        os.makedirs('oracle_tables')
    
    # Run the scraper
    scraper = OracleTableScraper('oracle_hr_urls.csv', 'oracle_tables')
    scraper.run()
    
    print("Scraping completed!")

def run_web_viewer():
    """Run the web viewer and open it in a browser."""
    print("Starting web viewer...")
    
    # Run the web viewer in a separate process
    process = subprocess.Popen(['python', 'web_viewer.py'], 
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
    
    # Wait for the server to start
    time.sleep(3)
    
    # Open the browser
    webbrowser.open('http://localhost:5000')
    
    print("Web viewer is running at http://localhost:5000")
    print("Press Ctrl+C to stop the server.")
    
    try:
        # Wait for the process to complete
        process.wait()
    except KeyboardInterrupt:
        # Kill the process if the user presses Ctrl+C
        process.kill()
        print("Web viewer stopped.")

def main():
    """Main function to run the demo."""
    # Check if templates directory exists, if not create it
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    if not os.path.exists(templates_dir):
        os.makedirs(templates_dir)
    
    # Create index.html template if it doesn't exist
    index_html_path = os.path.join(templates_dir, 'index.html')
    if not os.path.exists(index_html_path):
        with open(index_html_path, 'w') as f:
            f.write("""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Oracle HR Table Schemas</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        .table-container {
            margin-top: 20px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1 class="mt-4 mb-4">Oracle HR Table Schemas</h1>
        
        <div class="table-container">
            <table class="table table-striped">
                <thead>
                    <tr>
                        <th>Table Name</th>
                        <th>Description</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {% for table in tables %}
                    <tr>
                        <td>{{ table.name }}</td>
                        <td>{{ table.description }}</td>
                        <td>
                            <a href="/table/{{ table.name }}" class="btn btn-primary btn-sm">View Schema</a>
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
            """)
    
    # Create table.html template if it doesn't exist
    table_html_path = os.path.join(templates_dir, 'table.html')
    if not os.path.exists(table_html_path):
        with open(table_html_path, 'w') as f:
            f.write("""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ table.name }} - Oracle HR Table Schema</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        .schema-container {
            margin-top: 20px;
        }
        .table-info {
            margin-bottom: 20px;
            padding: 15px;
            background-color: #f8f9fa;
            border-radius: 5px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1 class="mt-4 mb-4">{{ table.name }} Schema</h1>
        
        <div class="table-info">
            <h4>Table Information</h4>
            <dl class="row">
                {% if info.description %}
                <dt class="col-sm-3">Description</dt>
                <dd class="col-sm-9">{{ info.description }}</dd>
                {% endif %}
                
                {% if info.owner %}
                <dt class="col-sm-3">Owner/Schema</dt>
                <dd class="col-sm-9">{{ info.owner }}</dd>
                {% endif %}
                
                {% if info.scrape_date %}
                <dt class="col-sm-3">Scraped On</dt>
                <dd class="col-sm-9">{{ info.scrape_date }}</dd>
                {% endif %}
            </dl>
        </div>
        
        <a href="/" class="btn btn-secondary mb-3">Back to Tables</a>
        
        <div class="schema-container">
            <h4>Column Definitions</h4>
            <table class="table table-striped">
                <thead>
                    <tr>
                        <th>Column Name</th>
                        <th>Data Type</th>
                        <th>Nullable</th>
                        {% if schema and schema[0].pk is defined %}
                        <th>PK</th>
                        {% endif %}
                        {% if schema and schema[0].description is defined %}
                        <th>Description</th>
                        {% endif %}
                    </tr>
                </thead>
                <tbody>
                    {% for column in schema %}
                    <tr>
                        <td>{{ column.column_name }}</td>
                        <td>{{ column.data_type }}</td>
                        <td>{{ column.nullable }}</td>
                        {% if schema[0].pk is defined %}
                        <td>{{ column.pk }}</td>
                        {% endif %}
                        {% if schema[0].description is defined %}
                        <td>{{ column.description }}</td>
                        {% endif %}
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
            """)
    
    # Run the scraper first
    run_scraper()
    
    # Then run the web viewer
    run_web_viewer()

if __name__ == '__main__':
    main()
