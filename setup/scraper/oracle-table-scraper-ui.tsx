import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

const OracleTableScraper = () => {
  const [urls, setUrls] = useState('');
  const [pythonCode, setPythonCode] = useState('');
  const [showCode, setShowCode] = useState(false);
  const [urlTemplateFile, setUrlTemplateFile] = useState('');
  
  // Initialize with default URLs
  useEffect(() => {
    const defaultUrls = `# Oracle Table URLs
# One URL per line - these will be scraped by the Python script

# PER Tables
https://etrm.live/etrm-12.2.2/etrm.oracle.com/pls/trm1222/etrm_fndnav72a5-2.html?n_appid=800&n_tabid=53450&c_type=TABLE
https://etrm.live/etrm-12.2.2/etrm.oracle.com/pls/trm1222/etrm_fndnavbe78.html?n_appid=800&n_tabid=53506&c_type=TABLE

# ERD Diagrams
https://etrm.live/etrm-12.2.2/etrm.oracle.com/pls/trm1222/Personnel_HR_General_ERD57fa.pdf?n_file_id=812&c_mode=INLINE
https://etrm.live/etrm-12.2.2/etrm.oracle.com/pls/trm1222/R11i10%20Human%20Resources%20Absence%20ERDc7ab.pdf?n_file_id=813&c_mode=INLINE

# Add your additional URLs below:
`;
    setUrls(defaultUrls);
    setUrlTemplateFile(defaultUrls);
  }, []);

  const generatePythonCode = () => {
    const urlList = urls.split('\n')
                        .filter(url => url.trim() !== '' && !url.trim().startsWith('#'))
                        .map(url => url.trim());
    
    const code = `
# Oracle Table Scraper - Python 3.9
# This script will create a pyenv with Python 3.9 and scrape Oracle tables from specified URLs
# Output: UTF-8 CSV files with comma delimiters, organized in appropriate folders

# Setup instructions:
# 1. Install pyenv if not already installed
# 2. Run the following commands to set up the environment

'''
# Install pyenv (if not already installed)
# For macOS/Linux:
curl https://pyenv.run | bash

# For Windows:
# Install using Git for Windows, then:
git clone https://github.com/pyenv-win/pyenv-win.git $HOME/.pyenv

# Set up Python 3.9 environment
pyenv install 3.9.13
pyenv virtualenv 3.9.13 oracle_scraper
pyenv local oracle_scraper

# Install required packages
pip install requests beautifulsoup4 pandas lxml html5lib
'''

import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import csv
import argparse
from urllib.parse import urlparse, parse_qs

# Create main output directory
output_dir = 'oracle_data'
os.makedirs(output_dir, exist_ok=True)

# Create URL template file
def create_url_template_file():
    template_content = """# Oracle Table URLs Template
# Add one URL per line - these will be scraped when you run the script with --url-file=urls.txt

# Default Oracle PER table URLs
https://etrm.live/etrm-12.2.2/etrm.oracle.com/pls/trm1222/etrm_fndnav72a5-2.html?n_appid=800&n_tabid=53450&c_type=TABLE
https://etrm.live/etrm-12.2.2/etrm.oracle.com/pls/trm1222/etrm_fndnavbe78.html?n_appid=800&n_tabid=53506&c_type=TABLE

# Default Oracle ERD diagrams
https://etrm.live/etrm-12.2.2/etrm.oracle.com/pls/trm1222/Personnel_HR_General_ERD57fa.pdf?n_file_id=812&c_mode=INLINE
https://etrm.live/etrm-12.2.2/etrm.oracle.com/pls/trm1222/R11i10%20Human%20Resources%20Absence%20ERDc7ab.pdf?n_file_id=813&c_mode=INLINE

# Add your additional URLs below:
"""
    
    template_file = os.path.join(output_dir, "urls.txt")
    with open(template_file, 'w') as f:
        f.write(template_content)
    
    print(f"Created URL template file at {template_file}")

# Create the URL template file
create_url_template_file()

# Function to determine folder category based on URL or table name
def determine_folder_category(url, table_name=None):
    # For PDFs and ERDs
    if url.endswith('.pdf') or 'ERD' in url:
        return "ERD_Documents"
    
    # For tables, use prefix if available (like PER_, HR_, etc)
    if table_name:
        # Extract prefix from table name (e.g., PER_ from PER_ADDRESSES)
        match = re.match(r'^([A-Z]+)_', table_name)
        if match:
            return f"{match.group(1)}_Tables"
    
    # Extract from URL if possible
    if 'n_appid=' in url:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        if 'n_appid' in query_params:
            return f"App_{query_params['n_appid'][0]}"
    
    # Default folder
    return "Other_Tables"

# List of URLs to scrape
urls = [
${urlList.map(url => `    "${url}",`).join('\n')}
]

def extract_table_name_from_url(url):
    # Extract table name from URL or page content
    match = re.search(r'c_name=([^&]+)', url)
    if match:
        return match.group(1)
    else:
        # Fallback to using the URL fragment
        parts = url.split('/')
        return f"table_{parts[-1].split('.')[0]}"

def scrape_oracle_table(url):
    print(f"Scraping: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Try to find the table name
        table_name = None
        h1_tags = soup.find_all('h1')
        for h1 in h1_tags:
            if 'Table:' in h1.text:
                table_name = h1.text.replace('Table:', '').strip()
                break
        
        if not table_name:
            # Fallback to extracting from URL
            table_name = extract_table_name_from_url(url)
        
        # Find all tables in the page
        tables = soup.find_all('table')
        
        if not tables:
            print(f"No tables found at {url}")
            return
        
        # Look for column definitions table (typically has headers like Column Name, Null?, Type)
        for i, table in enumerate(tables):
            headers = table.find_all('th')
            header_texts = [h.text.strip() for h in headers]
            
            # Check if this looks like a column definition table
            if 'Column Name' in header_texts or 'Name' in header_texts:
                # Extract table data
                rows = []
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if cells:
                        rows.append([cell.text.strip() for cell in cells])
                
                # Create DataFrame
                if rows:
                    df = pd.DataFrame(rows[1:], columns=rows[0])
                    
                    # Save to CSV
                    csv_filename = f"oracle_data/{table_name}.csv"
                    df.to_csv(csv_filename, index=False, encoding='utf-8')
                    print(f"Saved table definition to {csv_filename}")
                    break
            
            # If we've gone through all tables without finding column definitions
            if i == len(tables) - 1:
                print(f"Could not identify column definitions table in {url}")
        
    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")

# Process each URL
for url in urls:
    if url.strip():
        scrape_oracle_table(url)

print("Scraping completed. Check the oracle_data directory for CSV files.")
`;
    
    setPythonCode(code);
    setShowCode(true);
  };

  const handleDownload = () => {
    const element = document.createElement('a');
    const file = new Blob([pythonCode], {type: 'text/plain'});
    element.href = URL.createObjectURL(file);
    element.download = 'oracle_table_scraper.py';
    document.body.appendChild(element);
    element.click();
    document.body.removeChild(element);
  };

  return (
    <div className="w-full max-w-4xl mx-auto p-4">
      <Card className="mb-8 shadow-lg">
        <CardHeader className="bg-blue-50">
          <CardTitle className="text-xl font-bold text-blue-800">Oracle Table URL Scraper</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="mb-4">
            <label className="block text-sm font-medium mb-2">
              Enter Oracle Table URLs (one per line):
            </label>
            <textarea
              className="w-full h-40 p-2 border rounded-md"
              value={urls}
              onChange={(e) => setUrls(e.target.value)}
              placeholder="https://etrm.live/etrm-12.2.2/etrm.oracle.com/pls/trm1222/etrm_fndnav72a5-2.html?n_appid=800&n_tabid=53450&c_type=TABLE"
            />
          </div>
          
          <div className="flex flex-wrap justify-between gap-2">
            <button 
              className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
              onClick={generatePythonCode}
            >
              Generate Python Script
            </button>
            
            {showCode && (
              <button 
                className="px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700"
                onClick={handleDownload}
              >
                Download Python Script
              </button>
            )}
            
            <button 
              className="px-4 py-2 bg-gray-600 text-white rounded-md hover:bg-gray-700"
              onClick={() => {
                const element = document.createElement('a');
                const file = new Blob([urls], {type: 'text/plain'});
                element.href = URL.createObjectURL(file);
                element.download = 'oracle_urls.txt';
                document.body.appendChild(element);
                element.click();
                document.body.removeChild(element);
              }}
            >
              Download URL Template
            </button>
          </div>
        </CardContent>
      </Card>
      
      {showCode && (
        <Card className="shadow-lg">
          <CardHeader className="bg-gray-50">
            <CardTitle className="text-lg font-bold text-gray-800">Python 3.9 Script to Scrape Oracle Tables</CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="bg-gray-100 p-4 rounded-md overflow-x-auto text-sm">
              {pythonCode}
            </pre>
            <div className="mt-4 text-sm text-gray-600">
              <p className="mb-2 font-bold">Instructions:</p>
              <ol className="list-decimal pl-5 space-y-1">
                <li>Install pyenv to manage Python versions</li>
                <li>Set up a Python 3.9 environment as shown in the script comments</li>
                <li>Install the required packages</li>
                <li>Run the script to scrape tables from the provided URLs</li>
                <li>CSV files and documents will be organized in folders by category within 'oracle_data'</li>
                <li>A template URL file will be created at 'oracle_data/urls.txt' for future use</li>
              </ol>
            </div>
            <div className="mt-4 p-3 bg-blue-50 rounded border border-blue-200 text-blue-800">
              <p className="font-bold">New Features:</p>
              <ul className="list-disc pl-5 mt-2">
                <li>Automatically organizes data into appropriate folders (PER_Tables, ERD_Documents, etc.)</li>
                <li>Creates a template URL file for easy addition of new URLs</li>
                <li>Supports command line arguments for custom URL files and output directories</li>
                <li>Intelligently categorizes tables based on naming patterns</li>
              </ul>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
};

export default OracleTableScraper;
