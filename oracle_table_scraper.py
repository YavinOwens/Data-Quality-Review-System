#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Oracle HR Table Structure Scraper

This script scrapes Oracle HR table definitions from specified URLs and
generates a CSV file with table structure information.
"""

import os
import sys
import csv
import re
import time
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('oracle_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('oracle_scraper')

class OracleTableScraper:
    """Class for scraping Oracle HR table definitions from web pages."""
    
    def __init__(self, urls_file: str, output_dir: str):
        """
        Initialize the scraper with URLs file and output directory.
        
        Args:
            urls_file (str): Path to the CSV file containing URLs to scrape
            output_dir (str): Directory to save the output files
        """
        self.urls_file = urls_file
        self.output_dir = output_dir
        self.session = requests.Session()
        # Add headers to mimic a browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Created output directory: {output_dir}")
    
    def load_urls(self) -> List[Dict]:
        """
        Load URLs from the CSV file.
        
        Returns:
            List[Dict]: List of dictionaries with table name, URL, and description
        """
        try:
            df = pd.read_csv(self.urls_file)
            required_columns = ['table_name', 'url', 'description']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"CSV file missing required columns: {', '.join(missing_columns)}")
                return []
            
            urls = df.to_dict('records')
            logger.info(f"Loaded {len(urls)} URLs from {self.urls_file}")
            return urls
        except Exception as e:
            logger.error(f"Error loading URLs file: {e}")
            return []
    
    def fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch the content of a URL.
        
        Args:
            url (str): URL to fetch
            
        Returns:
            Optional[str]: HTML content of the page or None if there was an error
        """
        try:
            logger.info(f"Fetching {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def parse_table_definition(self, html: str) -> List[Dict]:
        """
        Parse table definition from HTML content.
        
        Args:
            html (str): HTML content of the page
            
        Returns:
            List[Dict]: List of dictionaries with column definitions
        """
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'lxml')
        logger.debug(f"Parsing HTML content of length: {len(html)}")
        
        try:
            # Look for any pre or code blocks in the HTML that might contain SQL
            code_blocks = soup.find_all(['pre', 'code'])
            logger.debug(f"Found {len(code_blocks)} code/pre blocks in HTML")
            
            sql_blocks = []
            for block in code_blocks:
                text = block.get_text()
                if 'SELECT' in text and 'FROM' in text:
                    sql_blocks.append(text)
                    logger.debug(f"Found SQL block: {text[:100]}...")
            
            if sql_blocks:
                longest_sql = max(sql_blocks, key=len)
                logger.debug(f"Using longest SQL block with length {len(longest_sql)}")
                
                # Extract column names from SQL - simpler approach
                # Remove everything before the first SELECT
                if 'SELECT' in longest_sql.upper():
                    sql_part = longest_sql[longest_sql.upper().find('SELECT'):]                    
                    # Remove everything after FROM
                    if 'FROM' in sql_part.upper():
                        sql_part = sql_part[:sql_part.upper().find('FROM')]                        
                        # Remove the SELECT keyword
                        sql_part = sql_part.replace('SELECT', '', 1).strip()
                        
                        # Split by comma and newline
                        column_parts = re.split(r',\s*\n*', sql_part)
                        logger.debug(f"Extracted {len(column_parts)} column parts from SQL")
                        
                        columns = []
                        for part in column_parts:
                            clean_part = part.strip()
                            
                            # Skip empty parts
                            if not clean_part:
                                continue
                                
                            # Remove leading/trailing parentheses, brackets, quotation marks
                            clean_part = re.sub(r'^[\(\[\{\'\"]|[\)\]\}\'\"]$', '', clean_part)
                            
                            # Extract just the column name (remove functions, aliases, etc.)
                            column_name = clean_part
                            if ' ' in column_name:
                                column_name = column_name.split(' ')[0]
                            if '.' in column_name:
                                column_name = column_name.split('.')[-1]
                                
                            # Replace aliases like 'AS' or 'as'
                            if ' AS ' in column_name.upper():
                                column_name = column_name.split(' AS ')[0].strip()
                            if ' as ' in column_name.lower():
                                column_name = column_name.split(' as ')[0].strip()
                                
                            # Remove any remaining non-alphanumeric characters except underscore
                            column_name = re.sub(r'[^a-zA-Z0-9_]', '', column_name)
                            
                            columns.append({
                                'column_name': column_name,
                                'data_type': 'VARCHAR2',  # Default since SQL query doesn't show types
                                'nullable': 'Y'     # Default since SQL query doesn't show nullable info
                            })
                        
                        if columns:
                            logger.info(f"Extracted {len(columns)} columns from SQL query")
                            return columns
            
            # Fallback: Try to find table definitions in traditional format
            tables = soup.find_all('table')
            column_table = None
            
            for table in tables:
                headers = table.find_all('th')
                header_text = [header.get_text().strip().lower() for header in headers]
                
                # Check if this table has column definitions (common column header names)
                if any(col in header_text for col in ['column name', 'data type', 'null?', 'pk']):
                    column_table = table
                    break
            
            if not column_table:
                # Fallback: Extract from columns section using code/pre elements
                columns_section = soup.find(string=re.compile(r'Columns', re.IGNORECASE))
                if columns_section:
                    # Look for list items after the Columns section
                    column_list = []
                    for sibling in columns_section.find_all_next():
                        if sibling.name == 'li':
                            column_list.append(sibling.get_text().strip())
                        elif sibling.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                            # We've moved past the columns section to another heading
                            break
                    
                    if column_list:
                        columns = []
                        for col in column_list:
                            columns.append({
                                'column_name': col,
                                'data_type': 'Unknown',
                                'nullable': 'Unknown'
                            })
                        logger.info(f"Extracted {len(columns)} columns from list items")
                        return columns
                
                # Final fallback: Look for a code block which might have a CREATE TABLE statement
                for code_block in soup.find_all(['code', 'pre']):
                    code_text = code_block.get_text()
                    if 'CREATE TABLE' in code_text.upper():
                        # Extract column definitions from CREATE TABLE statement
                        match = re.search(r'\(([\s\S]+?)\)', code_text)
                        if match:
                            column_defs = match.group(1).split(',')
                            columns = []
                            for col_def in column_defs:
                                parts = col_def.strip().split()
                                if parts:
                                    # Basic extraction - name and type only
                                    columns.append({
                                        'column_name': parts[0].strip('"'),
                                        'data_type': parts[1] if len(parts) > 1 else 'Unknown',
                                        'nullable': 'NOT NULL' if 'NOT NULL' in col_def.upper() else 'NULL'
                                    })
                            return columns
                
                logger.warning("Could not find table definitions in the HTML")
                return []
            
            # If we found a standard column table, parse it
            columns = []
            rows = column_table.find_all('tr')
            
            # Skip header row
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 3:  # Expect at least column name, data type, and nullable flag
                    column_info = {
                        'column_name': cells[0].get_text().strip(),
                        'data_type': cells[1].get_text().strip(),
                        'nullable': cells[2].get_text().strip(),
                    }
                    
                    # Add primary key flag if available
                    if len(cells) > 3:
                        column_info['pk'] = cells[3].get_text().strip()
                    
                    # Add comments/description if available
                    if len(cells) > 4:
                        column_info['description'] = cells[4].get_text().strip()
                    
                    columns.append(column_info)
            
            return columns
        except Exception as e:
            logger.error(f"Error parsing table definition: {e}")
            return []
    
    def extract_table_info(self, html: str) -> Dict:
        """
        Extract general table information from HTML content.
        
        Args:
            html (str): HTML content of the page
            
        Returns:
            Dict: Dictionary with table information
        """
        if not html:
            return {}
        
        soup = BeautifulSoup(html, 'lxml')
        table_info = {}
        
        try:
            # Look for table name and owner in header
            header_title = soup.find('h1')
            if header_title and 'TABLE:' in header_title.get_text():
                full_title = header_title.get_text().strip()
                match = re.search(r'TABLE:\s*([\w\.]+)', full_title)
                if match:
                    parts = match.group(1).split('.')
                    if len(parts) > 1:
                        table_info['owner'] = parts[0]
                        table_info['table_name'] = parts[1]
            
            # Look for description in the Object Details section
            object_details = soup.find(string=re.compile(r'Object Details', re.IGNORECASE))
            if object_details and object_details.parent:
                # Look for description text after object details
                for sibling in object_details.parent.find_next_siblings():
                    if sibling.name in ['p', 'div']:
                        desc_text = sibling.get_text().strip()
                        if desc_text and not desc_text.startswith('#') and not desc_text.lower().startswith('storage'):
                            table_info['description'] = desc_text
                            break
            
            # If we didn't find a description, try an alternative approach
            if 'description' not in table_info:
                # Look for text after the table name link and before the next heading
                table_link = soup.find('a', href=re.compile(r'.*' + table_info.get('table_name', ''), re.IGNORECASE))
                if table_link:
                    next_elem = table_link.find_next()
                    if next_elem and next_elem.name not in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        table_info['description'] = next_elem.get_text().strip()
            
            # Look for primary key information
            pk_section = soup.find(string=re.compile(r'Primary Key', re.IGNORECASE))
            if pk_section:
                pk_list = []
                # Look for list items after the Primary Key section
                for sibling in pk_section.find_all_next():
                    if sibling.name == 'li':
                        pk_list.append(sibling.get_text().strip())
                    elif sibling.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        # We've moved past the PK section to another heading
                        break
                
                if pk_list:
                    table_info['primary_key'] = ', '.join(pk_list)
            
            # Extract any other metadata available in the page
            for section_title in ['Storage Details', 'Indexes', 'Foreign Keys']:
                section = soup.find(string=re.compile(section_title, re.IGNORECASE))
                if section:
                    info_items = []
                    for sibling in section.find_all_next():
                        if sibling.name == 'li' or sibling.name == 'p':
                            info_items.append(sibling.get_text().strip())
                        elif sibling.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                            # We've moved to another heading
                            break
                    
                    if info_items:
                        key = section_title.lower().replace(' ', '_')
                        table_info[key] = '\n'.join(info_items)
            
            return table_info
        except Exception as e:
            logger.error(f"Error extracting table info: {e}")
            return {}
    
    def save_table_definition(self, table_name: str, columns: List[Dict], table_info: Dict) -> None:
        """
        Save table definition to a CSV file.
        
        Args:
            table_name (str): Name of the table
            columns (List[Dict]): List of column definitions
            table_info (Dict): General table information
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(self.output_dir, f"{table_name}_{timestamp}.csv")
        
        try:
            # If no columns were found, create a placeholder column
            if not columns:
                logger.warning(f"No columns found for table {table_name}, creating placeholder")
                # Create a placeholder column to allow the web viewer to display the table
                columns = [{
                    'column_name': 'placeholder_column',
                    'data_type': 'VARCHAR2',
                    'nullable': 'Y',
                    'description': 'No columns could be extracted from the source page. This is a placeholder.'
                }]
            
            # Create a DataFrame from the columns
            df = pd.DataFrame(columns)
            
            # Add table metadata as a separate CSV
            metadata_file = os.path.join(self.output_dir, f"{table_name}_{timestamp}_info.csv")
            
            with open(metadata_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['property', 'value'])
                writer.writerow(['table_name', table_name])
                for key, value in table_info.items():
                    writer.writerow([key, value])
                writer.writerow(['scrape_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
                # Add a note about placeholders if needed
                if len(columns) == 1 and columns[0]['column_name'] == 'placeholder_column':
                    writer.writerow(['note', 'This table contains placeholder data only. No actual columns could be extracted.'])
            
            # Save the column definitions
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            logger.info(f"Saved table definition to {output_file}")
            logger.info(f"Saved table metadata to {metadata_file}")
            return output_file
        except Exception as e:
            logger.error(f"Error saving table definition: {e}")
            return None
    
    def generate_consolidated_file(self, scraped_tables: List[Tuple[str, str]]) -> None:
        """
        Generate a consolidated CSV file with all table definitions.
        
        Args:
            scraped_tables (List[Tuple[str, str]]): List of (table_name, file_path) tuples
        """
        if not scraped_tables:
            logger.warning("No tables were scraped, skipping consolidated file")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        consolidated_file = os.path.join(self.output_dir, f"all_tables_{timestamp}.csv")
        
        try:
            # Create a DataFrame for the consolidated file
            all_columns = []
            
            for table_name, file_path in scraped_tables:
                if file_path and os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    
                    # Add table name column
                    df['table_name'] = table_name
                    
                    # Reorder columns to have table_name first
                    columns = ['table_name'] + [c for c in df.columns if c != 'table_name']
                    df = df[columns]
                    
                    all_columns.append(df)
            
            if all_columns:
                consolidated_df = pd.concat(all_columns, ignore_index=True)
                consolidated_df.to_csv(consolidated_file, index=False, encoding='utf-8')
                logger.info(f"Generated consolidated file: {consolidated_file}")
            else:
                logger.warning("No table definitions found for consolidation")
        except Exception as e:
            logger.error(f"Error generating consolidated file: {e}")
    
    def run(self) -> None:
        """Run the scraper to process all URLs."""
        urls = self.load_urls()
        if not urls:
            logger.error("No URLs loaded, exiting")
            return
        
        scraped_tables = []
        
        for item in urls:
            table_name = item.get('table_name')
            url = item.get('url')
            
            if not table_name or not url:
                logger.warning(f"Missing table_name or url in item: {item}")
                continue
            
            logger.info(f"Processing table: {table_name}")
            
            html = self.fetch_page(url)
            if not html:
                continue
            
            # Add delay to avoid overwhelming the server
            time.sleep(2)
            
            columns = self.parse_table_definition(html)
            table_info = self.extract_table_info(html)
            
            # Add description from the CSV if not found in the HTML
            if 'description' not in table_info and 'description' in item:
                table_info['description'] = item['description']
            
            output_file = self.save_table_definition(table_name, columns, table_info)
            if output_file:
                scraped_tables.append((table_name, output_file))
        
        # Generate consolidated file
        self.generate_consolidated_file(scraped_tables)
        
        logger.info(f"Scraping completed. Processed {len(urls)} URLs, scraped {len(scraped_tables)} tables.")


def main():
    """Main function to parse arguments and run the scraper."""
    parser = argparse.ArgumentParser(description='Scrape Oracle HR table definitions from web pages')
    parser.add_argument('--urls', required=False, default='oracle_hr_urls.csv',
                      help='Path to the CSV file containing URLs to scrape')
    parser.add_argument('--output', required=False, default='oracle_tables',
                      help='Directory to save the output files')
    
    args = parser.parse_args()
    
    scraper = OracleTableScraper(args.urls, args.output)
    scraper.run()


if __name__ == '__main__':
    main()
