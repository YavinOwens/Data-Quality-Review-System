"""
Module for automatically updating the Schema page with ERD diagrams.
"""

import os
from typing import Optional
from datetime import datetime
from helpers.db_connection import DatabaseConnection
from helpers.schema_visualizer import generate_erd_dot, generate_erd_image

def update_schema_page(conn: DatabaseConnection, 
                      readme_path: str = "README.md",
                      output_dir: str = "static/images") -> bool:
    """
    Update the Schema page section in README.md with the latest ERD diagram.
    
    Args:
        conn: DatabaseConnection object
        readme_path: Path to the README.md file
        output_dir: Directory where images are stored
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    try:
        # Generate new ERD diagram
        dot_file = generate_erd_dot(conn, output_dir)
        png_file = generate_erd_image(dot_file)
        
        if not png_file:
            return False
            
        # Get relative path for the image
        relative_png_path = os.path.relpath(png_file, os.path.dirname(readme_path))
        
        # Read current README content
        with open(readme_path, 'r') as f:
            content = f.read()
            
        # Define markers for the schema section
        schema_start = "## Database Schema"
        schema_end = "##"  # Next section marker
        
        # Create new schema section content
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_schema_section = f"""## Database Schema

This section provides a visual representation of the database schema using an Entity-Relationship Diagram (ERD).
The diagram shows tables, their columns, and relationships between tables.

### Entity-Relationship Diagram
*Last updated: {timestamp}*

![Database Schema ERD]({relative_png_path})

### Legend
- Tables are represented as boxes with table names at the top
- Each row in a table box represents a column with its data type
- Arrows indicate foreign key relationships between tables
- The crow's foot notation (→) indicates a many-to-one relationship

"""
        
        # Find the schema section in the current content
        start_idx = content.find(schema_start)
        if start_idx == -1:
            # If section doesn't exist, append it at the end
            updated_content = content + "\n" + new_schema_section
        else:
            # Find the end of the schema section
            end_idx = content.find(schema_end, start_idx + len(schema_start))
            if end_idx == -1:
                end_idx = len(content)
            
            # Replace the existing schema section
            updated_content = (
                content[:start_idx] +
                new_schema_section +
                content[end_idx:]
            )
        
        # Write updated content back to README
        with open(readme_path, 'w') as f:
            f.write(updated_content)
            
        # Clean up DOT file
        if os.path.exists(dot_file):
            os.remove(dot_file)
            
        return True
        
    except Exception as e:
        print(f"Error updating schema page: {str(e)}")
        return False

def get_schema_section(readme_path: str = "README.md") -> Optional[str]:
    """
    Extract the current schema section from the README file.
    
    Args:
        readme_path: Path to the README.md file
        
    Returns:
        Optional[str]: The current schema section content, or None if not found
    """
    try:
        with open(readme_path, 'r') as f:
            content = f.read()
            
        schema_start = "## Database Schema"
        schema_end = "##"
        
        start_idx = content.find(schema_start)
        if start_idx == -1:
            return None
            
        end_idx = content.find(schema_end, start_idx + len(schema_start))
        if end_idx == -1:
            return content[start_idx:]
            
        return content[start_idx:end_idx].strip()
        
    except Exception as e:
        print(f"Error reading schema section: {str(e)}")
        return None 