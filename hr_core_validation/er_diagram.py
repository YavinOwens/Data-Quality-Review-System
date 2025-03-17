"""
HR Core Data ER Diagram Module
"""
import os
import graphviz
import shutil
import time

def generate_er_diagram():
    """Generate ER diagram for HR Core data tables using the sadisplay-generated dot file."""
    # Input and output paths
    source_dot_file = 'hr_core_validation/er_diagram.dot'
    dot_file = 'hr_core_validation/documentation/er_diagram.dot'
    output_path = 'hr_core_validation/documentation/er_diagram'
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Copy the dot file if it doesn't exist in the documentation directory
    if not os.path.exists(dot_file) and os.path.exists(source_dot_file):
        shutil.copy(source_dot_file, dot_file)
    elif not os.path.exists(source_dot_file):
        raise FileNotFoundError(f"ER diagram source file not found: {source_dot_file}")
    
    # Render the diagram using the existing dot file
    graph = graphviz.Source.from_file(dot_file)
    graph.render(output_path, format='png', cleanup=True)
    
    # Wait for the file to exist
    png_file = output_path + '.png'
    start_time = time.time()
    while not os.path.exists(png_file) and time.time() - start_time < 10:
        time.sleep(0.1)
    
    if not os.path.exists(png_file):
        raise FileNotFoundError(f"ER diagram PNG file was not generated: {png_file}")
    
    return png_file 