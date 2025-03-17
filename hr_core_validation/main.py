import os
import time
import webbrowser
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from hr_core_validation.validation import validate_hr_core_data
from hr_core_validation.profile_reports import generate_profile_reports
from hr_core_validation.er_diagram import generate_er_diagram

def generate_home_page(profile_reports, validation_report_name):
    """Generate the home page for the HR Core Data Quality Report."""
    env = Environment(loader=FileSystemLoader('hr_core_validation/templates'))
    template = env.get_template('home.html')
    
    # Generate the home page
    html_content = template.render(
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        profile_reports=profile_reports,
        validation_report_name=validation_report_name
    )
    
    # Save the home page
    output_path = os.path.join('hr_core_validation/documentation', 'home.html')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    return output_path

def wait_for_files(files, timeout=30):
    """Wait for files to exist, up to timeout seconds."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if all(os.path.exists(f) for f in files):
            return True
        time.sleep(0.5)
    return False

def main():
    # Create documentation directory if it doesn't exist
    os.makedirs('hr_core_validation/documentation', exist_ok=True)
    os.makedirs('hr_core_validation/documentation/profiles', exist_ok=True)
    
    # Generate timestamp for file names
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Run validation
    validation_results = validate_hr_core_data()
    
    # Generate profile reports
    profile_reports = generate_profile_reports()
    
    # Generate ER diagram
    er_diagram_path = generate_er_diagram()
    
    # Generate validation report
    env = Environment(loader=FileSystemLoader('hr_core_validation/templates'))
    template = env.get_template('validation_report.html')
    
    validation_report_name = f'validation_report_{timestamp}.html'
    validation_report_path = os.path.join('hr_core_validation/documentation', validation_report_name)
    
    html_content = template.render(
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        validation_results=validation_results,
        profile_reports=profile_reports,
        er_diagram_path='er_diagram.png',
        total_tables=len(validation_results),
        passed_tables=sum(1 for results in validation_results.values() if all(result['success'] for result in results)),
        failed_tables=sum(1 for results in validation_results.values() if any(not result['success'] for result in results))
    )
    
    with open(validation_report_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    # Generate home page
    home_page_path = generate_home_page(profile_reports, validation_report_name)
    
    # Wait for all files to be generated
    required_files = [
        er_diagram_path,
        validation_report_path,
        home_page_path
    ]
    
    if wait_for_files(required_files):
        # Open the home page in the default browser
        webbrowser.open('file://' + os.path.abspath(home_page_path))
        
        print(f"Reports generated successfully!")
        print(f"Home page: {home_page_path}")
        print(f"Validation report: {validation_report_path}")
        print(f"ER diagram: {er_diagram_path}")
    else:
        print("Warning: Some files were not generated within the timeout period.")

if __name__ == "__main__":
    main() 