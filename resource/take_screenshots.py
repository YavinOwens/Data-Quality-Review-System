#!/usr/bin/env python3
import os
import time
import subprocess
import sys

def display_notification(message):
    """Display a notification using AppleScript"""
    applescript = f'''
    display notification "{message}" with title "Screenshot Automation"
    '''
    subprocess.run(['osascript', '-e', applescript])

def take_screenshot(filename):
    """Take a screenshot using screencapture utility"""
    filepath = os.path.join(os.getcwd(), 'resource', filename)
    subprocess.run(['screencapture', '-o', '-w', filepath])
    display_notification(f"Saved screenshot as {filename}")

def countdown(seconds):
    """Display a countdown notification"""
    for i in range(seconds, 0, -1):
        display_notification(f"Taking next screenshot in {i} seconds...")
        time.sleep(1)

if __name__ == "__main__":
    # List of screenshots to take with descriptions
    screenshots = [
        ("homepage.png", "Home Page - Navigate to http://localhost:5006"),
        ("schemas.png", "Schemas Page - Navigate to http://localhost:5006/schemas"),
        ("table_detail.png", "Table Detail Page - Open a specific table from schemas page"),
        ("erds.png", "ERD Diagrams Page - Navigate to http://localhost:5006/erds"),
        ("text_info.png", "Text Info Page - Navigate to http://localhost:5006/text-info"),
        ("text_info_detail.png", "Text Info Detail Page - Open a specific text info from the list"),
        ("search_results.png", "Search Results - Perform a search on any page"),
        ("mobile_view.png", "Mobile View - Resize browser to mobile dimensions")
    ]
    
    # First screenshot is already taken
    start_index = 1
    
    # Notification that we're starting
    display_notification("Screenshot automation starting. Please prepare the browser window.")
    time.sleep(3)
    
    # Take screenshots with countdowns between them
    for i in range(start_index, len(screenshots)):
        filename, instruction = screenshots[i]
        display_notification(f"Prepare for screenshot {i+1}/8: {instruction}")
        time.sleep(5)  # Time to prepare the page
        countdown(3)
        take_screenshot(filename)
        time.sleep(2)  # Brief pause after screenshot
    
    display_notification("All screenshots completed!")
