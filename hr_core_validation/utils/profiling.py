"""
Utility functions for generating data profile reports
"""

import pandas as pd
from ydata_profiling import ProfileReport
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProfilerConfig:
    """Configuration settings for profile report generation"""
    
    DEFAULT_CONFIG = {
        'title': None,
        'minimal': False,
        'explorative': True,
        'correlations': {
            'auto': True,
            'pearson': True,
            'spearman': True,
            'kendall': True,
            'phi_k': True,
            'cramers': True,
        },
        'missing_diagrams': {
            'bar': True,
            'matrix': True,
            'heatmap': True,
        },
        'interactions': None,
    }
    
    @classmethod
    def get_config(cls, table_name: str) -> Dict[str, Any]:
        """
        Get profile configuration for a specific table
        
        Args:
            table_name: Name of the table being profiled
            
        Returns:
            Dictionary of profile configuration settings
        """
        config = cls.DEFAULT_CONFIG.copy()
        config['title'] = f"{table_name.title()} Profile Report"
        
        # Table-specific configurations
        if table_name == 'workers':
            config.update({
                'vars': {
                    'num': {
                        'low_categorical_threshold': 0,  # Treat all numeric columns as continuous
                    }
                }
            })
        elif table_name == 'addresses':
            config.update({
                'vars': {
                    'cat': {
                        'length': True,  # Show length statistics for address fields
                    }
                }
            })
        elif table_name == 'communications':
            config.update({
                'vars': {
                    'cat': {
                        'length': True,
                        'characters': True,  # Show character statistics for email/phone
                    }
                }
            })
            
        return config

def generate_profile_report(
    df: pd.DataFrame,
    table_name: str,
    output_dir: str,
    timestamp: str
) -> Optional[str]:
    """
    Generate a profile report for a DataFrame with enhanced error handling
    
    Args:
        df: DataFrame to profile
        table_name: Name of the table (used for configuration and filename)
        output_dir: Directory to save the report
        timestamp: Timestamp string for the filename
        
    Returns:
        Path to the generated report file, or None if generation failed
    """
    try:
        # Get table-specific configuration
        config = ProfilerConfig.get_config(table_name)
        
        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Generate report filename
        report_path = output_path / f'{table_name}_profile_{timestamp}.html'
        
        logger.info(f"Generating profile report for {table_name}")
        
        # Handle empty DataFrame
        if df.empty:
            logger.warning(f"Empty DataFrame provided for {table_name}")
            return None
            
        # Handle missing values in key columns
        missing_cols = df.columns[df.isnull().any()].tolist()
        if missing_cols:
            logger.warning(f"Missing values found in columns: {missing_cols}")
            
        # Generate profile report with configuration
        profile = ProfileReport(df, **config)
        
        # Save report
        profile.to_file(str(report_path))
        
        logger.info(f"Profile report generated successfully: {report_path}")
        return report_path.name
        
    except Exception as e:
        logger.error(f"Failed to generate profile report for {table_name}")
        logger.error(f"Error: {str(e)}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        return None

def validate_data_for_profiling(df: pd.DataFrame) -> bool:
    """
    Validate DataFrame before profiling
    
    Args:
        df: DataFrame to validate
        
    Returns:
        Boolean indicating if DataFrame is valid for profiling
    """
    if df.empty:
        logger.warning("Empty DataFrame provided")
        return False
        
    if df.columns.empty:
        logger.warning("DataFrame has no columns")
        return False
        
    if df.size > 1_000_000:  # Adjust threshold as needed
        logger.warning("DataFrame is too large for profiling")
        return False
        
    return True 