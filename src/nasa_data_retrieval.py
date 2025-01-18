"""NASA Data Retrieval Script

This script fetches and processes space weather data from NASA's DONKI API,
specifically focusing on Coronal Mass Ejections (CMEs) and 
Geomagnetic Storms (GSTs).

The script performs the following operations:
1. Fetches CME data for a specified time period
2. Fetches GST data for the same period
3. Processes and cleans the data
4. Analyzes relationships between CMEs and GSTs
5. Exports the processed data to CSV format

Required environment variables:
- NASA_API_KEY: Your NASA API key
"""
# Global variables
NASA_API_KEY = None

# Standard library imports
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Third-party imports
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import seaborn as sns
import yaml
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nasa_data.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class RetryHandler:
    """Handler for retrying failed API requests with exponential backoff."""

    def __init__(self, max_retries=3, base_delay=1):
        """Initialize RetryHandler with retry parameters.

        Args:
            max_retries (int): Maximum number of retry attempts
            base_delay (int): Base delay between retries in seconds
        """
        self.max_retries = max_retries
        self.base_delay = base_delay

    def execute_with_retry(self, func):
        """Execute a function with retry logic.

        Args:
            func (callable): Function to execute

        Returns:
            The result of the function execution

        Raises:
            requests.exceptions.RequestException: If all retries fail
        """
        for attempt in range(self.max_retries):
            try:
                return func()
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise
                wait_time = self.base_delay * (2 ** attempt)
                logging.warning(f"Request failed: {str(e)}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

# Initialize retry handler for API requests
retry_handler = RetryHandler(max_retries=3, base_delay=1)

class DataValidationError(Exception):
    """Custom exception for data validation errors."""
    pass

class DateRangeError(DataValidationError):
    """Custom exception for date range validation errors."""
    pass

class CMEDataError(DataValidationError):
    """Custom exception for CME data validation errors."""
    pass

class GSTDataError(DataValidationError):
    """Custom exception for GST data validation errors."""
    pass

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def load_api_key():
    """Load NASA API key from environment variables and set it globally."""
    global NASA_API_KEY
    
    if not load_dotenv():
        sys.exit("Error: .env file not found. Please create one with your NASA_API_KEY.")
    
    NASA_API_KEY = os.getenv("NASA_API_KEY")
    if not NASA_API_KEY:
        sys.exit("Error: NASA_API_KEY not found in .env file.")


def validate_date_range(start_date: datetime, end_date: datetime) -> None:
    """
    Validate the date range for API queries.

    Args:
        start_date (datetime): Start date for data retrieval
        end_date (datetime): End date for data retrieval

    Raises:
        DateRangeError: If date range is invalid
    """
    if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
        raise DateRangeError("Start and end dates must be datetime objects")

    if start_date > end_date:
        raise DateRangeError("Start date must be before or equal to end date")

    if (end_date - start_date).days > 365:
        raise DateRangeError("Date range cannot exceed 1 year")

    if start_date.year < 2010:
        raise DateRangeError("Data is only available from 2010 onwards")

def validate_cme_data(cme_data: list) -> None:
    """
    Validate CME data structure and required fields.

    Args:
        cme_data (list): Raw CME data from NASA DONKI API

    Raises:
        CMEDataError: If data validation fails
    """
    if not isinstance(cme_data, list):
        raise CMEDataError("CME data must be a list")

    for cme in cme_data:
        if not isinstance(cme, dict):
            raise CMEDataError("Each CME entry must be a dictionary")

        required_fields = ["activityID", "startTime"]
        missing_fields = [field for field in required_fields if field not in cme]
        if missing_fields:
            raise CMEDataError(f"Missing required CME fields: {', '.join(missing_fields)}")

        if "cmeAnalyses" in cme and cme["cmeAnalyses"]:
            analysis = cme["cmeAnalyses"][0]
            if not isinstance(analysis, dict):
                raise CMEDataError("CME analysis must be a dictionary")

def validate_gst_data(gst_data: list) -> None:
    """
    Validate GST data structure and required fields.

    Args:
        gst_data (list): Raw GST data from NASA DONKI API

    Raises:
        GSTDataError: If data validation fails
    """
    if not isinstance(gst_data, list):
        raise GSTDataError("GST data must be a list")

    for gst in gst_data:
        if not isinstance(gst, dict):
            raise GSTDataError("Each GST entry must be a dictionary")

        required_fields = ["gstID", "startTime", "allKpIndex"]
        missing_fields = [field for field in required_fields if field not in gst]
        if missing_fields:
            raise GSTDataError(f"Missing required GST fields: {', '.join(missing_fields)}")

        if not isinstance(gst.get("allKpIndex", []), list):
            raise GSTDataError("allKpIndex must be a list")

    
def get_cme_data(start_date: datetime, end_date: datetime) -> Dict[str, Any]:
    """Fetch Coronal Mass Ejection (CME) data from NASA's DONKI API.
    
    Args:
        start_date (datetime): Start date for data retrieval
        end_date (datetime): End date for data retrieval
        
    Returns:
        dict: JSON response containing CME data
        
    Raises:
        DateRangeError: If date range is invalid
        CMEDataError: If data validation fails
        requests.exceptions.RequestException: If API request fails
    """
    try:
        validate_date_range(start_date, end_date)
        
        url = "https://api.nasa.gov/DONKI/CME"
        params = {
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
            "api_key": NASA_API_KEY
        }
        
        logging.info(f"Fetching CME data from {start_date} to {end_date}")
        
        def make_request():
            return requests.get(url, params=params)
            
        response = retry_handler.execute_with_retry(make_request)
        data = response.json()
        
        validate_cme_data(data)
        logging.info(f"Successfully retrieved and validated {len(data)} CME records")
        return data
        
    except (DateRangeError, CMEDataError) as e:
        logging.error(f"Validation error: {str(e)}")
        raise
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching CME data: {str(e)}")
        raise


def get_gst_data(start_date: datetime, end_date: datetime) -> Dict[str, Any]:
    """Fetch Geomagnetic Storm (GST) data from NASA's DONKI API.
    
    Args:
        start_date (datetime): Start date for data retrieval
        end_date (datetime): End date for data retrieval
        
    Returns:
        dict: JSON response containing GST data
        
    Raises:
        DateRangeError: If date range is invalid
        GSTDataError: If data validation fails
        requests.exceptions.RequestException: If API request fails
    """
    try:
        validate_date_range(start_date, end_date)
        
        url = "https://api.nasa.gov/DONKI/GST"
        params = {
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
            "api_key": NASA_API_KEY
        }
        
        logging.info(f"Fetching GST data from {start_date} to {end_date}")
        
        def make_request():
            return requests.get(url, params=params)
            
        response = retry_handler.execute_with_retry(make_request)
        data = response.json()
        
        validate_gst_data(data)
        logging.info(f"Successfully retrieved and validated {len(data)} GST records")
        return data
        
    except (DateRangeError, GSTDataError) as e:
        logging.error(f"Validation error: {str(e)}")
        raise
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching GST data: {str(e)}")
        raise

def safe_float_convert(value, default=float('nan')):
    """
    Safely convert a value to float, returning default if conversion fails.
    
    Args:
        value: Value to convert
        default: Default value to return if conversion fails
        
    Returns:
        float: Converted value or default if conversion fails
    """
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
        
def process_cme_data(cme_data):
    """
    Process and clean Coronal Mass Ejection (CME) data.
    
    Args:
        cme_data (list): Raw CME data from NASA DONKI API
        
    Returns:
        pd.DataFrame: Processed CME data with following columns:
            - cmeID: Unique identifier for the CME
            - cmeStartTime: Start time of the CME
            - speed: Speed of the CME in km/s
            - type: Type of CME (e.g., 'S', 'C', 'R')
            - angle: Principal angle of the CME
            - latitude: Source latitude
            - longitude: Source longitude
    """
    processed = []
    for cme in cme_data:
        # Extract relevant CME analysis data
        analysis = cme.get("cmeAnalyses", [{}])[0]
        logging.debug(f"Processing CME record: {cme.get('activityID')}")
        logging.debug(f"Raw analysis data: {analysis}")
        
        entry = {
            "cmeID": cme.get("activityID"),
            "cmeStartTime": pd.to_datetime(cme.get("startTime")),
            "speed": safe_float_convert(analysis.get("speed")),
            "type": analysis.get("type", "Unknown"),
            "angle": safe_float_convert(analysis.get("principalAngle")),
            "latitude": safe_float_convert(analysis.get("latitude")),
            "longitude": safe_float_convert(analysis.get("longitude"))
        }
        
        logging.debug(f"Processed entry: {entry}")
        processed.append(entry)
        
    # Create DataFrame and log column names for debugging
    df = pd.DataFrame(processed)
    logging.debug(f"CME DataFrame columns: {df.columns.tolist()}")
    logging.debug(f"CME DataFrame first row: {df.iloc[0] if not df.empty else 'Empty DataFrame'}")
    return df

def process_gst_data(gst_data, cme_df):
    """
    Process Geomagnetic Storm (GST) data and link it with CME events.
    
    Args:
        gst_data (list): Raw GST data from NASA DONKI API
        cme_df (pd.DataFrame): Processed CME data
        
    Returns:
        pd.DataFrame: Combined GST and CME data with following columns:
            - cmeID: Linked CME identifier
            - cmeStartTime: Start time of the CME
            - gstID: GST event identifier
            - gstStartTime: Start time of the GST
            - timeDifferenceHours: Time difference between CME and GST
            - kpIndex: Kp-index indicating storm strength
    """
    processed = []
    for gst in gst_data:
        gst_id = gst.get("gstID")
        gst_time = pd.to_datetime(gst.get("startTime"))
        kp_index = gst.get("allKpIndex", [{}])[0].get("kpIndex", 0)
        
        for event in gst.get("linkedEvents", []):
            if "-CME-" in event.get("activityID", ""):
                cme_id = event.get("activityID")
                cme_match = cme_df[cme_df["cmeID"] == cme_id]
                
                if not cme_match.empty:
                    cme_row = cme_match.iloc[0]
                    cme_time = cme_row["cmeStartTime"]
                    time_diff = (gst_time - cme_time).total_seconds() / 3600
                    
                    processed.append({
                        "cmeID": cme_id,
                        "cmeStartTime": cme_time,
                        "gstID": gst_id,
                        "gstStartTime": gst_time,
                        "timeDifferenceHours": time_diff,
                        "kpIndex": kp_index,
                        "speed": cme_row.get("speed", float('nan')),
                        "type": cme_row.get("type", "Unknown"),
                        "angle": cme_row.get("angle", float('nan')),
                        "latitude": cme_row.get("latitude", float('nan')),
                        "longitude": cme_row.get("longitude", float('nan'))
                    })
    return pd.DataFrame(processed)

def analyze_cme_gst_correlation(combined_df: pd.DataFrame) -> Dict[str, float]:
    """
    Calculate correlation coefficients between CME and GST characteristics.
    
    Args:
        combined_df (pd.DataFrame): Processed and combined CME-GST data
        
    Returns:
        dict: Dictionary containing correlation coefficients
    """
    correlations = {
        'speed_kp_correlation': combined_df['speed'].corr(combined_df['kpIndex']),
        'time_diff_kp_correlation': combined_df['timeDifferenceHours'].corr(combined_df['kpIndex']),
        'speed_time_diff_correlation': combined_df['speed'].corr(combined_df['timeDifferenceHours'])
    }
    return correlations

def analyze_propagation_times(combined_df: pd.DataFrame) -> Dict[str, float]:
    """
    Analyze the time delays between CMEs and their associated GSTs.
    
    Args:
        combined_df (pd.DataFrame): Processed and combined CME-GST data
        
    Returns:
        dict: Dictionary containing propagation time statistics
    """
    time_stats = {
        'mean_propagation_time': combined_df['timeDifferenceHours'].mean(),
        'median_propagation_time': combined_df['timeDifferenceHours'].median(),
        'std_propagation_time': combined_df['timeDifferenceHours'].std(),
        'min_propagation_time': combined_df['timeDifferenceHours'].min(),
        'max_propagation_time': combined_df['timeDifferenceHours'].max()
    }
    return time_stats

def generate_summary_statistics(combined_df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """
    Generate comprehensive summary statistics for the space weather events.
    
    Args:
        combined_df (pd.DataFrame): Processed and combined CME-GST data
        
    Returns:
        dict: Dictionary containing summary statistics
    """
    logging.debug("Combined DataFrame columns available for statistics: %s", combined_df.columns.tolist())
    
    # Add type conversion to ensure numeric columns
    numeric_columns = ['speed', 'kpIndex', 'angle', 'latitude', 'longitude']
    for col in numeric_columns:
        if col in combined_df.columns:
            combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')

    try:
        # Define statistics to compute for each numeric column
        stats_to_compute = ['mean', 'median', 'std', 'min', 'max']
        cme_stats = {}
        
        # Only compute statistics for columns that exist in the dataframe
        for col in numeric_columns:
            if col in combined_df.columns:
                cme_stats[col] = combined_df[col].agg(stats_to_compute).to_dict()
    except KeyError as e:
        logging.error("Failed to calculate statistics. Missing column: %s", str(e))
        logging.debug("Available columns: %s", combined_df.columns.tolist())
        cme_stats = {}
    
    event_counts = {
        'total_cmes': len(combined_df['cmeID'].unique()),
        'total_gsts': len(combined_df['gstID'].unique()),
        'linked_events': len(combined_df)
    }
    
    return {
        'cme_statistics': cme_stats,
        'event_counts': event_counts,
        'correlations': analyze_cme_gst_correlation(combined_df),
        'propagation_times': analyze_propagation_times(combined_df)
    }
def create_speed_kp_scatter(combined_df: pd.DataFrame, output_dir: str) -> None:
    """Create a scatter plot of CME speeds vs Kp indices."""
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=combined_df, x='speed', y='kpIndex', alpha=0.6)
    plt.title('CME Speed vs Geomagnetic Storm Strength')
    plt.xlabel('CME Speed (km/s)')
    plt.ylabel('Kp Index')
    plt.savefig(os.path.join(output_dir, 'speed_kp_correlation.png'))
    plt.close()

def create_propagation_histogram(combined_df: pd.DataFrame, output_dir: str) -> None:
    """Create a histogram of CME-to-GST propagation times."""
    plt.figure(figsize=(10, 6))
    sns.histplot(data=combined_df, x='timeDifferenceHours', bins=30)
    plt.title('Distribution of CME-to-GST Propagation Times')
    plt.xlabel('Propagation Time (hours)')
    plt.ylabel('Count')
    plt.savefig(os.path.join(output_dir, 'propagation_times.png'))
    plt.close()

def create_monthly_events(combined_df: pd.DataFrame, output_dir: str) -> None:
    """Create a line plot of monthly event counts."""
    monthly_events = combined_df.set_index('cmeStartTime').resample('M').size()
    plt.figure(figsize=(12, 6))
    monthly_events.plot(kind='line', marker='o')
    plt.title('Monthly Space Weather Events')
    plt.xlabel('Date')
    plt.ylabel('Number of Events')
    plt.grid(True)
    plt.savefig(os.path.join(output_dir, 'monthly_events.png'))
    plt.close()

def export_results(combined_df: pd.DataFrame, summary_stats: dict, output_dir: str) -> None:
    """Export analysis results in multiple formats."""
    # Create the output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Create a copy of the dataframe to avoid modifying the original
    export_df = combined_df.copy()
    
    # Convert timezone-aware datetime columns to timezone-naive UTC
    datetime_columns = export_df.select_dtypes(include=['datetime64[ns, UTC]']).columns
    for col in datetime_columns:
        export_df[col] = export_df[col].dt.tz_convert('UTC').dt.tz_localize(None)
    
    # Export to CSV
    export_df.to_csv(os.path.join(output_dir, 'combined_analysis.csv'), index=False)
    
    # Export summary statistics to JSON
    with open(os.path.join(output_dir, 'summary_statistics.json'), 'w') as f:
        json.dump(summary_stats, f, indent=4)
    
    # Export to Excel with multiple sheets
    with pd.ExcelWriter(os.path.join(output_dir, 'space_weather_analysis.xlsx')) as writer:
        export_df.to_excel(writer, sheet_name='Combined Data', index=False)
        pd.DataFrame(summary_stats).to_excel(writer, sheet_name='Summary Stats')

def main():
    """Main function to orchestrate the data retrieval and processing."""
    # Load configuration and API key
    load_api_key()
    try:
        # Initialize configuration and logging
        logging.info("Starting NASA data retrieval process")
        
        # Set the date range for data retrieval
        end_date = datetime.now()
        start_date = end_date - pd.Timedelta(days=30)  # Last 30 days of data
        logging.info(f"Retrieving data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Fetch data
        logging.info("Fetching CME (Coronal Mass Ejection) data...")
        cme_data = get_cme_data(start_date, end_date)
        logging.info("Fetching GST (Geomagnetic Storm) data...")
        gst_data = get_gst_data(start_date, end_date)
        
        # Process CME data
        cme_df = process_cme_data(cme_data)
        
        # Process GST data and find relationships
        combined_df = process_gst_data(gst_data, cme_df)
        
        # Save results
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        # Generate summary statistics
        logging.info("Generating summary statistics...")
        summary_stats = generate_summary_statistics(combined_df)
        logging.info(f"Found {summary_stats['event_counts']['total_cmes']} CME events and {summary_stats['event_counts']['total_gsts']} GST events")

        # Create visualizations
        create_speed_kp_scatter(combined_df, output_dir)
        create_propagation_histogram(combined_df, output_dir)
        create_monthly_events(combined_df, output_dir)

        # Export results in multiple formats
        export_results(combined_df, summary_stats, output_dir)

        print(f"Analysis complete. Results saved to {output_dir}/")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from NASA API: {e}")
        if hasattr(e.response, 'status_code'):
            logging.error(f"API Response Status Code: {e.response.status_code}")
        if hasattr(e.response, 'text'):
            logging.error(f"API Response Text: {e.response.text}")
        sys.exit(1)
    except DateRangeError as e:
        logging.error(f"Invalid date range: {e}")
        sys.exit(1)
    except (CMEDataError, GSTDataError) as e:
        logging.error(f"Data validation error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
