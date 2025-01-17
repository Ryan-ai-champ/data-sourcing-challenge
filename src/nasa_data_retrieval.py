import os
import time
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from requests.exceptions import RequestException
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)

# Load environment variables
load_dotenv()
NASA_API_KEY = os.getenv('NASA_API_KEY')

# API rate limiting constants
MIN_REQUEST_INTERVAL = 1.0  # Minimum seconds between requests
MAX_RETRIES = 3  # Maximum number of retry attempts
RETRY_DELAY_BASE = 2  # Base for exponential backoff

def make_nasa_request(url: str, params: Dict[str, Any], retry_count: int = 0) -> Optional[Dict]:
    """
    Make a request to NASA API with retry logic and rate limiting.
    
    Args:
        url: The API endpoint URL
        params: Query parameters for the request
        retry_count: Current retry attempt number
        
    Returns:
        JSON response from the API or None if all retries fail
        
    Raises:
        RequestException: If the request fails after all retries
    """
    try:
        # Add delay between requests
        time.sleep(MIN_REQUEST_INTERVAL)
        
        response = requests.get(url, params=params)
        
        if response.status_code == 429:  # Rate limit exceeded
            if retry_count >= MAX_RETRIES:
                raise RequestException(f"Rate limit exceeded after {MAX_RETRIES} retries")
            
            # Calculate exponential backoff delay
            delay = RETRY_DELAY_BASE ** retry_count
            logging.warning(f"Rate limit hit. Waiting {delay} seconds before retry {retry_count + 1}/{MAX_RETRIES}")
            time.sleep(delay)
            return make_nasa_request(url, params, retry_count + 1)
            
        elif response.status_code == 403:  # Invalid API key
            raise RequestException("Invalid NASA API key. Please check your .env file")
            
        elif response.status_code != 200:
            if retry_count >= MAX_RETRIES:
                raise RequestException(f"Request failed with status {response.status_code} after {MAX_RETRIES} retries")
            
            delay = RETRY_DELAY_BASE ** retry_count
            logging.warning(f"Request failed. Waiting {delay} seconds before retry {retry_count + 1}/{MAX_RETRIES}")
            time.sleep(delay)
            return make_nasa_request(url, params, retry_count + 1)
            
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        if retry_count >= MAX_RETRIES:
            raise RequestException(f"Connection error after {MAX_RETRIES} retries: {str(e)}")
        
        delay = RETRY_DELAY_BASE ** retry_count
        logging.warning(f"Connection error. Waiting {delay} seconds before retry {retry_count + 1}/{MAX_RETRIES}")
        time.sleep(delay)
        return make_nasa_request(url, params, retry_count + 1)

def get_cme_data(start_date, end_date):
    """
    Retrieve CME (Coronal Mass Ejection) data from NASA API.
    
    Args:
        start_date (datetime): Start date for data retrieval
        end_date (datetime): End date for data retrieval
        
    Returns:
        list: List of CME events
    """
    try:
        url = "https://api.nasa.gov/DONKI/CME"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'api_key': NASA_API_KEY
        }
        
        logging.info(f"Requesting CME data from {start_date} to {end_date}")
        response_data = make_nasa_request(url, params)
        
        if not response_data:
            raise RequestException("Failed to retrieve CME data after all retries")
            
        logging.info(f"Successfully retrieved {len(response_data)} CME records")
        return response_data
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error retrieving CME data: {str(e)}")
        raise

def get_gst_data(start_date, end_date):
    """
    Retrieve GST (Geomagnetic Storm) data from NASA API.
    
    Args:
        start_date (datetime): Start date for data retrieval
        end_date (datetime): End date for data retrieval
        
    Returns:
        list: List of GST events
    """
    try:
        url = "https://api.nasa.gov/DONKI/GST"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'api_key': NASA_API_KEY
        }
        
        logging.info(f"Requesting GST data from {start_date} to {end_date}")
        response_data = make_nasa_request(url, params)
        
        if not response_data:
            raise RequestException("Failed to retrieve GST data after all retries")
            
        logging.info(f"Successfully retrieved {len(response_data)} GST records")
        if response_data:
            logging.debug(f"Sample GST record structure: {response_data[0]}")
        return response_data
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error retrieving GST data: {str(e)}")
        raise

def process_cme_data(cme_data):
    """
    Process and clean CME data from NASA DONKI API.
    
    Args:
        cme_data (list): Raw CME data from NASA API containing fields:
            - activityID: Unique identifier for the CME
            - startTime: Start time of the CME event
            - link: URL to event details
            
    Returns:
        DataFrame: Processed CME data with columns:
            - cmeID: Unique identifier for the CME
            - time: Start time of the CME event
    """
    try:
        # Extract relevant fields
        processed_data = []
        for cme in cme_data:
            cme_info = {
                'cmeID': cme.get('activityID', ''),
                'startTime': cme.get('startTime')
            }
            processed_data.append(cme_info)
        
        df = pd.DataFrame(processed_data)
        df['cmeTime'] = pd.to_datetime(df['startTime'])
        df = df[['cmeID', 'cmeTime']]  # Keep only required fields
        
        logging.info(f"Processed {len(processed_data)} CME records")
        return df
        
    except Exception as e:
        logging.error(f"Error processing CME data: {str(e)}")
        raise

def process_gst_data(gst_data, cme_df):
    """
    Process and clean GST data, linking with CME events.
    
    Args:
        gst_data (list): Raw GST data from NASA API
        cme_df (DataFrame): Processed CME data for linking
        
    Returns:
        DataFrame: Processed GST data with CME links
    
    Raises:
        ValueError: If gst_data is None or empty
        KeyError: If required fields are missing in GST data
    """
    if gst_data is None:
        raise ValueError("GST data is None")
        
    if not isinstance(gst_data, list):
        raise ValueError("GST data must be a list")
        
    if not gst_data:
        logging.warning("No GST data found for the specified period")
        return pd.DataFrame()
        
    processed_data = []
    
    try:
        logging.info(f"Processing {len(gst_data)} raw GST records")
        for gst in gst_data:
            # Log the complete GST record for debugging
            try:
                if not isinstance(gst, dict):
                    raise ValueError(f"Invalid GST record: {gst}")
                        
                linked_events = gst.get('linkedEvents', [])
                if linked_events:
                    logging.info(f"Found {len(linked_events)} linked events for GST: {gst.get('gstID', 'unknown')}")
                    # Log details about each linked event
                    for idx, event in enumerate(linked_events):
                        logging.info(f"  Linked Event {idx + 1}:")
                        logging.info(f"    Type: {event.get('activityType', 'unknown')}")
                        logging.info(f"    ID: {event.get('activityID', 'unknown')}")
                else:
                    logging.info(f"No linked events found for GST: {gst.get('gstID', 'unknown')}")
                if linked_events is None:
                    continue
                        
                start_time = gst.get('startTime')
                if not start_time:
                    raise ValueError(f"Missing start time for GST: {gst.get('gstID', 'unknown')}")
                        
                try:
                    gst_time = pd.to_datetime(start_time)
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Invalid GST time format: {start_time}")
                        
                # Process linked CME events
                # Process linked CME events
                cme_links = []
                for event in linked_events:
                    if not isinstance(event, dict):
                        logging.warning(f"Invalid linked event format: {event}")
                        continue
                        
                    event_id = event.get('activityID', '')

                    # Check if the event is a CME based on ID pattern
                    if event_id and '-CME-' in event_id:
                        cme_links.append(event_id)

                if not cme_links:
                    logging.info(f"Skipping GST {gst.get('gstID', 'unknown')} - no valid linked CMEs found")
                    continue
                for cme_id in cme_links:
                    linked_cme = cme_df[cme_df['cmeID'] == cme_id]
                    if linked_cme.empty:
                        logging.warning(f"No matching CME found for ID: {cme_id}")
                        continue
                            
                    cme_time = linked_cme['cmeTime'].iloc[0]
                    time_diff = (gst_time - cme_time).total_seconds() / 3600  # hours
                        
                    kp_index_data = gst.get('allKpIndex', [])
                    kp_index = 0
                    if kp_index_data and isinstance(kp_index_data[0], dict):
                        kp_index = kp_index_data[0].get('kpIndex', 0)
                    
                    gst_info = {
                        'cmeID': cme_id,
                        'cmeTime': pd.to_datetime(cme_time),
                        'gstID': gst.get('gstID', ''),
                        'gstTime': pd.to_datetime(gst_time),
                        'timeDifferenceHours': time_diff,
                        'kpIndex': kp_index
                    }
                    processed_data.append(gst_info)
            except Exception as e:
                logging.warning(f"Error processing GST record: {str(e)}")
                continue
        
        # Create final DataFrame
        df = pd.DataFrame(processed_data)
        if not df.empty:
            # Ensure all datetime fields are properly formatted
            df['gstTime'] = pd.to_datetime(df['gstTime'])
            df['cmeTime'] = pd.to_datetime(df['cmeTime'])
            
            # Calculate time difference in hours
            df['timeDifferenceHours'] = (df['gstTime'] - df['cmeTime']).dt.total_seconds() / 3600
            
            # Keep only required columns in specific order
            df = df[['cmeID', 'cmeTime', 'gstID', 'gstTime', 'timeDifferenceHours', 'kpIndex']]
        
        logging.info(f"Processed {len(processed_data)} GST records")
        return df
            
    except Exception as e:
        logging.error(f"Error processing GST data: {str(e)}")
        raise

def merge_and_clean_data(cme_df, gst_df):
    """
    Merge and clean CME and GST data to create final dataset.
    
    Args:
        cme_df (DataFrame): Processed CME data with columns:
            - cmeID: Unique identifier for the CME
            - time: Start time of the CME event
        gst_df (DataFrame): Processed GST data with columns: 
            - cmeID: ID of linked CME event
            - cmeTime: Start time of CME event
            - gstID: Unique identifier for the GST
            - gstTime: Start time of GST event
            - timeDifferenceHours: Hours between CME and GST
            - kpIndex: Geomagnetic storm KP index
            
    Returns:
        DataFrame: Merged and cleaned data containing only linked CME-GST events
    """
    try:
        # We only need the GST dataframe since it already contains linked CME info
        if gst_df.empty:
            logging.warning("No GST records with linked CMEs found")
            return pd.DataFrame()
        
        # Sort by GST time
        cleaned_df = gst_df.sort_values('gstTime')
        
        # Reset index
        cleaned_df = cleaned_df.reset_index(drop=True)
        
        logging.info(f"Successfully merged and cleaned data. Final dataset has {len(cleaned_df)} records")
        return cleaned_df
        
    except Exception as e:
        logging.error(f"Error merging and cleaning data: {str(e)}")
        raise

def main():
    """
    Main execution function.
    """
    try:
        # Set specific date range per requirements 
        end_date = datetime(2024, 5, 1)
        start_date = datetime(2013, 5, 1)
        
        # Get data
        cme_data = get_cme_data(start_date, end_date)
        gst_data = get_gst_data(start_date, end_date)
        
        # Process data
        cme_df = process_cme_data(cme_data)
        gst_df = process_gst_data(gst_data, cme_df)
        
        # Merge and clean data
        final_df = merge_and_clean_data(cme_df, gst_df)
        
        # Calculate summary statistics
        if not final_df.empty:
            avg_time_diff = final_df['timeDifferenceHours'].mean()
            min_time_diff = final_df['timeDifferenceHours'].min()
            max_time_diff = final_df['timeDifferenceHours'].max()
            std_time_diff = final_df['timeDifferenceHours'].std()
            
            logging.info("\nSummary Statistics:")
            logging.info(f"Average time between CME and GST: {avg_time_diff:.2f} hours")
            logging.info(f"Minimum time difference: {min_time_diff:.2f} hours")
            logging.info(f"Maximum time difference: {max_time_diff:.2f} hours")
            logging.info(f"Standard deviation: {std_time_diff:.2f} hours")
        
        # Create output directory if it doesn't exist
        os.makedirs('output', exist_ok=True)
        
        # Save to CSV
        output_file = 'output/space_weather_data.csv'
        final_df.to_csv(output_file, index=False)
        logging.info(f"\nData successfully saved to {output_file}")
        
        # Save summary statistics to a separate file
        if not final_df.empty:
            stats_file = 'output/summary_statistics.txt'
            with open(stats_file, 'w') as f:
                f.write("Summary Statistics for CME-GST Time Differences\n")
                f.write("===========================================\n\n")
                f.write(f"Average time between CME and GST: {avg_time_diff:.2f} hours\n")
                f.write(f"Minimum time difference: {min_time_diff:.2f} hours\n")
                f.write(f"Maximum time difference: {max_time_diff:.2f} hours\n")
                f.write(f"Standard deviation: {std_time_diff:.2f} hours\n")
            logging.info(f"Summary statistics saved to {stats_file}")
        
        return final_df
        
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()

