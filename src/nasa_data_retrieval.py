import os
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()
NASA_API_KEY = os.getenv('NASA_API_KEY')

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
        
        url = f"https://api.nasa.gov/DONKI/CME"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'api_key': NASA_API_KEY
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        logging.info(f"Successfully retrieved CME data from {start_date} to {end_date}")
        return response.json()
        
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
        
        url = f"https://api.nasa.gov/DONKI/GST"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'api_key': NASA_API_KEY
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        logging.info(f"Successfully retrieved {len(data)} GST records from {start_date} to {end_date}")
        if data:
            logging.info(f"Sample GST record structure: {data[0]}")
        return data
        
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
                'time': cme.get('startTime')
            }
            processed_data.append(cme_info)
        
        df = pd.DataFrame(processed_data)
        df['cmeTime'] = pd.to_datetime(df['time'])
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
            logging.debug(f"Processing GST record: {gst.get('gstID', 'unknown')}")
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
                    logging.debug(f"No linked events for GST: {gst.get('gstID', 'unknown')}")
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
                        logging.info(f"Found valid linked CME: {event_id}")
                    else:
                        logging.debug(f"Skipping non-CME event with ID: {event_id}")

                if not cme_links:
                    logging.info(f"Skipping GST {gst.get('gstID', 'unknown')} - no valid linked CMEs found")
                    continue
                for cme_id in cme_links:
                    linked_cme = cme_df[cme_df['cmeID'] == cme_id]
                    if linked_cme.empty:
                        logging.warning(f"No matching CME found in database for ID: {cme_id}")
                        logging.debug(f"Available CME IDs: {cme_df['cmeID'].unique()}")
                        continue
                    else:
                        logging.info(f"Successfully matched CME {cme_id} with GST {gst.get('gstID', 'unknown')}")
                            
                    cme_time = linked_cme['time'].iloc[0]
                    time_diff = (gst_time - cme_time).total_seconds() / 3600  # hours
                        
                    kp_index_data = gst.get('allKpIndex', [])
                    kp_index = 0
                    if kp_index_data and isinstance(kp_index_data[0], dict):
                        kp_index = kp_index_data[0].get('kpIndex', 0)
                    
                    gst_info = {
                        'cmeID': cme_id,
                        'cmeTime': cme_time,
                        'gstID': gst.get('gstID', ''),
                        'gstTime': gst_time,
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
        # Set date range as specified in requirements
        start_date = datetime(2013, 5, 1)
        end_date = datetime(2024, 5, 1)
        
        # Get data
        cme_data = get_cme_data(start_date, end_date)
        gst_data = get_gst_data(start_date, end_date)
        
        # Process data
        cme_df = process_cme_data(cme_data)
        gst_df = process_gst_data(gst_data, cme_df)
        
        # Merge and clean data
        final_df = merge_and_clean_data(cme_df, gst_df)
        
        # Create output directory if it doesn't exist
        os.makedirs('output', exist_ok=True)
        
        # Save to CSV
        output_file = 'output/space_weather_data.csv'
        final_df.to_csv(output_file, index=False)
        logging.info(f"Data successfully saved to {output_file}")
        
        return final_df
        
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()

