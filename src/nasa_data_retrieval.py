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
        
        logging.info(f"Successfully retrieved GST data from {start_date} to {end_date}")
        return response.json()
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error retrieving GST data: {str(e)}")
        raise

def process_cme_data(cme_data):
    """
    Process and clean CME data.
    
    Args:
        cme_data (list): Raw CME data from NASA API
        
    Returns:
        DataFrame: Processed CME data
    """
    try:
        # Extract relevant fields
        processed_data = []
        for cme in cme_data:
            cme_info = {
                'time': cme.get('startTime'),
                'speed': cme.get('speed', 0),
                'type': 'CME',
                'halfAngle': cme.get('halfAngle', 0),
                'note': cme.get('note', ''),
                'latitude': cme.get('latitude', 0),
                'longitude': cme.get('longitude', 0),
                'cmeID': cme.get('activityID', '')
            }
            processed_data.append(cme_info)
        
        df = pd.DataFrame(processed_data)
        df['time'] = pd.to_datetime(df['time'])
        
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
        for gst in gst_data:
            # Process each GST record
            try:
                if not isinstance(gst, dict):
                    raise ValueError(f"Invalid GST record: {gst}")
                        
                linked_events = gst.get('linkedEvents')
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
                cme_links = []
                for event in linked_events:
                    if isinstance(event, dict) and event.get('activityType') == 'CME':
                        cme_id = event.get('activityID')
                        if cme_id:
                            cme_links.append(cme_id)
                            logging.debug(f"Found linked CME: {cme_id}")
                
                if not cme_links:  # Skip GSTs without linked CMEs
                    continue
                        
                for cme_id in cme_links:
                    linked_cme = cme_df[cme_df['cmeID'] == cme_id]
                    if linked_cme.empty:
                        logging.warning(f"No matching CME found for ID: {cme_id}")
                        continue
                            
                    cme_time = linked_cme['time'].iloc[0]
                    time_diff = (gst_time - cme_time).total_seconds() / 3600  # hours
                        
                    kp_index_data = gst.get('allKpIndex', [])
                    kp_index = 0
                    if kp_index_data and isinstance(kp_index_data[0], dict):
                        kp_index = kp_index_data[0].get('kpIndex', 0)
                    
                    gst_info = {
                        'time': start_time,
                        'type': 'GST',
                        'kpIndex': kp_index,
                        'gstID': gst.get('gstID', ''),
                        'linkedCME': cme_id,
                        'timeDifferenceHours': time_diff
                    }
                    processed_data.append(gst_info)
            except Exception as e:
                logging.warning(f"Error processing GST record: {str(e)}")
                continue
        
        # Create final DataFrame
        df = pd.DataFrame(processed_data)
        if not df.empty:
            df['time'] = pd.to_datetime(df['time'])
        
        logging.info(f"Processed {len(processed_data)} GST records")
        return df
            
    except Exception as e:
        logging.error(f"Error processing GST data: {str(e)}")
        raise

def merge_and_clean_data(cme_df, gst_df):
    """
    Merge and clean CME and GST data.
    """
    try:
        # Merge dataframes
        merged_df = pd.concat([cme_df, gst_df], ignore_index=True)
        
        # Sort by time
        merged_df = merged_df.sort_values('time')
        
        # Clean data
        merged_df = merged_df.dropna(subset=['time'])
        
        # Reset index
        merged_df = merged_df.reset_index(drop=True)
        
        logging.info(f"Successfully merged and cleaned data. Final dataset has {len(merged_df)} records")
        return merged_df
        
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

