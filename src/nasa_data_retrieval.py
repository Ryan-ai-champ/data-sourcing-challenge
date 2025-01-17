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

def get_cme_data(start_date=None, end_date=None):
    """
    Retrieve CME (Coronal Mass Ejection) data from NASA API.
    """
    try:
        if not start_date:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
        
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

def get_gst_data(start_date=None, end_date=None):
    """
    Retrieve GST (Geomagnetic Storm) data from NASA API.
    """
    try:
        if not start_date:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
        
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
                'longitude': cme.get('longitude', 0)
            }
            processed_data.append(cme_info)
        
        df = pd.DataFrame(processed_data)
        df['time'] = pd.to_datetime(df['time'])
        
        logging.info(f"Processed {len(processed_data)} CME records")
        return df
        
    except Exception as e:
        logging.error(f"Error processing CME data: {str(e)}")
        raise

def process_gst_data(gst_data):
    """
    Process and clean GST data.
    """
    try:
        # Extract relevant fields
        processed_data = []
        for gst in gst_data:
            gst_info = {
                'time': gst.get('startTime'),
                'type': 'GST',
                'kpIndex': gst.get('allKpIndex', [{}])[0].get('kpIndex', 0) if gst.get('allKpIndex') else 0,
                'linkedEvents': len(gst.get('linkedEvents', [])),
                'gstID': gst.get('gstID', ''),
            }
            processed_data.append(gst_info)
        
        df = pd.DataFrame(processed_data)
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
        # Set date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        # Get data
        cme_data = get_cme_data(start_date, end_date)
        gst_data = get_gst_data(start_date, end_date)
        
        # Process data
        cme_df = process_cme_data(cme_data)
        gst_df = process_gst_data(gst_data)
        
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

