import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List

class DataPipeline:
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def fetch_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch space weather data for the specified date range."""
        try:
            # Implement data fetching logic here
            self.logger.info(f"Fetching data from {start_date} to {end_date}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            raise
            
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and preprocess the raw data."""
        try:
            # Remove duplicates
            df = df.drop_duplicates()
            
            # Handle missing values
            df = df.fillna(method='ffill')
            
            # Remove outliers
            for col in self.config['numerical_columns']:
                df = self._remove_outliers(df, col)
                
            return df
        except Exception as e:
            self.logger.error(f"Error cleaning data: {str(e)}")
            raise
            
    def _remove_outliers(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Remove outliers using IQR method."""
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
        
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform data for model training."""
        try:
            # Add derived features
            df['hour'] = df.index.hour
            df['day_of_week'] = df.index.dayofweek
            
            # Calculate rolling statistics
            for col in self.config['feature_columns']:
                df[f'{col}_rolling_mean'] = df[col].rolling(
                    window=self.config['rolling_window']
                ).mean()
                
            return df
        except Exception as e:
            self.logger.error(f"Error transforming data: {str(e)}")
            raise
            
    def run_pipeline(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Execute the full data pipeline."""
        df = self.fetch_data(start_date, end_date)
        df = self.clean_data(df)
        df = self.transform_data(df)
        return df

