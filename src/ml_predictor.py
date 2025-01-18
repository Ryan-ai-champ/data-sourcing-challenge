import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
import joblib
import logging

class SpaceWeatherPredictor:
    def __init__(self, config_path):
        self.model = RandomForestRegressor()
        self.scaler = StandardScaler()
        self.logger = logging.getLogger(__name__)
        
    def prepare_features(self, df):
        """Prepare features for the model."""
        features = ['cme_speed', 'cme_width', 'cme_type', 'previous_kp_index']
        return df[features]
        
    def prepare_target(self, df):
        """Prepare target variable."""
        return df['kp_index']
        
    def train(self, X, y):
        """Train the model on the provided data."""
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        self.model.fit(X_train_scaled, y_train)
        
        y_pred = self.model.predict(X_test_scaled)
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        self.logger.info(f"Model performance - MSE: {mse:.4f}, R2: {r2:.4f}")
        
    def predict(self, features):
        """Make predictions using the trained model."""
        features_scaled = self.scaler.transform(features)
        return self.model.predict(features_scaled)
        
    def save_model(self, path):
        """Save the trained model to disk."""
        joblib.dump((self.model, self.scaler), path)
        
    def load_model(self, path):
        """Load a trained model from disk."""
        self.model, self.scaler = joblib.load(path)

