# Data Sourcing Challenge - NASA Space Weather Analysis

## Project Overview
This project analyzes the relationship between Coronal Mass Ejections (CMEs) and Geomagnetic Storms (GSTs) using NASA's DONKI API. It helps NOAA's Space Weather Prediction Center gather and analyze space weather data for better prediction and understanding of these phenomena.

## Features
- Retrieves CME and GST data from NASA's DONKI API
- Processes and merges related space weather events
- Exports cleaned data for analysis
- Handles API rate limits and errors
- Provides comprehensive logging

## Requirements
- Python 3.8+
- NASA API Key (from api.nasa.gov)
- Required Python packages (see requirements.txt)

## Installation
1. Clone the repository:
```bash
git clone https://github.com/Ryan-ai-champ/data-sourcing-challenge.git
cd data-sourcing-challenge
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # Unix/macOS
# or
.\venv\Scripts\activate  # Windows
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

4. Create a .env file with your NASA API key:
```
NASA_API_KEY=your_api_key_here
```

## Usage
Run the main script:
```bash
python src/nasa_data_retrieval.py
```

The script will:
1. Fetch recent CME and GST data from NASA
2. Process and merge the data
3. Save results to the output directory

## Project Structure
```
data-sourcing-challenge/
├── src/
│   └── nasa_data_retrieval.py
├── data/
├── output/
├── README.md
├── requirements.txt
└── .env
```

## Data Description
The merged dataset includes:
- CME characteristics (speed, angle, type)
- GST timing and intensity (Kp index)
- Related space weather events

## Acknowledgments
- NASA DONKI API
- NOAA Space Weather Prediction Center
