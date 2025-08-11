"""
Shared utility functions for Loop data processing and prediction.
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict

def load_json_data(filepath: str) -> Dict:
    """Load JSON data from file."""
    with open(filepath, 'r') as f:
        data = json.load(f)
    return data

def extract_glucose_timeseries(data: Dict) -> pd.DataFrame:
    """Extract glucose timeseries from JSON data and convert to DataFrame."""
    # Try different possible keys for glucose data
    glucose_data = None
    if 'glucoseHistory' in data:
        glucose_data = data['glucoseHistory']
    elif 'glucose' in data:
        glucose_data = data['glucose']
    elif 'cgm' in data:
        glucose_data = data['cgm']
    else:
        raise ValueError("Could not find glucose data in JSON file")
    
    # Convert to DataFrame
    glucose_list = []
    for entry in glucose_data:
        timestamp = entry.get('date', entry.get('timestamp', entry.get('time')))
        value = entry.get('value', entry.get('glucose', entry.get('bg')))
        
        if timestamp and value is not None:
            # Parse timestamp
            if isinstance(timestamp, str):
                # Handle different timestamp formats
                if timestamp.endswith('Z'):
                    timestamp = timestamp.replace('Z', '+00:00')
                dt = datetime.fromisoformat(timestamp)
            else:
                dt = datetime.fromtimestamp(timestamp)
            
            glucose_list.append({'datetime': dt, 'glucose': value})
    
    if not glucose_list:
        raise ValueError("No valid glucose data found")
    
    df = pd.DataFrame(glucose_list)
    df = df.sort_values('datetime').set_index('datetime')
    
    # Resample to 5-minute intervals if needed
    df = df.resample('5T').mean().interpolate()
    
    return df

def create_loop_input_for_window(original_data: Dict, start_time: datetime, 
                                 prediction_time: datetime, scheduled_time: datetime = None) -> Dict:
    """Create a Loop input data structure for a specific window."""
    
    loop_input = original_data.copy()

    # Historical data
    loop_input['predictionStart'] = prediction_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    loop_input['glucoseHistory'] = [s for s in original_data['glucoseHistory'] if start_time < pd.to_datetime(s['date']) < prediction_time]
    loop_input['carbEntries'] = [s for s in original_data['carbEntries'] if start_time < pd.to_datetime(s['date']) < prediction_time]
    loop_input['doses'] = [s for s in original_data['doses'] if start_time < pd.to_datetime(s['startDate']) < prediction_time]

    if scheduled_time is None:
        scheduled_time = prediction_time + timedelta(hours=6)
    
    # Scheduled data    
    loop_input['target'] = [s for s in original_data['target'] if pd.to_datetime(s['endDate']) > start_time and pd.to_datetime(s['startDate']) < scheduled_time]
    loop_input['sensitivity'] = [s for s in original_data['sensitivity'] if pd.to_datetime(s['endDate']) > start_time and pd.to_datetime(s['startDate']) < scheduled_time]
    loop_input['carbRatio'] = [s for s in original_data['carbRatio'] if pd.to_datetime(s['endDate']) > start_time and pd.to_datetime(s['startDate']) < scheduled_time]
    loop_input['basal'] = [s for s in original_data['basal'] if pd.to_datetime(s['endDate']) > start_time and pd.to_datetime(s['startDate']) < scheduled_time]
    
    return loop_input
