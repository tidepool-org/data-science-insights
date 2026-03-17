import json
import numpy as np
import pandas as pd
from pathlib import Path
import os

def load_patient_settings(file_path):
    """Load patient settings from JSON file."""
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

def extract_schedule_settings(schedule_data):
    """Extract numeric settings from a schedule."""
    settings = []
    if isinstance(schedule_data, dict) and 'schedule' in schedule_data:
        for item in schedule_data['schedule']:
            if 'setting' in item:
                try:
                    settings.append(float(item['setting']))
                except (ValueError, TypeError):
                    continue
    return settings

def compute_statistics(values, setting_name):
    """Compute and display statistical measures for a list of values."""
    if not values:
        print(f"\n{setting_name}: No valid data found")
        return
    
    values = np.array(values)
    
    # Compute statistics
    stats = {
        'min': np.min(values),
        'max': np.max(values),
        'mean': np.mean(values),
        'median': np.median(values),
        'std': np.std(values, ddof=1),  # Sample standard deviation
        'q25': np.percentile(values, 25),
        'q75': np.percentile(values, 75),
        'iqr': np.percentile(values, 75) - np.percentile(values, 25),
        'count': len(values)
    }
    
    # Display results
    print(f"\n{setting_name} Statistics (n={stats['count']}):")
    print("=" * 50)
    print(f"Min:              {stats['min']:.4f}")
    print(f"Max:              {stats['max']:.4f}")
    print(f"Mean:             {stats['mean']:.4f}")
    print(f"Median:           {stats['median']:.4f}")
    print(f"Std Deviation:    {stats['std']:.4f}")
    print(f"25th Percentile:  {stats['q25']:.4f}")
    print(f"75th Percentile:  {stats['q75']:.4f}")
    print(f"IQR:              {stats['iqr']:.4f}")
    
    return stats

def main():
    """Main function to analyze patient settings statistics."""
    # Load patient settings data
    file_path = "/Users/mconn/data/simulator/processed/icgm/icgm_patient_settings.json"
    print(f"Loading patient settings from: {file_path}")
    
    data = load_patient_settings(file_path)
    if data is None:
        return
    
    # Initialize lists to store all settings
    insulin_sensitivity_values = []
    basal_schedule_values = []
    carb_ratio_values = []
    
    # Extract settings from all patients
    for patient_data in data:
        if 'pump' not in patient_data:
            continue
            
        pump_data = patient_data['pump']
        
        # Extract insulin sensitivity settings
        if 'insulin_sensitivity_schedule' in pump_data:
            insulin_sensitivity_values.extend(
                extract_schedule_settings(pump_data['insulin_sensitivity_schedule'])
            )
        
        # Extract basal schedule settings
        if 'basal_schedule' in pump_data:
            basal_schedule_values.extend(
                extract_schedule_settings(pump_data['basal_schedule'])
            )
        
        # Extract carb ratio settings
        if 'carb_ratio_schedule' in pump_data:
            carb_ratio_values.extend(
                extract_schedule_settings(pump_data['carb_ratio_schedule'])
            )
    
    # Compute and display statistics
    print(f"\nAnalyzing {len(data)} patient records...")
    
    compute_statistics(insulin_sensitivity_values, "Insulin Sensitivity (mg/dL per unit)")
    compute_statistics(basal_schedule_values, "Basal Rate (units/hour)")
    compute_statistics(carb_ratio_values, "Carb Ratio (grams per unit)")
    
    print("\n" + "="*60)
    print("Analysis Complete")

if __name__ == "__main__":
    main()
