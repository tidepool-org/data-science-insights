import json
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Dict, Tuple
import argparse
from loop_to_python_api.api import generate_prediction

FILE_PATH = "/Users/mconn/Downloads/loop_input_data_window_30.json"

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
                                 prediction_time: datetime, scheduled_time: datetime = []) -> Dict:
    """Create a Loop input data structure for a specific 6-hour window."""
    
    loop_input = original_data.copy()

    # Historical data
    loop_input['predictionStart'] = prediction_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    loop_input['glucoseHistory'] = [s for s in original_data['glucoseHistory'] if start_time < pd.to_datetime(s['date']) < prediction_time]
    loop_input['carbEntries'] = [s for s in original_data['carbEntries'] if start_time < pd.to_datetime(s['date']) < prediction_time]
    loop_input['doses'] = [s for s in original_data['doses'] if start_time < pd.to_datetime(s['startDate']) < prediction_time]

    scheduled_time = prediction_time + timedelta(hours=6) if not scheduled_time else scheduled_time
    
    # Scheduled data    
    loop_input['target'] = [s for s in original_data['target'] if start_time < pd.to_datetime(s['startDate']) < scheduled_time]
    loop_input['sensitivity'] = [s for s in original_data['sensitivity'] if start_time < pd.to_datetime(s['startDate']) < scheduled_time]
    loop_input['carbRatio'] = [s for s in original_data['carbRatio'] if start_time < pd.to_datetime(s['startDate']) < scheduled_time]
    loop_input['basal'] = [s for s in original_data['basal'] if start_time < pd.to_datetime(s['startDate']) < scheduled_time]

    return loop_input

def calculate_prediction_errors(glucose_df: pd.DataFrame, loop_input: Dict, 
                              prediction_start: datetime) -> Tuple[List[float], bool]:
    """
    Calculate prediction errors for a 6-hour window.
    
    Returns:
        errors: List of absolute errors for each 5-minute timepoint
        success: Whether we had enough actual data to calculate all errors
    """
    try:
        # Generate prediction using Loop API
        predictions = generate_prediction(loop_input)
        
        if not predictions or len(predictions) < 72:  # Need 6 hours = 72 points
            return [], False
        
        # Get actual values for the next 6 hours
        prediction_end = prediction_start + timedelta(hours=6)
        actual_data = glucose_df[
            (glucose_df.index > prediction_start) & 
            (glucose_df.index <= prediction_end)
        ]
        
        if len(actual_data) < 72:
            return [], False
        
        # Take first 72 predictions and actual values
        predictions = predictions[:72]
        actual_values = actual_data['glucose'].values[:72]
        
        # Calculate absolute errors
        errors = np.abs(np.array(predictions) - actual_values).tolist()
        
        return errors, True
        
    except Exception as e:
        print(f"Error in prediction: {e}")
        return [], False

def run_sliding_window_evaluation(filepath: str) -> Tuple[List[List[float]], pd.DataFrame]:
    """
    Run sliding window evaluation on multi-day JSON file.
    
    Returns:
        all_errors: List of error lists for each evaluation window
        df: The processed dataframe for reference
    """
    # Load and process data
    data = load_json_data(filepath)
    glucose_df = extract_glucose_timeseries(data)
    
    print(f"Loaded data with {len(glucose_df)} timepoints")
    print(f"Data spans from {glucose_df.index[0]} to {glucose_df.index[-1]}")
    
    # Calculate time boundaries
    data_start = glucose_df.index[0]
    data_end = glucose_df.index[-1]
    
    # Start at least 6 hours into the file
    evaluation_start = data_start + timedelta(hours=6)
    
    # End at least 6 hours before the end (need 6 hours of future data)
    evaluation_end = data_end - timedelta(hours=6)
    
    if evaluation_end <= evaluation_start:
        raise ValueError("Data file is too short. Need at least 12 hours of data.")
    
    print(f"Will evaluate from {evaluation_start} to {evaluation_end}")
    
    all_errors = []
    successful_evaluations = 0
    total_windows = 0
    
    # Sliding window evaluation (advance by 5 minutes)
    current_time = evaluation_start
    
    while current_time <= evaluation_end:
        total_windows += 1
        
        # Define 12-hour historical window and 6-hour prediction window
        window_start = current_time - timedelta(hours=12)
        window_end = current_time + timedelta(hours=6)
        
        # Create loop input for this window
        try:
            loop_input = create_loop_input_for_window(data, window_start, current_time, window_end)
            
            # Calculate errors
            errors, success = calculate_prediction_errors(glucose_df, loop_input, current_time)
            
            if success:
                all_errors.append(errors)
                successful_evaluations += 1
            
        except Exception as e:
            print(f"Error processing window at {current_time}: {e}")
        
        # Progress update every 12 hours
        if total_windows % 144 == 0:
            elapsed_hours = (current_time - evaluation_start).total_seconds() / 3600
            total_hours = (evaluation_end - evaluation_start).total_seconds() / 3600
            progress = elapsed_hours / total_hours * 100
            print(f"Progress: {progress:.1f}% ({successful_evaluations}/{total_windows} successful evaluations)")
        
        # Advance by 5 minutes
        current_time += timedelta(minutes=5)
    
    print(f"Completed {successful_evaluations} successful evaluations out of {total_windows} windows")
    
    return all_errors, glucose_df

def plot_error_statistics(all_errors: List[List[float]], save_path: str = None):
    """
    Plot mean and standard deviation of prediction errors over the 6-hour horizon.
    """
    if not all_errors:
        print("No errors to plot")
        return
    
    # Convert to numpy array for easier statistics
    errors_array = np.array(all_errors)  # Shape: (n_evaluations, 72_timepoints)
    
    # Calculate statistics
    mean_errors = np.mean(errors_array, axis=0)
    std_errors = np.std(errors_array, axis=0)
    
    # Time axis (in hours)
    time_hours = np.arange(72) * 5 / 60  # 5-minute intervals converted to hours
    
    # Create plot
    plt.figure(figsize=(12, 8))
    
    # Plot mean with error bars
    plt.fill_between(time_hours, 
                     mean_errors - std_errors, 
                     mean_errors + std_errors, 
                     alpha=0.3, color='blue', label='Mean ± 1 SD')
    
    plt.plot(time_hours, mean_errors, 'b-', linewidth=2, label='Mean Absolute Error')
    
    plt.xlabel('Prediction Horizon (hours)')
    plt.ylabel('Absolute Error (mg/dL)')
    plt.title(f'Loop Glucose Prediction Error Statistics\n({len(all_errors)} evaluation windows)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Add summary statistics as text
    overall_mean = np.mean(errors_array)
    overall_std = np.std(errors_array)
    plt.text(0.02, 0.98, f'Overall Mean Error: {overall_mean:.1f} ± {overall_std:.1f} mg/dL', 
             transform=plt.gca().transAxes, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {save_path}")
    else:
        plt.savefig('loop_prediction_errors.png', dpi=300, bbox_inches='tight')
        print("Plot saved to 'loop_prediction_errors.png'")
    
    plt.show()
    
    # Print summary statistics
    print(f"\nSummary Statistics:")
    print(f"Number of evaluation windows: {len(all_errors)}")
    print(f"Overall mean error: {overall_mean:.2f} mg/dL")
    print(f"Overall std error: {overall_std:.2f} mg/dL")
    
    # Error statistics by prediction horizon
    print(f"\nError by prediction horizon:")
    for i, hour in enumerate([1, 2, 3, 4, 5, 6]):
        idx = int(hour * 12) - 1  # Convert hours to 5-min intervals
        if idx < len(mean_errors):
            print(f"  {hour}h: {mean_errors[idx]:.2f} ± {std_errors[idx]:.2f} mg/dL")

def main():
    # parser = argparse.ArgumentParser(description='Evaluate Loop glucose prediction model on multi-day data')
    # parser.add_argument('input_file', help='Path to JSON file with multi-day glucose data')
    # parser.add_argument('--output_plot', help='Path to save error plot (optional)')
    # parser.add_argument('--output_data', help='Path to save error data as CSV (optional)')
    
    # args = parser.parse_args()
    
    try:
        # Run evaluation
        print("Starting sliding window evaluation...")
        all_errors, df = run_sliding_window_evaluation(FILE_PATH)
        
        if not all_errors:
            print("No successful evaluations completed.")
            return
        
        # Plot results
        plot_error_statistics(all_errors)
        
        # Save error data if requested
        # if args.output_data:
        #     errors_df = pd.DataFrame(all_errors)
        #     errors_df.columns = [f'horizon_{i*5}min' for i in range(72)]
        #     errors_df.to_csv(args.output_data, index=False)
        #     print(f"Error data saved to {args.output_data}")
            
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
