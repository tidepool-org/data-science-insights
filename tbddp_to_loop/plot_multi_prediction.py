import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Dict
import pickle
import os
from loop_to_python_api.api import generate_prediction
from loop_utils import load_json_data, extract_glucose_timeseries, create_loop_input_for_window

FILE_PATH = "/Users/mconn/Downloads/loop_input_data_window_30.json"
USE_RC = True  # Set to True to enable retrospective correction
REGENERATE_PREDICTIONS = True  # Set to True to regenerate predictions, False to use saved file
PREDICTIONS_SAVE_PATH = "multi_predictions_data.pkl"  # Path to save/load prediction data

def generate_single_prediction(data: Dict, glucose_df: pd.DataFrame, prediction_start: datetime) -> List[float]:
    """Generate a single 6-hour prediction from the given start time."""
    try:
        # Create Loop input for this window
        window_start = prediction_start - timedelta(hours=6)
        window_end = prediction_start + timedelta(hours=6)
        
        loop_input = create_loop_input_for_window(data, window_start, prediction_start, window_end)
        loop_input['includePositiveVelocityAndRC'] = USE_RC
        
        # Generate prediction using Loop API
        predictions = generate_prediction(loop_input)
        
        if not predictions or len(predictions) < 72:  # Need 6 hours = 72 points
            return []
        
        return predictions[:72]  # Return first 6 hours
        
    except Exception as e:
        print(f"Error generating prediction at {prediction_start}: {e}")
        return []

def generate_and_save_predictions(filepath: str, save_path: str = PREDICTIONS_SAVE_PATH):
    """
    Generate all predictions and save them to a file for later use.
    """
    # Load and process data
    data = load_json_data(filepath)
    glucose_df = extract_glucose_timeseries(data)
    
    print(f"Loaded data with {len(glucose_df)} timepoints")
    print(f"Data spans from {glucose_df.index[0]} to {glucose_df.index[-1]}")
    
    # Find a suitable 13-hour window with at least 6 hours of historical data before it
    data_start = glucose_df.index[0]
    data_end = glucose_df.index[-1]
    
    # Start at least 6 hours into the file (need history for predictions)
    # and ensure we have 13 hours of data available
    earliest_start = data_start + timedelta(hours=6)
    latest_start = data_end - timedelta(hours=13)
    
    if latest_start <= earliest_start:
        raise ValueError("Data file is too short. Need at least 19 hours of data.")
    
    # Select a window (using the earliest suitable start for simplicity)
    window_start = earliest_start
    window_end = window_start + timedelta(hours=13)
    
    print(f"Prediction window from {window_start} to {window_end}")
    
    # Extract the 13-hour glucose window
    glucose_window = glucose_df[
        (glucose_df.index >= window_start) & 
        (glucose_df.index <= window_end)
    ].copy()
    
    if len(glucose_window) < 156:  # 13 hours * 12 points/hour
        print(f"Warning: Only have {len(glucose_window)} points for 13-hour window")
    
    # Create time axis in hours relative to start
    glucose_window['hours_from_start'] = (glucose_window.index - window_start).total_seconds() / 3600
    
    # Generate predictions every 5 minutes for 1 hour starting at the 6-hour mark
    prediction_start_time = window_start + timedelta(hours=6)
    
    successful_predictions = 0
    all_predictions = []
    
    for i in range(13):  # 0, 5, 10, ..., 60 minutes = 13 predictions
        current_prediction_start = prediction_start_time + timedelta(minutes=i*5)
        
        # Don't generate predictions beyond our data window
        if current_prediction_start + timedelta(hours=6) > glucose_df.index[-1]:
            print(f"Skipping prediction {i} - not enough data")
            continue
            
        print(f"Generating prediction {i+1}/13 starting at {current_prediction_start}")
        
        # Generate prediction
        predictions = generate_single_prediction(data, glucose_df, current_prediction_start)
        
        if predictions:
            # Create time axis for predictions (6 hours from prediction start)
            pred_hours = np.arange(len(predictions)) * 5/60  # 5-minute intervals in hours
            pred_time_axis = 6 + (i * 5/60) + pred_hours  # Offset by 6 hours + prediction delay
            
            # Only save predictions that fall within our 13-hour window
            valid_indices = pred_time_axis <= 13
            if np.any(valid_indices):
                prediction_entry = {
                    'prediction_index': i,
                    'prediction_start': current_prediction_start,
                    'time_axis': pred_time_axis[valid_indices],
                    'predictions': np.array(predictions)[valid_indices],
                    'delay_minutes': i * 5
                }
                all_predictions.append(prediction_entry)
                successful_predictions += 1
    
    # Prepare data structure to save
    save_data = {
        'glucose_window': glucose_window,
        'window_start': window_start,
        'window_end': window_end,
        'predictions': all_predictions,
        'successful_predictions': successful_predictions,
        'use_rc': USE_RC,
        'file_path': filepath,
        'generation_timestamp': datetime.now()
    }
    
    # Save to file
    with open(save_path, 'wb') as f:
        pickle.dump(save_data, f)
    
    print(f"\nPredictions saved to {save_path}")
    print(f"Successfully generated {successful_predictions} predictions")
    print(f"Real glucose data: {len(glucose_window)} points over 13 hours")
    
    return save_data

def load_saved_predictions(save_path: str = PREDICTIONS_SAVE_PATH):
    """
    Load previously saved predictions from file.
    """
    if not os.path.exists(save_path):
        raise FileNotFoundError(f"Predictions file {save_path} not found. Set REGENERATE_PREDICTIONS=True to generate new predictions.")
    
    with open(save_path, 'rb') as f:
        save_data = pickle.load(f)
    
    print(f"Loaded predictions from {save_path}")
    print(f"Original file: {save_data['file_path']}")
    print(f"Generated on: {save_data['generation_timestamp']}")
    print(f"USE_RC: {save_data['use_rc']}")
    print(f"Number of predictions: {save_data['successful_predictions']}")
    
    return save_data

def _plot_error_subplot(ax, prediction_data_for_error, glucose_window, error_type='regular'):
    """
    Helper function to plot error data on a subplot to avoid code duplication.
    
    Args:
        ax: Matplotlib axis to plot on
        prediction_data_for_error: List of prediction data dictionaries
        glucose_window: DataFrame with glucose data and hours_from_start
        error_type: 'regular' for signed error, 'absolute' for absolute error
    """
    for pred_data in prediction_data_for_error:
        # Interpolate actual glucose values for prediction time points
        actual_glucose_interp = np.interp(pred_data['time_axis'], 
                                         glucose_window['hours_from_start'], 
                                         glucose_window['glucose'])
        
        # Calculate error based on type
        if error_type == 'regular':
            error = pred_data['predictions'] - actual_glucose_interp
        elif error_type == 'absolute':
            error = np.abs(pred_data['predictions'] - actual_glucose_interp)
        else:
            raise ValueError("error_type must be 'regular' or 'absolute'")
        
        # Plot error
        ax.plot(pred_data['time_axis'], error,
                linestyle=pred_data['linestyle'],
                color=pred_data['color'],
                linewidth=pred_data['linewidth'],
                alpha=pred_data['alpha'])

def plot_multi_prediction_timeline(prediction_data: Dict = None, plot_save_path: str = None):
    """
    Create a plot showing 13 hours of real glucose data with multiple overlaid predictions.
    
    Args:
        prediction_data: Dictionary containing glucose data and predictions (from generate_and_save_predictions or load_saved_predictions)
        plot_save_path: Path to save the plot image
    """
    if prediction_data is None:
        raise ValueError("prediction_data is required")
    
    glucose_window = prediction_data['glucose_window']
    all_predictions = prediction_data['predictions']
    successful_predictions = prediction_data['successful_predictions']
    window_start = prediction_data['window_start']
    window_end = prediction_data['window_end']
    use_rc = prediction_data['use_rc']
    
    print(f"Plotting window from {window_start} to {window_end}")
    
    # Create the plot with 3 subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 10), height_ratios=[3, 1, 1])
    
    # Main plot - glucose values
    # Plot real glucose data - first 6 hours in blue
    first_6_hours = glucose_window[glucose_window['hours_from_start'] <= 6]
    ax1.plot(first_6_hours['hours_from_start'], first_6_hours['glucose'], 
            color='#271b45', linewidth=3, label='Historical Glucose (0-6 hours)', alpha=0.8)
    
    # Plot real glucose data - next 7 hours in green  
    next_7_hours = glucose_window[glucose_window['hours_from_start'] > 6]
    ax1.plot(next_7_hours['hours_from_start'], next_7_hours['glucose'],
            color='#607cff', linewidth=3, label='Actual Glucose (6-13 hours)', alpha=0.8)
    
    # Plot predictions
    prediction_colors = plt.cm.cividis(np.linspace(0.3, 0.9, 13))
    prediction_data_for_error = []  # Store prediction data for error calculation
    
    for pred_entry in all_predictions:
        i = pred_entry['prediction_index']
        alpha_val = 1.0
        linestyle = '--' if i == 0 else ':'  # First prediction dashed, others dotted
        linewidth = 2 if i == 0 else 2
        
        # Plot on main subplot
        ax1.plot(pred_entry['time_axis'], 
                pred_entry['predictions'],
                linestyle=linestyle, 
                color=prediction_colors[i],
                linewidth=linewidth,
                alpha=alpha_val,
                label=f'Prediction {i+1} (+{i*5}min)' if i < 5 else None)  # Only label first few
        
        # Store prediction data for error calculation
        prediction_data_for_error.append({
            'prediction_index': i,
            'time_axis': pred_entry['time_axis'],
            'predictions': pred_entry['predictions'],
            'color': prediction_colors[i],
            'alpha': alpha_val,
            'linestyle': linestyle,
            'linewidth': linewidth
        })
    
    # Plot regular error (prediction - actual) in second subplot
    _plot_error_subplot(ax2, prediction_data_for_error, glucose_window, error_type='regular')
    
    # Plot absolute error in third subplot
    _plot_error_subplot(ax3, prediction_data_for_error, glucose_window, error_type='absolute')
    
    # Formatting for main plot
    ax1.set_ylabel('Glucose (mg/dL)', fontsize=12)
    ax1.set_title(f'{successful_predictions} Loop Predictions Every 5min Starting at 6h Mark\n'
                 f'USE_RC={use_rc}', fontsize=14)
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc='upper left')
    ax1.set_xlim(3, 13)
    ax1.axvline(x=6, color='red', linestyle='--', alpha=0.5, label='Prediction Start')
    ax1.set_xticklabels([])  # Hide x-ticks for main plot
    
    # Formatting for regular error plot
    ax2.set_ylabel('Error (mg/dL)', fontsize=12)
    ax2.set_title('Prediction Error vs Actual Glucose (Signed)', fontsize=12)
    ax2.grid(True, alpha=0.3)
    ax2.set_xlim(3, 13)
    ax2.axhline(y=0, color='black', linestyle='-', alpha=0.5)  # Reference line at 0 error
    ax2.axvline(x=6, color='red', linestyle='--', alpha=0.5)
    ax2.set_xticklabels([])  # Hide x-ticks for main plot

    # Formatting for absolute error plot
    ax3.set_xlabel('Hours from Start', fontsize=12)
    ax3.set_ylabel('|Error| (mg/dL)', fontsize=12)
    ax3.set_title('Prediction Absolute Error vs Actual Glucose', fontsize=12)
    ax3.grid(True, alpha=0.3)
    ax3.set_xlim(3, 13)
    ax3.axhline(y=0, color='black', linestyle='-', alpha=0.5)  # Reference line at 0 error
    ax3.axvline(x=6, color='red', linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    
    # Save plot
    if plot_save_path:
        fig.savefig(plot_save_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {plot_save_path}")
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_save_path = f'multi_prediction_timeline_RC={use_rc}_{timestamp}.png'
        fig.savefig(default_save_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {default_save_path}")
    
    plt.show()
    
    print(f"\nSummary:")
    print(f"Plotted {successful_predictions} predictions")
    print(f"Real glucose data: {len(glucose_window)} points over 13 hours")
    print(f"Window: {window_start} to {window_end}")

def main():
    """Main function to run the multi-prediction timeline plot."""
    try:
        if REGENERATE_PREDICTIONS:
            print("Generating new predictions...")
            prediction_data = generate_and_save_predictions(FILE_PATH)
        else:
            print("Loading saved predictions...")
            prediction_data = load_saved_predictions()
        
        print("Creating multi-prediction timeline plot...")
        plot_multi_prediction_timeline(prediction_data)
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
