import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Dict, Tuple
import multiprocessing as mp
import functools
import os

import time
from loop_to_python_api.api import generate_prediction
from loop_utils import load_json_data, extract_glucose_timeseries, create_loop_input_for_window

FILE_PATH = "/Users/mconn/Downloads/loop_input_data_window_30.json"

USE_RC = True  # Set to True to enable retrospective correction
USE_MID_ABSORPTION_ISF = False  # Set to True to enable changes to ISF mid-absorption of insulin
CARB_ABSORPTION_MODEL = "piecewiseLinear"  # Options: "linear" or "piecewiseLinear"
SAVE_PATH = f'/Users/mconn/data/tbddp/poc/error_data_use_rc={USE_RC}_mid_abs_isf={USE_MID_ABSORPTION_ISF}_carb_model={CARB_ABSORPTION_MODEL}.csv' 

N_WINDOWS = 1500  # Limit number of evaluation windows for testing

# Parallel processing configuration
USE_PARALLEL_PROCESSING = True  # Set to True to enable parallel processing, False for serial processing
N_PROCESSES = max(1, mp.cpu_count() - 1)  # Use all but one CPU core
BATCH_SIZE = 50  # Process windows in batches to manage memory

def process_window(window_data: Tuple[datetime, Dict, pd.DataFrame]) -> Tuple[int, List[float], List[float], bool, float]:
    """
    Worker function to process a single window in parallel.
    
    Args:
        window_data: Tuple containing (current_time, data, glucose_df)
        
    Returns:
        Tuple of (window_index, residuals, errors, success, processing_time)
    """
    start_time = time.time()
    current_time, data, glucose_df = window_data
    
    try:
        # Define window boundaries
        window_start = current_time - timedelta(hours=6)
        window_end = current_time + timedelta(hours=6)
        
        # Create loop input for this window
        loop_input = create_loop_input_for_window(data, window_start, current_time, window_end)
        
        loop_input['includePositiveVelocityAndRC'] = USE_RC 
        loop_input['useMidAbsorptionISF'] = USE_MID_ABSORPTION_ISF
        loop_input['carbAbsorptionModel'] = CARB_ABSORPTION_MODEL

        # Calculate errors
        residuals, errors, success = calculate_prediction_errors(glucose_df, loop_input, current_time)
        
        processing_time = time.time() - start_time
        
        # Return window index (timestamp as int for sorting), results, and timing
        return (int(current_time.timestamp()), residuals, errors, success, processing_time)
        
    except Exception as e:
        processing_time = time.time() - start_time
        print(f"Error processing window at {current_time}: {e}")
        return (int(current_time.timestamp()), [], [], False, processing_time)


def calculate_prediction_errors(glucose_df: pd.DataFrame, loop_input: Dict, 
                              prediction_start: datetime) -> Tuple[List[float], List[float], bool]:
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
        
        # Calculate the residual errors
        residuals = np.array(predictions) - actual_values
        
        # Calculate absolute errors
        errors = np.abs(residuals)
        
        return residuals.tolist(), errors.tolist(), True
        
    except Exception as e:
        print(f"Error in prediction: {e}")
        return [], False

def format_time(seconds: float) -> str:
    """
    Format time in seconds to human-readable format (hours, minutes, seconds).
    
    Args:
        seconds: Time in seconds
        
    Returns:
        Formatted time string
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds:.1f}s"
    else:
        hours = int(seconds // 3600)
        remaining_minutes = int((seconds % 3600) // 60)
        remaining_seconds = seconds % 60
        return f"{hours}h {remaining_minutes}m {remaining_seconds:.1f}s"


def process_windows_sequential(windows_data: List[Tuple[datetime, Dict, pd.DataFrame]], 
                              overall_start_time: float) -> Tuple[List[Dict], int, int]:
    """
    Process windows sequentially with progress reporting.
    
    Args:
        windows_data: List of window data tuples
        overall_start_time: Start time for progress calculation
        
    Returns:
        Tuple of (all_results, successful_evaluations, failed_evaluations)
    """
    all_results = []
    successful_evaluations = 0
    failed_evaluations = 0
    actual_windows = len(windows_data)
    
    for i, window_data in enumerate(windows_data):
        if i % 50 == 0:
            elapsed_time = time.time() - overall_start_time
            print(f"Processing window {i+1}/{actual_windows}")
            
            if i > 0:
                avg_time_per_window = elapsed_time / i
                remaining_windows = actual_windows - i
                estimated_remaining_time = remaining_windows * avg_time_per_window
                estimated_completion = datetime.now() + timedelta(seconds=estimated_remaining_time)
                
                print(f"  Progress: {i}/{actual_windows} ({i/actual_windows*100:.1f}%)")
                print(f"  Elapsed: {format_time(elapsed_time)}")
                print(f"  Estimated remaining: {format_time(estimated_remaining_time)}")
                print(f"  Estimated completion: {estimated_completion.strftime('%I:%M:%S %p')}")
                print(f"  Success rate: {successful_evaluations}/{i} ({successful_evaluations/i*100:.1f}%)")
        
        result = process_window(window_data)
        timestamp, residuals, errors, success, processing_time = result
        
        if success and len(residuals) > 0 and len(errors) > 0:
            all_results.append({
                'timestamp': timestamp,
                'residuals': residuals,
                'errors': errors,
                'processing_time': processing_time
            })
            successful_evaluations += 1
        else:
            failed_evaluations += 1
    
    return all_results, successful_evaluations, failed_evaluations


def process_windows_parallel(windows_data: List[Tuple[datetime, Dict, pd.DataFrame]], 
                           overall_start_time: float) -> Tuple[List[Dict], int, int]:
    """
    Process windows in parallel using multiprocessing.
    
    Args:
        windows_data: List of window data tuples
        overall_start_time: Start time for progress calculation
        
    Returns:
        Tuple of (all_results, successful_evaluations, failed_evaluations)
    """
    all_results = []
    successful_evaluations = 0
    failed_evaluations = 0
    actual_windows = len(windows_data)
    
    with mp.Pool(processes=N_PROCESSES) as pool:
        for batch_start in range(0, actual_windows, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, actual_windows)
            batch_windows = windows_data[batch_start:batch_end]
            
            batch_num = batch_start // BATCH_SIZE + 1
            total_batches = (actual_windows - 1) // BATCH_SIZE + 1
            
            print(f"Processing batch {batch_num}/{total_batches} (windows {batch_start+1}-{batch_end})")
            batch_start_time = time.time()
            
            # Process batch in parallel
            batch_results = pool.map(process_window, batch_windows)
            
            batch_time = time.time() - batch_start_time
            
            # Process results
            batch_successful = 0
            batch_failed = 0
            
            for result in batch_results:
                timestamp, residuals, errors, success, processing_time = result
                
                if success and len(residuals) > 0 and len(errors) > 0:
                    all_results.append({
                        'timestamp': timestamp,
                        'residuals': residuals,
                        'errors': errors,
                        'processing_time': processing_time
                    })
                    batch_successful += 1
                    successful_evaluations += 1
                else:
                    batch_failed += 1
                    failed_evaluations += 1
            
            # Progress reporting
            windows_processed = batch_end
            elapsed_time = time.time() - overall_start_time
            
            print(f"  Batch time: {batch_time:.1f}s")
            print(f"  Batch success: {batch_successful}/{len(batch_windows)} ({batch_successful/len(batch_windows)*100:.1f}%)")
            
            if windows_processed < actual_windows:
                avg_time_per_window = elapsed_time / windows_processed
                remaining_windows = actual_windows - windows_processed
                estimated_remaining_time = remaining_windows * avg_time_per_window
                estimated_completion = datetime.now() + timedelta(seconds=estimated_remaining_time)
                
                print(f"  Overall progress: {windows_processed}/{actual_windows} ({windows_processed/actual_windows*100:.1f}%)")
                print(f"  Elapsed: {format_time(elapsed_time)}")
                print(f"  Estimated remaining: {format_time(estimated_remaining_time)}")
                print(f"  Estimated completion: {estimated_completion.strftime('%I:%M:%S %p')}")
                print(f"  Overall success rate: {successful_evaluations}/{windows_processed} ({successful_evaluations/windows_processed*100:.1f}%)")
            print()
    
    return all_results, successful_evaluations, failed_evaluations


def run_sliding_window_evaluation(filepath: str) -> Tuple[List[List[float]], List[List[float]], pd.DataFrame]:
    """
    Run sliding window evaluation on multi-day JSON file with parallel processing.

    Returns:
        all_residuals: List of residual lists for each evaluation window
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
    
    # Calculate total expected windows
    total_expected_windows = int((evaluation_end - evaluation_start).total_seconds() / 300)  # 5-minute intervals
    total_expected_windows = min(total_expected_windows, N_WINDOWS)
    
    print(f"Expected to process ~{total_expected_windows} windows (limited to {N_WINDOWS})")
    
    if USE_PARALLEL_PROCESSING:
        print(f"Using parallel processing with {N_PROCESSES} CPU cores")
        print(f"Batch size: {BATCH_SIZE} windows per batch")
    else:
        print("Using sequential processing")
    
    # Prepare all windows for processing
    windows_data = []
    current_time = evaluation_start
    
    processing_mode = "parallel" if USE_PARALLEL_PROCESSING else "sequential"
    print(f"Preparing windows for {processing_mode} processing...")
    while current_time <= evaluation_end and len(windows_data) < N_WINDOWS:
        windows_data.append((current_time, data, glucose_df))
        current_time += timedelta(minutes=5)
    
    actual_windows = len(windows_data)
    print(f"Prepared {actual_windows} windows for processing")
    
    if actual_windows == 0:
        print("No windows to process!")
        return [], [], glucose_df
    
    overall_start_time = time.time()
    
    print(f"Starting {processing_mode} processing...\n")
    
    if USE_PARALLEL_PROCESSING:
        # Parallel processing with fallback to sequential
        try:
            all_results, successful_evaluations, failed_evaluations = process_windows_parallel(
                windows_data, overall_start_time
            )
        except Exception as e:
            print(f"Error in parallel processing: {str(e)}")
            print("Falling back to sequential processing...")
            
            # Reset start time for fallback processing
            overall_start_time = time.time()
            all_results, successful_evaluations, failed_evaluations = process_windows_sequential(
                windows_data, overall_start_time
            )
    else:
        # Sequential processing
        all_results, successful_evaluations, failed_evaluations = process_windows_sequential(
            windows_data, overall_start_time
        )
    
    total_time = time.time() - overall_start_time
    
    # Sort results by timestamp to maintain order
    all_results.sort(key=lambda x: x['timestamp'])
    
    # Extract residuals and errors in order
    all_residuals = [result['residuals'] for result in all_results]
    all_errors = [result['errors'] for result in all_results]
    
    # Final summary
    print(f"{'='*60}")
    evaluation_mode = "PARALLEL" if USE_PARALLEL_PROCESSING else "SEQUENTIAL"
    print(f"{evaluation_mode} EVALUATION COMPLETE")
    print(f"{'='*60}")
    print(f"Total processing time: {format_time(total_time)}")
    print(f"Windows processed: {actual_windows}")
    print(f"Successful evaluations: {successful_evaluations}")
    print(f"Failed evaluations: {failed_evaluations}")
    print(f"Success rate: {successful_evaluations/actual_windows*100:.1f}%")
    
    if successful_evaluations > 0:
        avg_processing_time = np.mean([r['processing_time'] for r in all_results])
        print(f"Avg processing time/window: {avg_processing_time:.2f}s")
        
        if USE_PARALLEL_PROCESSING:
            # Calculate speedup estimate for parallel processing
            sequential_estimate = actual_windows * avg_processing_time
            speedup = sequential_estimate / total_time if total_time > 0 else 1
            print(f"Estimated speedup: {speedup:.1f}x over sequential processing")
    
    print(f"{'='*60}")
    
    return all_residuals, all_errors, glucose_df

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
    
    try:
        # Run evaluation
        print("Starting sliding window evaluation...")
        all_residuals, all_errors, df = run_sliding_window_evaluation(FILE_PATH)
        
        if not all_errors:
            print("No successful evaluations completed.")
            return
        
        # Save error data if requested
        if SAVE_PATH:
            errors_df = pd.DataFrame(all_errors)
            errors_df.columns = [f'horizon_{i*5}min' for i in range(72)]
            errors_df.to_csv(SAVE_PATH, index=False)

            residuals_df = pd.DataFrame(all_residuals)
            residuals_df.columns = [f'horizon_{i*5}min' for i in range(72)]
            residuals_df.to_csv(SAVE_PATH.replace('error_data', 'residual_data'), index=False)
            print(f"Error data saved to {SAVE_PATH}")

        # Plot results
        plot_error_statistics(all_errors)
            
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    # Required for multiprocessing on Windows and some Unix systems
    try:
        mp.set_start_method('spawn', force=True)
    except RuntimeError:
        # start method already set
        pass
    main()
