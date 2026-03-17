import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Dict, Tuple
import multiprocessing as mp
import functools
import os

import itertools
import time
from loop_to_python_api.api import generate_prediction
from loop_utils import load_json_data, extract_glucose_timeseries, create_loop_input_for_window

FILE_PATH = "/Users/mconn/Downloads/loop_input_data_window_30.json"

# Defaults (will be overridden in loop)
USE_RC = True  
USE_MID_ABSORPTION_ISF = False  
CARB_ABSORPTION_MODEL = "piecewiseLinear"  

N_WINDOWS = 1500 

# Parallel processing configuration
USE_PARALLEL_PROCESSING = True  
N_PROCESSES = max(1, mp.cpu_count() - 1)  
BATCH_SIZE = 50


def process_window(window_data: Tuple[datetime, Dict, pd.DataFrame, Dict]) -> Tuple[int, List[float], List[float], bool, float]:
    """
    Worker function to process a single window in parallel.
    """
    start_time = time.time()
    current_time, data, glucose_df, config = window_data
    
    try:
        window_start = current_time - timedelta(hours=6)
        window_end = current_time + timedelta(hours=6)
        
        loop_input = create_loop_input_for_window(data, window_start, current_time, window_end)
        
        # Use configuration parameters passed to worker
        loop_input['includePositiveVelocityAndRC'] = config['USE_RC']
        loop_input['useMidAbsorptionISF'] = config['USE_MID_ABSORPTION_ISF']
        loop_input['carbAbsorptionModel'] = config['CARB_ABSORPTION_MODEL']

        residuals, errors, success = calculate_prediction_errors(glucose_df, loop_input, current_time)
        
        processing_time = time.time() - start_time
        return (int(current_time.timestamp()), residuals, errors, success, processing_time)
        
    except Exception as e:
        processing_time = time.time() - start_time
        print(f"Error processing window at {current_time}: {e}")
        return (int(current_time.timestamp()), [], [], False, processing_time)


def calculate_prediction_errors(glucose_df: pd.DataFrame, loop_input: Dict, 
                              prediction_start: datetime) -> Tuple[List[float], List[float], bool]:
    try:
        predictions = generate_prediction(loop_input)

        if not predictions or len(predictions) < 72:
            return [], [], False
        
        prediction_end = prediction_start + timedelta(hours=6)
        actual_data = glucose_df[
            (glucose_df.index > prediction_start) & 
            (glucose_df.index <= prediction_end)
        ]
        
        if len(actual_data) < 72:
            return [], [], False
        
        predictions = predictions[:72]
        actual_values = actual_data['glucose'].values[:72]
        
        residuals = np.array(predictions) - actual_values
        errors = np.abs(residuals)
        
        return residuals.tolist(), errors.tolist(), True
        
    except Exception as e:
        print(f"Error in prediction: {e}")
        return [], [], False


def format_time(seconds: float) -> str:
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


def process_windows_sequential(windows_data: List[Tuple[datetime, Dict, pd.DataFrame, Dict]], 
                              overall_start_time: float) -> Tuple[List[Dict], int, int]:
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


def process_windows_parallel(windows_data: List[Tuple[datetime, Dict, pd.DataFrame, Dict]], 
                           overall_start_time: float) -> Tuple[List[Dict], int, int]:
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
            
            batch_results = pool.map(process_window, batch_windows)
            batch_time = time.time() - batch_start_time
            
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


def run_sliding_window_evaluation(filepath: str, save_path: str, config: Dict) -> Tuple[List[List[float]], List[List[float]], pd.DataFrame]:
    data = load_json_data(filepath)
    glucose_df = extract_glucose_timeseries(data)
    
    data_start = glucose_df.index[0]
    data_end = glucose_df.index[-1]
    evaluation_start = data_start + timedelta(hours=6)
    evaluation_end = data_end - timedelta(hours=6)
    
    total_expected_windows = int((evaluation_end - evaluation_start).total_seconds() / 300)  
    total_expected_windows = min(total_expected_windows, N_WINDOWS)
    
    windows_data = []
    current_time = evaluation_start
    while current_time <= evaluation_end and len(windows_data) < N_WINDOWS:
        windows_data.append((current_time, data, glucose_df, config))
        current_time += timedelta(minutes=5)
    
    if not windows_data:
        return [], [], glucose_df
    
    overall_start_time = time.time()
    
    if USE_PARALLEL_PROCESSING:
        try:
            all_results, successful_evaluations, failed_evaluations = process_windows_parallel(windows_data, overall_start_time)
        except Exception:
            all_results, successful_evaluations, failed_evaluations = process_windows_sequential(windows_data, overall_start_time)
    else:
        all_results, successful_evaluations, failed_evaluations = process_windows_sequential(windows_data, overall_start_time)
    
    all_results.sort(key=lambda x: x['timestamp'])
    all_residuals = [result['residuals'] for result in all_results]
    all_errors = [result['errors'] for result in all_results]
    
    if save_path:
        errors_df = pd.DataFrame(all_errors)
        errors_df.columns = [f'horizon_{i*5}min' for i in range(72)]
        errors_df.to_csv(save_path, index=False)

        residuals_df = pd.DataFrame(all_residuals)
        residuals_df.columns = [f'horizon_{i*5}min' for i in range(72)]
        residuals_df.to_csv(save_path.replace('error_data', 'residual_data'), index=False)
        print(f"Error data saved to {save_path}")
    
    return all_residuals, all_errors, glucose_df

def main():
    for use_rc, use_mid_absorption_isf, carb_absorption_model in itertools.product(
        [True, False],
        [True, False],
        ["linear", "piecewiseLinear"]
    ):
        config = {
            'USE_RC': use_rc,
            'USE_MID_ABSORPTION_ISF': use_mid_absorption_isf,
            'CARB_ABSORPTION_MODEL': carb_absorption_model
        }
        
        print("="*80)
        print(f"Running config: RC={use_rc}, MidISF={use_mid_absorption_isf}, CarbModel={carb_absorption_model}")
        print("="*80)

        save_path = f"/Users/mconn/data/tbddp/poc/error_data_use_rc={use_rc}_mid_abs_isf={use_mid_absorption_isf}_carb_model={carb_absorption_model}.csv"
        
        all_residuals, all_errors, df = run_sliding_window_evaluation(FILE_PATH, save_path, config)

if __name__ == "__main__":
    try:
        mp.set_start_method('spawn', force=True)
    except RuntimeError:
        pass
    main()
