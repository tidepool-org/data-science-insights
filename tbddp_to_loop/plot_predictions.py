
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import List
import re
import os

DATA_DIR = '/Users/mconn/data/tbddp/poc/'
FILE_1 = DATA_DIR + 'error_data_use_rc=True_mid_abs_isf=True_carb_model=linear.csv'
FILE_2 = DATA_DIR + 'error_data_use_rc=True_mid_abs_isf=True_carb_model=piecewiseLinear.csv'

'''
horizon_0min,horizon_5min,horizon_10min,horizon_15min,...,horizon_355min
2.000267546,10.20069773,13.45398785,13.38326964,...,42.3849428   
'''

def parse_parameters_from_filename(filepath: str) -> dict:
    """
    Parse parameters from filename like 'error_data_use_rc=True_mid_abs_isf=True_carb_model=linear.csv'
    Returns dict with parameter values.
    """
    filename = os.path.basename(filepath)
    params = {}
    
    # Extract parameters using regex
    use_rc_match = re.search(r'use_rc=([^_]+)', filename)
    if use_rc_match:
        params['use_rc'] = use_rc_match.group(1) == 'True'
    
    mid_abs_isf_match = re.search(r'mid_abs_isf=([^_]+)', filename)
    if mid_abs_isf_match:
        params['mid_abs_isf'] = mid_abs_isf_match.group(1) == 'True'
    
    carb_model_match = re.search(r'carb_model=([^.]+)', filename)
    if carb_model_match:
        params['carb_model'] = carb_model_match.group(1)
    
    return params

def generate_label_from_params(params: dict) -> str:
    """
    Generate a readable label from parameter dictionary.
    """
    parts = []
    
    if 'use_rc' in params:
        rc_label = "RC enabled" if params['use_rc'] else "RC disabled"
        parts.append(rc_label)
    
    if 'mid_abs_isf' in params:
        isf_label = "Mid-abs ISF" if params['mid_abs_isf'] else "Standard ISF"
        parts.append(isf_label)
    
    if 'carb_model' in params:
        model_label = f"Carb: {params['carb_model']}"
        parts.append(model_label)
    
    return ", ".join(parts)

def load_error_data(filepath: str) -> List[List[float]]:
    """
    Load error data from CSV file.
    Returns list of error arrays, one per row.
    Adds a zero value at the beginning and shifts everything forward by 5 minutes.
    """
    try:
        df = pd.read_csv(filepath, sep=',')
        # Convert to list of lists, each row is one evaluation window
        errors = df.values.tolist()
        # Add zero value at the beginning of each row (shift everything forward by 5 minutes)
        errors_with_zero = [[0.0] + row for row in errors]
        return errors_with_zero
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return []

def plot_comparison_errors(file1_path: str, file2_path: str, dataset1_name: str = None, dataset2_name: str = None, save_path: str = None):
    """
    Load and plot error statistics from two different files overlaid.
    
    Args:
        file1_path: Path to first CSV file
        file2_path: Path to second CSV file  
        dataset1_name: Name for first dataset in legend (if None, auto-generated from filename)
        dataset2_name: Name for second dataset in legend (if None, auto-generated from filename)
        save_path: Optional path to save plot
    """
    # Parse parameters and generate labels if not provided
    if dataset1_name is None:
        params1 = parse_parameters_from_filename(file1_path)
        dataset1_name = generate_label_from_params(params1)
    
    if dataset2_name is None:
        params2 = parse_parameters_from_filename(file2_path)
        dataset2_name = generate_label_from_params(params2)
    
    # Load data from both files
    errors1 = load_error_data(file1_path)
    errors2 = load_error_data(file2_path)
    
    if not errors1 and not errors2:
        print("No data to plot from either file")
        return
    
    # Create plot
    plt.figure(figsize=(12, 8))
    
    # Time axis (in hours)
    if errors1:
        time_hours = np.arange(len(errors1[0])) * 5 / 60  # 5-minute intervals converted to hours
    elif errors2:
        time_hours = np.arange(len(errors2[0])) * 5 / 60
    
    # Plot first dataset
    if errors1:
        errors_array1 = np.array(errors1)
        mean_errors1 = np.mean(errors_array1, axis=0)
        std_errors1 = np.std(errors_array1, axis=0)
        
        plt.fill_between(time_hours, 
                         mean_errors1 - std_errors1, 
                         mean_errors1 + std_errors1, 
                         alpha=0.3, color='blue', label=f'{dataset1_name} Mean ± 1 SD ({len(errors1)} windows)')
        
        plt.plot(time_hours, mean_errors1, 'b-', linewidth=2)
    
    # Plot second dataset
    if errors2:
        errors_array2 = np.array(errors2)
        mean_errors2 = np.mean(errors_array2, axis=0)
        std_errors2 = np.std(errors_array2, axis=0)
        
        plt.fill_between(time_hours, 
                         mean_errors2 - std_errors2, 
                         mean_errors2 + std_errors2, 
                         alpha=0.3, color='red', label=f'{dataset2_name} Mean ± 1 SD ({len(errors2)} windows)')
        
        plt.plot(time_hours, mean_errors2, 'r-', linewidth=2)
    
    plt.xlabel('Prediction Horizon (hours)')
    plt.ylabel('Absolute Error (mg/dL)')
    plt.title('Comparison of Glucose Prediction Error Statistics')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Add summary statistics as text
    text_lines = []
    if errors1:
        overall_mean1 = np.mean(errors_array1)
        overall_std1 = np.std(errors_array1)
        text_lines.append(f'{dataset1_name}: {overall_mean1:.1f} ± {overall_std1:.1f} mg/dL')
    
    if errors2:
        overall_mean2 = np.mean(errors_array2)
        overall_std2 = np.std(errors_array2)
        text_lines.append(f'{dataset2_name}: {overall_mean2:.1f} ± {overall_std2:.1f} mg/dL')
    
    if text_lines:
        plt.text(0.02, 0.98, '\n'.join(text_lines), 
                 transform=plt.gca().transAxes, verticalalignment='top',
                 bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {save_path}")
    else:
        plt.savefig('prediction_errors_comparison.png', dpi=300, bbox_inches='tight')
        print("Plot saved to 'prediction_errors_comparison.png'")
    
    plt.show()
    
    # Print summary statistics
    print(f"\nComparison Summary:")
    if errors1:
        print(f"{dataset1_name}: {len(errors1)} windows, mean error: {overall_mean1:.2f} ± {overall_std1:.2f} mg/dL")
    if errors2:
        print(f"{dataset2_name}: {len(errors2)} windows, mean error: {overall_mean2:.2f} ± {overall_std2:.2f} mg/dL")

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

if __name__ == "__main__":
    # Load and plot comparison of the two error files
    # Labels will be auto-generated from filenames
    plot_comparison_errors(FILE_1, FILE_2)
