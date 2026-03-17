#!/usr/bin/env python3
"""
Convert basal rate change events to timeseries with 5-minute intervals.

This script:
1. Loads tbddp/basal_rate_change_events.csv
2. Creates a timeseries grid with 5-minute intervals from first to last event
3. Converts basal rate change events to timeseries by forward-filling rates
"""

import polars as pl
from pathlib import Path
from datetime import timedelta


def load_basal_events(csv_path):
    """
    Load basal rate change events from CSV.
    
    Parameters
    ----------
    csv_path : str or Path
        Path to the CSV file containing basal rate events
        
    Returns
    -------
    pl.DataFrame
        DataFrame with columns: user_id, date (datetime), rate (float)
    """
    # Load the CSV
    df = pl.read_csv(csv_path)
    
    # Parse dates with timezone info (handle ISO8601 formats with UTC)
    df = df.with_columns([
        pl.col('date').str.strptime(pl.Datetime, format='%+').dt.replace_time_zone('UTC'),
        pl.col('rate').cast(pl.Float64, strict=False)
    ])
    
    # Sort by date to ensure proper ordering
    df = df.sort('date')
    
    return df


def create_timeseries_grid(start_time, end_time, freq='5m'):
    """
    Create a complete timeseries grid with specified frequency.
    
    Parameters
    ----------
    start_time : datetime
        Start of the timeseries
    end_time : datetime
        End of the timeseries
    freq : str, default='5m'
        Frequency string (e.g., '5m' for 5 minutes)
        
    Returns
    -------
    pl.Series
        Complete datetime series with specified frequency
    """
    # Calculate the number of 5-minute intervals
    delta = end_time - start_time
    total_seconds = delta.total_seconds()
    num_intervals = int(total_seconds / 300) + 1  # 300 seconds = 5 minutes
    
    # Create timestamps manually
    timestamps = []
    current_time = start_time
    for _ in range(num_intervals):
        timestamps.append(current_time)
        current_time = current_time + timedelta(minutes=5)
    
    return pl.Series(timestamps)


def convert_events_to_timeseries(events_df, freq='5m'):
    """
    Convert basal rate change events to complete timeseries.
    
    This function creates a complete timeseries grid and forward-fills
    basal rates from events, so each rate persists until the next change.
    
    Parameters
    ----------
    events_df : pl.DataFrame
        DataFrame with columns: user_id, date, rate
    freq : str, default='5m'
        Frequency for the timeseries grid (e.g., '5m' for 5 minutes)
        
    Returns
    -------
    pl.DataFrame
        Complete timeseries with columns: timestamp, user_id, basal_rate
    """
    # Handle empty input
    if len(events_df) == 0:
        return pl.DataFrame({
            'timestamp': pl.Series([], dtype=pl.Datetime('us', 'UTC')),
            'user_id': pl.Series([], dtype=pl.Utf8),
            'basal_rate': pl.Series([], dtype=pl.Float64)
        })
    
    # Get unique users
    users = events_df['user_id'].unique()
    
    # Process each user separately
    user_timeseries = []
    
    for user in users:
        # Filter events for this user
        user_events = events_df.filter(pl.col('user_id') == user)
        
        # Get the time range for this user
        date_col = user_events['date']
        start_time = date_col.min()
        end_time = date_col.max()
        
        # Floor start_time to 5-minute boundary
        # Use floor division to round down to nearest 5 minutes
        start_minutes = start_time.minute
        start_floored_minutes = (start_minutes // 5) * 5
        start_time = start_time.replace(minute=start_floored_minutes, second=0, microsecond=0)
        
        # Ceil end_time to 5-minute boundary
        end_minutes = end_time.minute
        end_ceiled_minutes = ((end_minutes + 4) // 5) * 5
        if end_ceiled_minutes >= 60:
            end_time = end_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else:
            end_time = end_time.replace(minute=end_ceiled_minutes, second=0, microsecond=0)
        
        # If still not past the max, add one more interval
        if end_time < date_col.max():
            end_time = end_time + timedelta(minutes=5)
        
        # Create complete timeseries grid
        timeseries_grid = create_timeseries_grid(start_time, end_time, freq=freq)
        
        # Create DataFrame with the grid
        # Create user_id column with same length as timeseries_grid
        user_id_series = [user] * len(timeseries_grid)
        ts_df = pl.DataFrame({
            'timestamp': timeseries_grid,
            'user_id': user_id_series
        })
        
        # Prepare events DataFrame - remove NaN rates
        events_for_merge = user_events.select(['date', 'rate']).drop_nulls(subset=['rate'])
        
        # Perform join_asof to find the most recent rate for each timestamp
        ts_df = ts_df.join_asof(
            events_for_merge,
            left_on='timestamp',
            right_on='date',
            strategy='backward'
        )
        
        # Rename and select columns
        ts_df = ts_df.select([
            'timestamp',
            'user_id',
            pl.col('rate').alias('basal_rate')
        ])
        
        user_timeseries.append(ts_df)
    
    # Combine all users
    if len(user_timeseries) == 0:
        return pl.DataFrame({
            'timestamp': pl.Series([], dtype=pl.Datetime('us', 'UTC')),
            'user_id': pl.Series([], dtype=pl.Utf8),
            'basal_rate': pl.Series([], dtype=pl.Float64)
        })
    
    result = pl.concat(user_timeseries)
    
    # Sort by user and timestamp
    result = result.sort(['user_id', 'timestamp'])
    
    return result


def main():
    """Main execution function."""
    # Define the CSV path (relative to this script's location)
    script_dir = Path(__file__).parent
    csv_path = script_dir / 'basal_rate_change_events.csv'
    
    print(f"Loading basal rate events from: {csv_path}")
    
    # Load the events
    events = load_basal_events(csv_path)
    print(f"Loaded {len(events)} basal rate events")
    print(f"Date range: {events['date'].min()} to {events['date'].max()}")
    print(f"Unique users: {events['user_id'].n_unique()}")
    
    # Convert to timeseries
    print("\nConverting events to 5-minute timeseries...")
    timeseries = convert_events_to_timeseries(events, freq='5m')
    
    print(f"\nCreated timeseries with {len(timeseries)} rows")
    print(f"Time points per user: {len(timeseries) / events['user_id'].n_unique():.0f}")
    
    # Display summary statistics
    print("\nBasal rate summary:")
    print(timeseries['basal_rate'].describe())
    
    # Display first few rows
    print("\nFirst few rows:")
    print(timeseries.head(10))
    
    # Display last few rows
    print("\nLast few rows:")
    print(timeseries.tail(10))
    
    # Save to CSV
    output_path = script_dir / 'basal_rate_timeseries.csv'
    timeseries.write_csv(output_path)
    print(f"\nSaved timeseries to: {output_path}")
    
    # Save to Parquet
    parquet_path = script_dir / 'basal_rate_timeseries.parquet'
    timeseries.write_parquet(parquet_path)
    print(f"Saved timeseries to: {parquet_path}")
    return timeseries


if __name__ == '__main__':
    timeseries = main()
