#!/usr/bin/env python3
"""
Tests for convert_basal_event_to_timeseries.py

This test suite verifies that basal rate events are correctly converted
to a complete 5-minute interval timeseries with proper forward-filling.
"""

import pytest
import polars as pl
from pathlib import Path
from datetime import datetime, timezone
from convert_basal_event_to_timeseries import (
    load_basal_events,
    create_timeseries_grid,
    convert_events_to_timeseries
)


def test_load_basal_events():
    """Test that basal events are loaded correctly from CSV."""
    test_csv = Path(__file__).parent / 'test_basal_events.csv'
    
    df = load_basal_events(test_csv)
    
    # Check that we have the expected columns
    assert 'user_id' in df.columns
    assert 'date' in df.columns
    assert 'rate' in df.columns
    
    # Check that dates are parsed as datetime with timezone
    assert df['date'].dtype.base_type() == pl.Datetime
    assert df['date'].dtype.time_zone == 'UTC'
    
    # Check that rate is numeric
    assert df['rate'].dtype == pl.Float64
    
    # Check expected number of rows
    assert len(df) == 3
    
    # Check that data is sorted by date (check if ascending)
    dates = df['date'].to_list()
    assert dates == sorted(dates)


def test_create_timeseries_grid():
    """Test that timeseries grid is created with correct frequency."""
    start_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
    
    grid = create_timeseries_grid(start_time, end_time, freq='5m')
    
    # Should have 7 timestamps: 10:00, 10:05, 10:10, 10:15, 10:20, 10:25, 10:30
    assert len(grid) == 7
    
    # Check first and last timestamps
    assert grid[0] == start_time
    assert grid[-1] == end_time


def test_convert_events_to_timeseries():
    """Test that events are correctly converted to timeseries with forward-fill."""
    test_csv = Path(__file__).parent / 'test_basal_events.csv'
    expected_csv = Path(__file__).parent / 'test_expected_output.csv'
    
    # Load test events
    events = load_basal_events(test_csv)
    
    # Convert to timeseries
    result = convert_events_to_timeseries(events, freq='5m')
    
    # Load expected output (convert from pandas-format CSV to polars)
    expected = pl.read_csv(expected_csv)
    expected = expected.with_columns([
        pl.col('timestamp').str.strptime(pl.Datetime, format='%+').dt.replace_time_zone('UTC')
    ])
    
    # Check that we have the expected number of rows
    assert len(result) == len(expected)
    
    # Check that timestamps match
    assert result['timestamp'].to_list() == expected['timestamp'].to_list()
    
    # Check that user_id matches
    assert result['user_id'].to_list() == expected['user_id'].to_list()
    
    # Check that basal_rate values match (allowing for floating point precision)
    result_rates = result['basal_rate'].to_list()
    expected_rates = expected['basal_rate'].to_list()
    for r, e in zip(result_rates, expected_rates):
        assert abs(r - e) < 1e-9


def test_forward_fill_behavior():
    """Test that basal rates are correctly forward-filled between events."""
    test_csv = Path(__file__).parent / 'test_basal_events.csv'
    
    events = load_basal_events(test_csv)
    result = convert_events_to_timeseries(events, freq='5m')
    
    # First event at 10:00 with rate 0.5
    # Should persist at 10:00 and 10:05
    ts_10_00 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    ts_10_05 = datetime(2024, 1, 1, 10, 5, 0, tzinfo=timezone.utc)
    
    rate_10_00 = result.filter(pl.col('timestamp') == ts_10_00)['basal_rate'][0]
    rate_10_05 = result.filter(pl.col('timestamp') == ts_10_05)['basal_rate'][0]
    
    assert rate_10_00 == 0.5
    assert rate_10_05 == 0.5
    
    # Second event at 10:10 with rate 1.0
    # Should persist from 10:10 through 10:25
    timestamps_with_rate_1_0 = [
        datetime(2024, 1, 1, 10, 10, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 10, 15, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 10, 20, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 10, 25, 0, tzinfo=timezone.utc),
    ]
    for ts in timestamps_with_rate_1_0:
        rate = result.filter(pl.col('timestamp') == ts)['basal_rate'][0]
        assert rate == 1.0
    
    # Third event at 10:30 with rate 0.8
    ts_10_30 = datetime(2024, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
    rate_10_30 = result.filter(pl.col('timestamp') == ts_10_30)['basal_rate'][0]
    assert rate_10_30 == 0.8


def test_timestamp_alignment():
    """Test that timestamps are properly aligned to 5-minute boundaries."""
    test_csv = Path(__file__).parent / 'test_basal_events.csv'
    
    events = load_basal_events(test_csv)
    result = convert_events_to_timeseries(events, freq='5m')
    
    # Check that all timestamps have seconds and microseconds as 0
    timestamps = result['timestamp'].to_list()
    for ts in timestamps:
        assert ts.second == 0
        assert ts.microsecond == 0
        # Check that minutes are divisible by 5
        assert ts.minute % 5 == 0


def test_empty_input():
    """Test behavior with empty input."""
    # Create empty DataFrame with correct structure
    empty_events = pl.DataFrame({
        'user_id': pl.Series([], dtype=pl.Utf8),
        'date': pl.Series([], dtype=pl.Datetime('us', 'UTC')),
        'rate': pl.Series([], dtype=pl.Float64)
    })
    
    result = convert_events_to_timeseries(empty_events, freq='5m')
    
    # Should return empty DataFrame with correct columns
    assert len(result) == 0
    assert list(result.columns) == ['timestamp', 'user_id', 'basal_rate']


def test_single_event():
    """Test behavior with a single event."""
    # Create DataFrame with single event
    single_event = pl.DataFrame({
        'user_id': ['test_user'],
        'date': [datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)],
        'rate': [0.5]
    })
    
    result = convert_events_to_timeseries(single_event, freq='5m')
    
    # Should have exactly one row (start and end are same)
    assert len(result) == 1
    assert result['basal_rate'][0] == 0.5
    assert result['user_id'][0] == 'test_user'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
