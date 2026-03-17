import pandas as pd
from pyspark.sql import SparkSession
import json
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("TBDDPToLoop").getOrCreate()


def safe_json_parse(value, default=None):
    """Safely parse JSON string."""
    if not isinstance(value, str):
        return default
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        logger.warning(f"Failed to parse JSON: {value}")
        return default


def safe_float_convert(value, multiplier=1.0):
    """Safely convert value to float with optional multiplier."""
    try:
        if pd.isna(value):
            return None
        return float(value) * multiplier
    except (ValueError, TypeError):
        logger.warning(f"Failed to convert to float: {value}")
        return None


def build_sql_query(user_id, start_date, end_date, metric_column, additional_columns=None, additional_conditions=None):
    """Build SQL query string."""
    select_parts = ["parsed_time", f"b.{metric_column}"]
    
    if additional_columns:
        select_parts.extend(f"b.{col}" for col in additional_columns)
    
    select_clause = ",\n    ".join(select_parts)
    
    where_conditions = [
        f"b._userId = '{user_id}'",
        f"b.{metric_column} IS NOT NULL"
    ]
    
    if additional_conditions:
        where_conditions.append(additional_conditions)
    
    where_clause = "AND ".join(f"({condition})" for condition in where_conditions)
    
    return f"""
        WITH 
            processed_data AS (
                SELECT
                    b._userId,
                    TIMESTAMPADD(MINUTE, b.timezoneOffset, TRY_CAST(b.time:`$date` AS TIMESTAMP) ) AS parsed_time,
                    {select_clause}
                FROM bddp_sample_100 AS b
            ),
            
            range_data AS (
                SELECT {select_clause}
                FROM processed_data AS b
                WHERE 
                    {where_clause}
                    AND parsed_time BETWEEN '{start_date}' AND '{end_date}'
            ),

            fallback_data AS (
                SELECT {select_clause}
                FROM processed_data as b
                WHERE 
                    {where_clause}
                    AND parsed_time < '{end_date}'
                    AND NOT EXISTS (SELECT 1 from range_data)
                ORDER BY parsed_time DESC
                LIMIT 1
            )

            SELECT * FROM range_data
            UNION ALL
            SELECT * FROM fallback_data
            ORDER BY parsed_time ASC
    """


def process_dataframe(df, to_round=False, round_minutes="5min"):
    """Process the dataframe with rounding and date formatting."""
    if to_round:
        df["parsed_time"] = df["parsed_time"].dt.round(round_minutes)
        agg_dict = {col: "first" for col in df.columns if col != "parsed_time"}
        df = df.groupby("parsed_time").agg(agg_dict).reset_index()

    df["parsed_time"] = pd.to_datetime(df["parsed_time"])
    df["date"] = df["parsed_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return df


def create_schedule_entries(schedule_data, active_schedule, start_date, end_date, entry_processor):
    """Create schedule entries for a date range."""
    parsed_schedules = safe_json_parse(schedule_data)
    if not (parsed_schedules and active_schedule in parsed_schedules):
        return []
        
    active_data = parsed_schedules[active_schedule]
    if not (isinstance(active_data, list) and len(active_data) > 0):
        return []

    start_date_dt = pd.to_datetime(start_date)
    period_start = (start_date_dt - pd.Timedelta(days=1)).floor('D')
    end_date_dt = pd.to_datetime(end_date)
    period_end = (end_date_dt + pd.Timedelta(days=1)).ceil('D')

    metrics_data = []
    sorted_data = sorted(active_data, key=lambda x: x["start"])
    
    for current_date in pd.date_range(start=period_start, end=period_end, freq='D'):
        for idx, entry in enumerate(sorted_data):
            entry_start = current_date + pd.Timedelta(milliseconds=entry["start"])
            
            if idx + 1 < len(sorted_data):
                entry_end = current_date + pd.Timedelta(milliseconds=sorted_data[idx+1]["start"])
            else:
                entry_end = current_date + pd.Timedelta(days=1)
            
            processed_entry = entry_processor(entry, entry_start, entry_end)
            if processed_entry:
                metrics_data.append(processed_entry)
    
    return metrics_data


def extract_glucose_target_metrics(user_id, start_date, end_date):
    """Extract target metrics with bgTarget conversion."""
    query = build_sql_query(
        user_id, start_date, end_date, "bgTargets",
        additional_columns=['activeSchedule'],
        additional_conditions="b.activeSchedule IS NOT NULL AND b.bgTargets IS NOT NULL"
    )
    
    df = spark.sql(query).toPandas()
    if df.empty or len(df) > 1:
        return {"target": []}
    
    df = df.iloc[0]
    
    def process_target_entry(entry, entry_start, entry_end):
        bg_target = entry["target"] * 18.018  # Convert to mg/dL
        return {
            "startDate": entry_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDate": entry_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lowerBound": float(round(bg_target - 10)),
            "upperBound": float(round(bg_target + 10)),
        }
    
    metrics_data = create_schedule_entries(
        df["bgTargets"], df["activeSchedule"], start_date, end_date, process_target_entry
    )
    
    return {"target": metrics_data}


def extract_sensitivity_metrics(user_id, start_date, end_date):
    """Extract insulin sensitivity metrics."""
    query = build_sql_query(
        user_id, start_date, end_date, "insulinSensitivities",
        additional_columns=['activeSchedule'],
        additional_conditions="b.activeSchedule IS NOT NULL AND b.insulinSensitivities IS NOT NULL"
    )
    
    df = spark.sql(query).toPandas()
    if df.empty or len(df) > 1:
        return {"sensitivity": []}
    
    df = df.iloc[0]
    
    def process_sensitivity_entry(entry, entry_start, entry_end):
        return {
            "startDate": entry_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDate": entry_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "value": entry["amount"] * 18.018,  # Convert to mg/dL
        }
    
    metrics_data = create_schedule_entries(
        df["insulinSensitivities"], df["activeSchedule"], start_date, end_date, process_sensitivity_entry
    )
    
    return {"sensitivity": metrics_data}


def extract_glucose_history(user_id, start_date, end_date):
    """Extract glucose history."""
    query = build_sql_query(
        user_id, start_date, end_date, "value",
        additional_columns=['type'],
        additional_conditions="b.type = 'cbg'"
    )
    
    df = spark.sql(query).toPandas()
    df = process_dataframe(df)
    
    metrics_data = []
    for i in range(len(df)):
        glucose_value = df["value"].iloc[i]
        converted = safe_float_convert(glucose_value, 18.018)
        
        if converted is not None:
            metrics_data.append({
                "date": df["date"].iloc[i],
                "value": converted
            })
    
    return {"glucoseHistory": metrics_data}


def extract_carb_ratio_metrics(user_id, start_date, end_date):
    """Extract carbRatio metrics."""
    query = build_sql_query(
        user_id, start_date, end_date, "carbRatios",
        additional_columns=["activeSchedule"],
        additional_conditions="b.activeSchedule IS NOT NULL AND b.carbRatios IS NOT NULL"
    )
    
    df = spark.sql(query).toPandas()
    if df.empty or len(df) > 1:
        return {"carbRatio": []}
    
    df = df.iloc[0]
    
    def process_carb_ratio_entry(entry, entry_start, entry_end):
        return {
            "startDate": entry_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDate": entry_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "value": entry["amount"],
        }
    
    metrics_data = create_schedule_entries(
        df["carbRatios"], df["activeSchedule"], start_date, end_date, process_carb_ratio_entry
    )
    
    return {"carbRatio": metrics_data}


def extract_carb_inputs_metrics(user_id, start_date, end_date):
    """Extract carb inputs metrics."""
    query = build_sql_query(
        user_id, start_date, end_date, "carbInput",
        additional_conditions="b.carbInput IS NOT NULL"
    )
    
    df = spark.sql(query).toPandas()
    df = process_dataframe(df)
    
    metrics_data = []
    for i in range(len(df)):
        carb_input = df["carbInput"].iloc[i]
        parsed = safe_json_parse(carb_input)
        
        if parsed:
            metrics_data.append({
                "date": df["date"].iloc[i],
                "grams": parsed,
                "absorptionTime": 14400
            })
    
    return {"carbEntries": metrics_data}


def extract_bolus_dose_metrics(user_id, start_date, end_date):
    """Extract bolus insulin doses."""
    query = build_sql_query(user_id, start_date, end_date, "normal")
    
    df = spark.sql(query).toPandas()
    df = process_dataframe(df)
    df['normal'] = pd.to_numeric(df['normal'], errors='coerce')
    
    metrics_data = []
    for i in range(len(df)):
        dose = df["normal"].iloc[i]
        
        if dose:
            start_time = pd.to_datetime(df["date"].iloc[i])
            end_time = start_time + timedelta(seconds=5)
            
            metrics_data.append({
                "startDate": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endDate": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "volume": dose,
                "type": "bolus"
            })
    
    return metrics_data


def extract_basal_dose_metrics(user_id, start_date, end_date):
    """Extract basal insulin rates."""
    query = build_sql_query(user_id, start_date, end_date, "rate")
    
    df = spark.sql(query).toPandas()
    df = process_dataframe(df, to_round=True)
    
    if df.empty:
        return []
    
    # Create 5-minute time grid
    start_time = df['parsed_time'].min()
    end_time = df['parsed_time'].max()
    time_grid = pd.date_range(start=start_time, end=end_time, freq='5min')
    
    # Round times and reindex
    df_processed = df.copy()
    df_processed['parsed_time'] = pd.to_datetime(df_processed['parsed_time']).dt.round('5min')
    df_processed = df_processed.drop_duplicates(['parsed_time']).set_index('parsed_time')
    df_processed['rate'] = pd.to_numeric(df_processed['rate'], errors='coerce')
    
    # Reindex and forward fill
    df_reindexed = df_processed.reindex(time_grid).fillna(method='ffill').reset_index()
    
    metrics_data = []
    for i, row in df_reindexed.iterrows():
        if pd.notna(row['rate']):
            start_time = row['index']
            end_time = start_time + timedelta(minutes=5)
            dose = row['rate'] / 12  # Convert hourly rate to 5-minute dose
            
            metrics_data.append({
                "startDate": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endDate": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "volume": dose,
                "type": "basal"
            })
    
    return metrics_data


def extract_insulin_metrics(user_id, start_date, end_date):
    """Extract combined insulin metrics (basal + bolus)."""
    bolus_metrics = extract_bolus_dose_metrics(user_id, start_date, end_date)
    basal_metrics = extract_basal_dose_metrics(user_id, start_date, end_date)
    
    # Combine and sort by start date
    combined_metrics = basal_metrics + bolus_metrics
    combined_metrics.sort(key=lambda x: x["startDate"])
    
    return {"doses": combined_metrics}


def extract_basal_rate_metrics(user_id, start_date, end_date):
    """Extract basal rate schedule metrics."""
    query = build_sql_query(
        user_id, start_date, end_date, "basalSchedules",
        additional_columns=['activeSchedule'],
        additional_conditions="b.activeSchedule IS NOT NULL AND b.basalSchedules IS NOT NULL"
    )
    
    df = spark.sql(query).toPandas()
    if df.empty or len(df) > 1:
        return {"basal": []}
    
    df = df.iloc[0]
    
    def process_basal_rate_entry(entry, entry_start, entry_end):
        return {
            "startDate": entry_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDate": entry_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "value": entry["rate"],
        }
    
    metrics_data = create_schedule_entries(
        df["basalSchedules"], df["activeSchedule"], start_date, end_date, process_basal_rate_entry
    )
    
    return {"basal": metrics_data}


def extract_data_window(user_id, start_date, prediction_date, end_date):
    """Extract data window for a specific time range."""
    try:
        # Extract schedule data
        basal_rate_result = extract_basal_rate_metrics(user_id, start_date, prediction_date)
        sensitivity_result = extract_sensitivity_metrics(user_id, start_date, prediction_date)
        carb_ratio_result = extract_carb_ratio_metrics(user_id, start_date, prediction_date)
        target_result = extract_glucose_target_metrics(user_id, start_date, prediction_date)

        # Extract event data
        glucose_history = extract_glucose_history(user_id, start_date, prediction_date)
        carb_inputs_result = extract_carb_inputs_metrics(user_id, start_date, prediction_date)
        insulin_result = extract_insulin_metrics(user_id, start_date, prediction_date)

        # Combine all results
        loop_data = {
            **basal_rate_result,
            **sensitivity_result,
            **carb_ratio_result,
            **target_result,
            **glucose_history,
            **carb_inputs_result,
            **insulin_result
        }

        config_data = {
            "predictionStart": end_date,
            "recommendationInsulinType": "novolog",
            "maxBasalRate": 35,
            "maxBolus": 30,
            "suspendThreshold": 70,
            "automaticBolusApplicationFactor": 0.4,
            "useMidAbsorptionISF": True,
            "recommendationType": "automaticBolus"
        }

        input_data = {**config_data, **loop_data}

        # Extract ground truth data
        true_glucose_data = extract_glucose_history(user_id, prediction_date, end_date)
        carb_inputs_result = extract_carb_inputs_metrics(user_id, prediction_date, end_date)
        insulin_result = extract_insulin_metrics(user_id, prediction_date, end_date)

        ground_truth_data = {
            **carb_inputs_result,
            **insulin_result,
            **true_glucose_data
        }

        return input_data, ground_truth_data

    except Exception as e:
        logger.error(f"Error in extract_data_window: {str(e)}")
        raise


def extract_pump_settings_windows(user_id):
    """Extract data windows between pump settings with 6-hour buffers."""
    # Query to get pump settings timestamps
    query = f"""
        SELECT
            TIMESTAMPADD(MINUTE, b.timezoneOffset, TRY_CAST(b.time:`$date` AS TIMESTAMP)) AS parsed_time
        FROM bddp_sample_100 as b
        WHERE 
            b.type = 'pumpSettings'
            AND b._userId = '{user_id}'
        ORDER BY parsed_time ASC
    """
    
    try:
        df = spark.sql(query).toPandas()
        
        if df.empty:
            logger.warning(f"No pump settings found for user {user_id}")
            return []
        
        # Convert to datetime
        df['parsed_time'] = pd.to_datetime(df['parsed_time'])
        pump_settings_times = df['parsed_time'].tolist()
        
        data_windows = []
        
        # Iterate through consecutive pairs of pump settings
        for i in range(len(pump_settings_times) - 1):
            current_setting = pump_settings_times[i]
            next_setting = pump_settings_times[i + 1]
            
            # Calculate start and end dates with 6-hour buffers
            start_date = current_setting + timedelta(hours=6)
            end_date = next_setting - timedelta(hours=6)
            
            # Only proceed if we have a valid window (at least 0 hours between start and end)
            if start_date < end_date:
                prediction_date = end_date  # Use end_date as prediction point
                
                start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                prediction_date_str = prediction_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                end_date_str = (end_date + timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")  # Add 6 hours for ground truth window
                
                logger.info(f"Extracting data window {i+1}: {start_date_str} to {end_date_str}")
                
                try:
                    input_data, ground_truth_data = extract_data_window(
                        user_id, start_date_str, prediction_date_str, end_date_str
                    )
                    
                    data_windows.append({
                        'window_index': i + 1,
                        'pump_setting_start': current_setting.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        'pump_setting_end': next_setting.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        'data_start': start_date_str,
                        'data_end': prediction_date_str,
                        'input_data': input_data,
                        'ground_truth_data': ground_truth_data
                    })
                    
                except Exception as e:
                    logger.error(f"Error extracting data window {i+1}: {str(e)}")
                    continue
            else:
                logger.warning(f"Skipping invalid window {i+1}: start_date {start_date} >= end_date {end_date}")
        
        logger.info(f"Successfully extracted {len(data_windows)} data windows")
        return data_windows
        
    except Exception as e:
        logger.error(f"Error in extract_pump_settings_windows: {str(e)}")
        raise


if __name__ == "__main__":
    user_id = "xxxxx"
    
    # Extract data windows between pump settings
    data_windows = extract_pump_settings_windows(user_id)
    
    # Save each window to separate files
    for window in data_windows:
        window_index = window['window_index']
        
        # Save input data
        with open(f'loop_input_data_window_{window_index}.json', 'w') as f:
            json.dump(window['input_data'], f, indent=2)
        
        # Save ground truth data
        with open(f'ground_truth_data_window_{window_index}.json', 'w') as f:
            json.dump(window['ground_truth_data'], f, indent=2)
        
        # Save window metadata
        metadata = {
            'window_index': window['window_index'],
            'pump_setting_start': window['pump_setting_start'],
            'pump_setting_end': window['pump_setting_end'],
            'data_start': window['data_start'],
            'data_end': window['data_end']
        }
        
        with open(f'window_metadata_{window_index}.json', 'w') as f:
            json.dump(metadata, f, indent=2)
    
    logger.info(f"Saved {len(data_windows)} data windows to files")
