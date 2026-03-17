import polars as pl
from datetime import datetime, timezone as tz
# Example CSV data for reference:
# tbddp/basal_rate_change_events.csv
# ----------------------------------
# user_id,timestamp,rate
# test_user,2024-01-01T10:00:00Z,1
# test_user,2024-01-01T10:10:00Z,2
# test_user,2024-01-01T10:30:00Z,3
# test_user,2024-01-02T10:00:00Z,4
# test_user,2024-01-02T10:10:00Z,5
# test_user,2024-01-05T10:00:00Z,6
# test_user,2024-01-05T10:10:00Z,7

# Read and prepare data
df = pl.read_csv("tbddp/basal_rate_change_events.csv")

# Parse timestamp if needed
if df["timestamp"].dtype == pl.Utf8:
    df = df.with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, format='%+').dt.replace_time_zone('UTC')
    )

# Sort by timestamp
df = df.sort("timestamp")

# Create a complete date range from min to max date
date_range = pl.date_range(
    start=df["timestamp"].dt.date().min(),
    end=df["timestamp"].dt.date().max(),
    interval="1d",
    eager=True
).alias("date")

# Get the rate that should apply for each day
# Strategy: for each date, find the most recent rate change before that date
df_date_range = pl.DataFrame({"date": date_range})

# Add midnight timestamps to original events for joining
df_with_date = df.with_columns(
    pl.col("timestamp").dt.date().alias("date")
)

# For each date in the range, find the last rate that was set before or on that date
result = (
    df_date_range
    .join_asof(
        df_with_date.select(["date", "rate", "user_id"]),
        on="date",
        strategy="backward"
    )
    .with_columns(
        # Create timestamp at midnight UTC
        pl.col("date").dt.combine(
            pl.time(0, 0, 0)
        ).dt.replace_time_zone("UTC").alias("timestamp")
    )
    .select(["user_id", "timestamp", "rate"])
)

# Combine with original events and sort
final_df = (
    pl.concat([df, result])
    .unique(subset=["timestamp"], maintain_order=True)
    .sort("timestamp")
)
print(df)
print(final_df)