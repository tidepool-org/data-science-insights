"""
Transition Detection Pipeline for Autobolus Analysis

Identifies users who transition between temp basal and autobolus dosing strategies
by analyzing sliding windows of Loop recommendation data.

For FDA 510(k) regulatory submission - all parameters documented.

Author: Mark
Last Modified: 2025-01-06
"""

from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict
from enum import Enum

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass(frozen=True)
class PipelineConfig:
    """
    All tunable parameters for the transition detection pipeline.
    
    Frozen dataclass ensures immutability during analysis runs.
    Document any changes for regulatory traceability.
    """
    # Segment definitions
    seg1_days: int = 14                    # "Before" segment duration
    seg2_days: int = 14                    # "After" segment duration  
    gap_days: int = 0                      # Gap between segments (0 = contiguous)
    
    # Data quality thresholds
    min_coverage: float = 0.70             # Minimum data completeness (0-1)
    samples_per_day: int = 288             # Expected samples/day (5-min = 288)
    
    # Transition classification thresholds
    autobolus_low: float = 0.30            # Below this = "temp basal mode"
    autobolus_high: float = 0.70           # Above this = "autobolus mode"
    
    # Source table
    source_table: str = "bddp_sample_all"
    reason_filter: str = "loop"
    
    @property
    def samples_per_segment(self) -> int:
        """Expected samples in a full segment (for coverage denominator)."""
        return self.samples_per_day * self.seg1_days
    
    @property
    def total_window_days(self) -> int:
        """Total calendar days spanned by both segments."""
        return self.seg1_days + self.seg2_days + self.gap_days


class TransitionType(Enum):
    """Direction of dosing strategy transition."""
    TEMP_BASAL_TO_AUTOBOLUS = "tb_to_ab"
    AUTOBOLUS_TO_TEMP_BASAL = "ab_to_tb"
    NONE = "none"


# =============================================================================
# DATA LOADING & CLEANING
# =============================================================================

def load_base_recommendations(
    spark: SparkSession,
    config: PipelineConfig,
    run_diagnostics: bool = True
) -> Tuple[DataFrame, dict]:
    """
    Load and clean raw Loop recommendation data.
    
    Applies initial filters and timestamp parsing.
    Excludes rows with unparseable timestamps.
    
    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    config : PipelineConfig
        Pipeline configuration
    run_diagnostics : bool, default True
        If True, run counts for data quality reporting.
        Set to False in production for better performance.
        
    Returns
    -------
    Tuple[DataFrame, dict]
        - DataFrame with columns: _userId, time (timestamp), recommendedBasal, recommendedBolus
        - Diagnostics dict with row counts and data quality metrics
        
    Raises
    ------
    ValueError
        If source table doesn't exist or returns zero rows
    """
    diagnostics = {
        "source_table": config.source_table,
        "reason_filter": config.reason_filter,
        "total_rows_raw": None,
        "rows_null_timestamp": None,
        "rows_after_cleaning": None,
        "null_timestamp_pct": None,
        "distinct_users": None,
    }
    
    # Verify table exists
    if not spark.catalog.tableExists(config.source_table):
        raise ValueError(f"Source table '{config.source_table}' does not exist")
    
    if run_diagnostics:
        # Single-pass aggregation to get all diagnostics at once
        # Avoids multiple scans and cache() which isn't supported on serverless
        diag_query = f"""
            SELECT 
                COUNT(*) AS total_rows,
                SUM(CASE WHEN TRY_CAST(time:`$date` AS TIMESTAMP) IS NULL THEN 1 ELSE 0 END) AS null_timestamp_rows,
                SUM(CASE WHEN TRY_CAST(time:`$date` AS TIMESTAMP) IS NOT NULL THEN 1 ELSE 0 END) AS valid_rows,
                COUNT(DISTINCT CASE WHEN TRY_CAST(time:`$date` AS TIMESTAMP) IS NOT NULL THEN _userId END) AS distinct_users
            FROM {config.source_table}
            WHERE LOWER(TRIM(reason)) = '{config.reason_filter.lower()}'
        """
        
        diag_row = spark.sql(diag_query).collect()[0]
        
        total_rows = diag_row["total_rows"]
        null_timestamp_rows = diag_row["null_timestamp_rows"]
        clean_rows = diag_row["valid_rows"]
        distinct_users = diag_row["distinct_users"]
        
        diagnostics["total_rows_raw"] = total_rows
        diagnostics["rows_null_timestamp"] = null_timestamp_rows
        diagnostics["rows_after_cleaning"] = clean_rows
        diagnostics["null_timestamp_pct"] = (null_timestamp_rows / total_rows * 100) if total_rows > 0 else 0
        diagnostics["distinct_users"] = distinct_users
        
        if total_rows == 0:
            raise ValueError(
                f"No rows found in '{config.source_table}' "
                f"with reason='{config.reason_filter}'"
            )
        
        if clean_rows == 0:
            raise ValueError(
                f"All {total_rows:,} rows had unparseable timestamps. "
                f"Check time:`$date` format in source table."
            )
        
        # Log warnings
        if null_timestamp_rows > 0:
            print(
                f"WARNING: {null_timestamp_rows:,} rows "
                f"({diagnostics['null_timestamp_pct']:.2f}%) "
                f"excluded due to unparseable timestamps"
            )
            
            if diagnostics["null_timestamp_pct"] > 5.0:
                print(
                    f"  ⚠️  High timestamp parse failure rate. "
                    f"Investigate source data quality."
                )
        
        print(f"Loaded {clean_rows:,} rows from {distinct_users:,} users")
    
    # Now get the actual data (separate query to return DataFrame)
    # This will be a second scan, but unavoidable without cache()
    # Parse JSON strings to extract the values we need
    # recommendedBasal is JSON like: {"rate": 0.5, "duration": 1800000}
    # recommendedBolus is JSON like: {"amount": 0.15}
    data_query = f"""
        SELECT 
            _userId,
            TRY_CAST(time:`$date` AS TIMESTAMP) AS time,
            CAST(GET_JSON_OBJECT(recommendedBasal, '$.rate') AS DOUBLE) AS basal_rate,
            CAST(GET_JSON_OBJECT(recommendedBasal, '$.duration') AS BIGINT) AS basal_duration,
            CAST(GET_JSON_OBJECT(recommendedBolus, '$.amount') AS DOUBLE) AS bolus_amount,
            -- Keep flags for whether the original fields were present (not null strings)
            recommendedBasal IS NOT NULL AND recommendedBasal != 'null' AS has_basal,
            recommendedBolus IS NOT NULL AND recommendedBolus != 'null' AS has_bolus
        FROM {config.source_table}
        WHERE LOWER(TRIM(reason)) = '{config.reason_filter.lower()}'
          AND TRY_CAST(time:`$date` AS TIMESTAMP) IS NOT NULL
    """
    
    df_clean = spark.sql(data_query)
    
    if not run_diagnostics:
        # Fast path: just validate we have SOME data
        if df_clean.take(1) == []:
            raise ValueError(
                f"No valid rows found in '{config.source_table}' "
                f"with reason='{config.reason_filter}' and parseable timestamps"
            )
    
    return df_clean, diagnostics


def check_for_duplicate_timestamps(
    df: DataFrame,
    sample_fraction: float = 0.01
) -> dict:
    """
    Check for duplicate timestamps per user (data quality diagnostic).
    
    Duplicate timestamps can cause overcounting in aggregations.
    Runs on a sample for performance.
    
    Parameters
    ----------
    df : DataFrame
        Must contain _userId and time columns
    sample_fraction : float
        Fraction of data to sample (0.01 = 1%)
        
    Returns
    -------
    dict
        duplicate_pairs_found: number of duplicate (user, time) pairs in sample
        example_duplicates: sample of duplicate (user, time) pairs
    """
    # Sample and find duplicates in one pass
    duplicates = (
        df
        .sample(fraction=min(1.0, sample_fraction))
        .groupBy("_userId", "time")
        .count()
        .filter(F.col("count") > 1)
        .limit(100)  # Cap results to avoid memory issues
    )
    
    # Collect to driver (already limited)
    dup_rows = duplicates.collect()
    dup_count = len(dup_rows)
    
    result = {
        "sample_fraction": sample_fraction,
        "duplicate_pairs_found": dup_count,
        "example_duplicates": []
    }
    
    if dup_count > 0:
        result["example_duplicates"] = [
            {"_userId": row["_userId"], "time": str(row["time"]), "count": row["count"]}
            for row in dup_rows[:5]
        ]
        print(
            f"⚠️  Found {dup_count}+ duplicate (user, timestamp) pairs "
            f"in {sample_fraction:.1%} sample"
        )
    
    return result


def deduplicate_recommendations(
    df: DataFrame,
    run_diagnostics: bool = True
) -> Tuple[DataFrame, dict]:
    """
    Remove duplicate (user, timestamp) entries from recommendations.
    
    Deduplication Strategy
    ----------------------
    When multiple rows exist for the same (user, timestamp), we keep the row
    with the most information (prioritizing rows with bolus > 0, then basal present).
    
    This handles cases where:
    - Exact duplicate rows were logged
    - Multiple recommendations were generated at the same timestamp
    
    Parameters
    ----------
    df : DataFrame
        Must contain: _userId, time, bolus_amount, has_basal, has_bolus
    run_diagnostics : bool
        If True, compute before/after counts
        
    Returns
    -------
    Tuple[DataFrame, dict]
        - Deduplicated DataFrame
        - Diagnostics dict with deduplication statistics
    """
    diagnostics = {
        "rows_before": None,
        "rows_after": None,
        "duplicates_removed": None,
        "duplicate_pct": None,
    }
    
    if run_diagnostics:
        rows_before = df.count()
        diagnostics["rows_before"] = rows_before
    
    # Create a priority score for each row:
    # - Higher priority for rows with actual bolus amount > 0
    # - Then rows with basal recommendation present
    # - Then any other row
    # This ensures we keep the most informative row when deduplicating
    df_with_priority = df.withColumn(
        "_dedup_priority",
        F.when(F.col("bolus_amount") > 0, 3)
        .when(F.col("has_basal"), 2)
        .when(F.col("has_bolus"), 1)
        .otherwise(0)
    )
    
    # Rank rows within each (user, timestamp) group by priority (descending)
    window_spec = (
        Window
        .partitionBy("_userId", "time")
        .orderBy(F.desc("_dedup_priority"), F.desc("bolus_amount"))
    )
    
    df_ranked = df_with_priority.withColumn(
        "_dedup_rank",
        F.row_number().over(window_spec)
    )
    
    # Keep only the highest priority row for each (user, timestamp)
    df_deduped = (
        df_ranked
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_priority", "_dedup_rank")
    )
    
    if run_diagnostics:
        rows_after = df_deduped.count()
        duplicates_removed = rows_before - rows_after
        
        diagnostics["rows_after"] = rows_after
        diagnostics["duplicates_removed"] = duplicates_removed
        diagnostics["duplicate_pct"] = (duplicates_removed / rows_before * 100) if rows_before > 0 else 0
        
        if duplicates_removed > 0:
            print(
                f"Deduplication: removed {duplicates_removed:,} duplicate rows "
                f"({diagnostics['duplicate_pct']:.2f}%)"
            )
            print(f"  Rows before: {rows_before:,}")
            print(f"  Rows after:  {rows_after:,}")
        else:
            print("Deduplication: no duplicates found")
    
    return df_deduped, diagnostics


def classify_recommendation_type(df: DataFrame) -> Tuple[DataFrame, dict]:
    """
    Classify each recommendation as autobolus (1), temp basal (0), or null.
    
    Classification Logic
    --------------------
    Input columns (from load_base_recommendations):
    - bolus_amount: extracted from recommendedBolus.amount (DOUBLE, nullable)
    - has_basal: whether recommendedBasal was present (BOOLEAN)
    - has_bolus: whether recommendedBolus was present (BOOLEAN)
    
    Classification:
    - bolus_amount > 0 → 1 (autobolus)
    - bolus_amount is null/0 AND has_basal → 0 (temp basal)
    - Neither → NULL (no recommendation)
    
    Edge cases:
    - {"amount": 0.0} means "explicitly don't bolus" → temp basal if basal present
    - A row with basal present and bolus=0 is TEMP BASAL, not autobolus
    
    Parameters
    ----------
    df : DataFrame
        Must contain columns: bolus_amount, has_basal, has_bolus
        
    Returns
    -------
    Tuple[DataFrame, dict]
        - Original DataFrame plus: is_autobolus (int, nullable)
        - Diagnostics dict with classification counts
    """
    # Classify based on actual bolus amount, not just presence
    df_classified = df.withColumn(
        "is_autobolus",
        F.when(
            # Autobolus: bolus amount > 0 (regardless of basal)
            F.col("bolus_amount") > 0,
            F.lit(1)
        )
        .when(
            # Temp basal: no meaningful bolus, but basal recommendation exists
            # This catches: bolus is null, OR bolus.amount == 0
            F.col("has_basal"),
            F.lit(0)
        )
        .otherwise(
            # Neither: no recommendation
            F.lit(None).cast("int")
        )
    )
    
    # Compute classification breakdown in single pass
    # Include extra diagnostics for the edge cases
    diagnostics = (
        df_classified
        .groupBy()
        .agg(
            F.count("*").alias("total_rows"),
            F.sum(F.when(F.col("is_autobolus") == 1, 1).otherwise(0)).alias("autobolus_rows"),
            F.sum(F.when(F.col("is_autobolus") == 0, 1).otherwise(0)).alias("temp_basal_rows"),
            F.sum(F.when(F.col("is_autobolus").isNull(), 1).otherwise(0)).alias("null_rows"),
            
            # Detailed edge case tracking
            F.sum(
                F.when(
                    (F.col("bolus_amount") == 0) & F.col("has_basal"),
                    1
                ).otherwise(0)
            ).alias("zero_bolus_with_basal_rows"),
            
            F.sum(
                F.when(
                    (F.col("bolus_amount") > 0) & F.col("has_basal"),
                    1
                ).otherwise(0)
            ).alias("bolus_and_basal_rows"),
            
            F.sum(
                F.when(
                    F.col("has_bolus") & ((F.col("bolus_amount") == 0) | F.col("bolus_amount").isNull()),
                    1
                ).otherwise(0)
            ).alias("explicit_zero_bolus_rows"),
            
            # Track null/null case (neither basal nor bolus present)
            F.sum(
                F.when(
                    ~F.col("has_basal") & ~F.col("has_bolus"),
                    1
                ).otherwise(0)
            ).alias("both_null_rows"),
        )
        .collect()[0]
        .asDict()
    )
    
    # Log summary
    total = diagnostics.get("total_rows") or 0
    if total > 0:
        autobolus = diagnostics.get('autobolus_rows') or 0
        temp_basal = diagnostics.get('temp_basal_rows') or 0
        null_rows = diagnostics.get('null_rows') or 0
        
        print(f"Classification breakdown ({total:,} rows):")
        print(f"  Autobolus (bolus_amount > 0):  {autobolus:,} ({autobolus/total*100:.1f}%)")
        print(f"  Temp basal (basal only/zero):  {temp_basal:,} ({temp_basal/total*100:.1f}%)")
        print(f"  Null (no recommendation):      {null_rows:,} ({null_rows/total*100:.1f}%)")
        
        print(f"\nEdge case breakdown:")
        print(f"  Both null (no basal, no bolus): {diagnostics.get('both_null_rows') or 0:,}")
        print(f"  Explicit zero bolus (amount=0): {diagnostics.get('explicit_zero_bolus_rows') or 0:,}")
        print(f"  Zero bolus + basal present:     {diagnostics.get('zero_bolus_with_basal_rows') or 0:,} (→ temp basal)")
        print(f"  Positive bolus + basal present: {diagnostics.get('bolus_and_basal_rows') or 0:,} (→ autobolus)")
    else:
        print("Classification breakdown: No rows to classify")
    
    return df_classified, diagnostics


# =============================================================================
# AGGREGATION
# =============================================================================

def compute_daily_aggregates(df: DataFrame) -> Tuple[DataFrame, dict]:
    """
    Roll up recommendations to daily level per user.
    
    Parameters
    ----------
    df : DataFrame
        Must contain: _userId, time, is_autobolus
        
    Returns
    -------
    Tuple[DataFrame, dict]
        - DataFrame with columns: _userId, day, autobolus_count, recommendation_count, null_count, total_rows
        - Diagnostics dict with aggregation statistics
        
    Notes
    -----
    - autobolus_count: SUM of is_autobolus (count of autobolus recommendations)
    - recommendation_count: COUNT of non-null is_autobolus (excludes null rows)
    - null_count: COUNT of null is_autobolus (no recommendation)
    - total_rows: COUNT(*) including nulls (for coverage calculation)
    """
    df_daily = (
        df
        .withColumn("day", F.to_date("time"))
        .groupBy("_userId", "day")
        .agg(
            # Count of autobolus recommendations (is_autobolus = 1)
            F.sum(
                F.when(F.col("is_autobolus") == 1, 1).otherwise(0)
            ).alias("autobolus_count"),
            
            # Count of all valid recommendations (is_autobolus is not null)
            # This is the denominator for autobolus percentage
            F.sum(
                F.when(F.col("is_autobolus").isNotNull(), 1).otherwise(0)
            ).alias("recommendation_count"),
            
            # Count of null recommendations (neither basal nor bolus)
            F.sum(
                F.when(F.col("is_autobolus").isNull(), 1).otherwise(0)
            ).alias("null_count"),
            
            # Total rows including nulls (for coverage calculation)
            F.count("*").alias("total_rows"),
        )
    )
    
    # Compute diagnostics in single pass
    diagnostics = (
        df_daily
        .groupBy()
        .agg(
            F.count("*").alias("total_user_days"),
            F.countDistinct("_userId").alias("distinct_users"),
            F.sum("autobolus_count").alias("total_autobolus"),
            F.sum("recommendation_count").alias("total_recommendations"),
            F.sum("null_count").alias("total_null"),
            F.sum("total_rows").alias("total_rows"),
            F.avg("total_rows").alias("avg_rows_per_day"),
            F.min("total_rows").alias("min_rows_per_day"),
            F.max("total_rows").alias("max_rows_per_day"),
            F.avg("null_count").alias("avg_null_per_day"),
        )
        .collect()[0]
        .asDict()
    )
    
    # Log summary
    print(f"Daily aggregation complete:")
    print(f"  User-days: {diagnostics.get('total_user_days') or 0:,}")
    print(f"  Distinct users: {diagnostics.get('distinct_users') or 0:,}")
    
    avg_rows = diagnostics.get('avg_rows_per_day') or 0
    min_rows = diagnostics.get('min_rows_per_day') or 0
    max_rows = diagnostics.get('max_rows_per_day') or 0
    avg_null = diagnostics.get('avg_null_per_day') or 0
    
    print(f"  Avg rows/day: {avg_rows:.1f} (expected ~288 for 5-min intervals)")
    print(f"  Min/Max rows/day: {min_rows} / {max_rows}")
    print(f"  Avg null/day: {avg_null:.1f}")
    
    total_rec = diagnostics.get('total_recommendations') or 0
    total_ab = diagnostics.get('total_autobolus') or 0
    total_rows = diagnostics.get('total_rows') or 0
    total_null = diagnostics.get('total_null') or 0
    
    if total_rec > 0:
        overall_autobolus_pct = total_ab / total_rec * 100
        print(f"  Overall autobolus rate: {overall_autobolus_pct:.1f}%")
    
    if total_rows > 0:
        null_pct = total_null / total_rows * 100
        print(f"  Overall null rate: {null_pct:.1f}%")
    
    return df_daily, diagnostics


def compute_user_bounds(daily_df: DataFrame, config: PipelineConfig) -> Tuple[DataFrame, dict]:
    """
    Compute first and last day of data for each user.
    
    Used to ensure sliding windows don't extend before user's data begins.
    
    Parameters
    ----------
    daily_df : DataFrame
        Must contain: _userId, day
    config : PipelineConfig
        Used for minimum days threshold
        
    Returns
    -------
    Tuple[DataFrame, dict]
        - DataFrame with columns: _userId, first_day, last_day, total_days, calendar_span
        - Diagnostics dict with user statistics
    """
    df_bounds = (
        daily_df
        .groupBy("_userId")
        .agg(
            F.min("day").alias("first_day"),
            F.max("day").alias("last_day"),
            F.count("day").alias("total_days"),  # Days with data
        )
        .withColumn(
            # Calendar span (inclusive)
            "calendar_span",
            F.datediff(F.col("last_day"), F.col("first_day")) + 1
        )
    )
    
    # Compute diagnostics
    min_days_required = config.total_window_days  # seg1 + seg2 + gap
    
    diagnostics = (
        df_bounds
        .groupBy()
        .agg(
            F.count("*").alias("total_users"),
            F.avg("total_days").alias("avg_days_per_user"),
            F.min("total_days").alias("min_days"),
            F.max("total_days").alias("max_days"),
            F.avg("calendar_span").alias("avg_calendar_span"),
            # Users with enough days for at least one window
            F.sum(
                F.when(F.col("calendar_span") >= min_days_required, 1).otherwise(0)
            ).alias("users_with_enough_days"),
            F.sum(
                F.when(F.col("calendar_span") < min_days_required, 1).otherwise(0)
            ).alias("users_insufficient_days"),
        )
        .collect()[0]
        .asDict()
    )
    
    diagnostics["min_days_required"] = min_days_required
    
    # Log summary
    print(f"User bounds computed:")
    print(f"  Total users: {diagnostics.get('total_users') or 0:,}")
    
    avg_days = diagnostics.get('avg_days_per_user') or 0
    min_days = diagnostics.get('min_days') or 0
    max_days = diagnostics.get('max_days') or 0
    avg_span = diagnostics.get('avg_calendar_span') or 0
    users_enough = diagnostics.get('users_with_enough_days') or 0
    users_insuff = diagnostics.get('users_insufficient_days') or 0
    
    print(f"  Avg days with data per user: {avg_days:.1f}")
    print(f"  Min/Max days: {min_days} / {max_days}")
    print(f"  Avg calendar span: {avg_span:.1f} days")
    print(f"  Min days required for analysis: {min_days_required}")
    print(f"  Users with enough data: {users_enough:,}")
    print(f"  Users insufficient data: {users_insuff:,} (will be excluded)")
    
    return df_bounds, diagnostics


# =============================================================================
# SLIDING WINDOW CALCULATIONS
# =============================================================================

def compute_sliding_windows(
    daily_df: DataFrame,
    config: PipelineConfig
) -> Tuple[DataFrame, dict]:
    """
    Compute rolling aggregates for seg1 (before) and seg2 (after) windows.
    
    Window Structure
    ----------------
    For a row with day=D, we look back in time:
    - seg2 (after/current): [D - seg2_days + 1, D]  (most recent segment)
    - seg1 (before):        [D - seg1_days - seg2_days - gap + 1, D - seg2_days - gap]
    
    With default config (14/14/0):
    - seg2: [D-13, D]   = 14 days ending at D
    - seg1: [D-27, D-14] = 14 days before seg2
    
    The naming "seg1=before" and "seg2=after" refers to the transition:
    we're looking for users who were in one mode (seg1) then switched (seg2).
    
    Parameters
    ----------
    daily_df : DataFrame
        Daily aggregates with: _userId, day, autobolus_count, recommendation_count, 
        null_count, total_rows, first_day, last_day
    config : PipelineConfig
        Segment definitions
        
    Returns
    -------
    Tuple[DataFrame, dict]
        - DataFrame with original columns plus per-segment aggregates
        - Diagnostics dict
    """
    # Convert day to timestamp for RANGE window (needs numeric type)
    # We'll use days since epoch as the ordering column
    df_with_daynum = daily_df.withColumn(
        "day_num",
        F.datediff(F.col("day"), F.lit("1970-01-01"))
    )
    
    # Calculate window boundaries in days
    # seg2 is the "current" window ending at day D
    seg2_start_offset = config.seg2_days - 1  # e.g., 13 days back from D
    seg2_end_offset = 0  # ends at D
    
    # seg1 is the "before" window
    seg1_end_offset = config.seg2_days + config.gap_days  # e.g., 14 days back from D
    seg1_start_offset = config.seg1_days + config.seg2_days + config.gap_days - 1  # e.g., 27 days back
    
    # Define window specs using RANGE (calendar-aware)
    # RANGE BETWEEN X PRECEDING AND Y PRECEDING means [current - X, current - Y]
    window_seg1 = (
        Window
        .partitionBy("_userId")
        .orderBy("day_num")
        .rangeBetween(-seg1_start_offset, -seg1_end_offset)
    )
    
    window_seg2 = (
        Window
        .partitionBy("_userId")
        .orderBy("day_num")
        .rangeBetween(-seg2_start_offset, -seg2_end_offset)
    )
    
    # Compute rolling aggregates for each segment
    df_windowed = (
        df_with_daynum
        # Segment 1 (before) aggregates
        .withColumn("autobolus_seg1", F.sum("autobolus_count").over(window_seg1))
        .withColumn("rec_count_seg1", F.sum("recommendation_count").over(window_seg1))
        .withColumn("null_count_seg1", F.sum("null_count").over(window_seg1))
        .withColumn("total_rows_seg1", F.sum("total_rows").over(window_seg1))
        .withColumn("days_present_seg1", F.count("day").over(window_seg1))
        
        # Segment 2 (after) aggregates
        .withColumn("autobolus_seg2", F.sum("autobolus_count").over(window_seg2))
        .withColumn("rec_count_seg2", F.sum("recommendation_count").over(window_seg2))
        .withColumn("null_count_seg2", F.sum("null_count").over(window_seg2))
        .withColumn("total_rows_seg2", F.sum("total_rows").over(window_seg2))
        .withColumn("days_present_seg2", F.count("day").over(window_seg2))
        
        # Add window boundary dates for debugging/validation
        .withColumn("seg1_start", F.date_sub(F.col("day"), seg1_start_offset))
        .withColumn("seg1_end", F.date_sub(F.col("day"), seg1_end_offset))
        .withColumn("seg2_start", F.date_sub(F.col("day"), seg2_start_offset))
        .withColumn("seg2_end", F.col("day"))  # seg2 ends at current day
    )
    
    # Filter to rows where the full window is valid
    # (window start must be >= user's first day of data)
    df_valid_windows = df_windowed.filter(
        F.col("seg1_start") >= F.col("first_day")
    )
    
    # Compute diagnostics
    total_windows = df_windowed.count()
    valid_windows = df_valid_windows.count()
    
    diagnostics = {
        "seg1_days": config.seg1_days,
        "seg2_days": config.seg2_days,
        "gap_days": config.gap_days,
        "seg1_offset_range": f"-{seg1_start_offset} to -{seg1_end_offset}",
        "seg2_offset_range": f"-{seg2_start_offset} to -{seg2_end_offset}",
        "total_window_positions": total_windows,
        "valid_window_positions": valid_windows,
        "excluded_insufficient_history": total_windows - valid_windows,
    }
    
    # Additional stats on valid windows
    if valid_windows > 0:
        window_stats = (
            df_valid_windows
            .groupBy()
            .agg(
                F.countDistinct("_userId").alias("users_with_valid_windows"),
                F.avg("days_present_seg1").alias("avg_days_present_seg1"),
                F.avg("days_present_seg2").alias("avg_days_present_seg2"),
                F.avg("total_rows_seg1").alias("avg_rows_seg1"),
                F.avg("total_rows_seg2").alias("avg_rows_seg2"),
            )
            .collect()[0]
            .asDict()
        )
        diagnostics.update(window_stats)
    
    # Log summary
    print(f"Sliding window computation:")
    print(f"  Window structure: seg1=[D-{seg1_start_offset}, D-{seg1_end_offset}], seg2=[D-{seg2_start_offset}, D-{seg2_end_offset}]")
    print(f"  Total window positions: {total_windows:,}")
    print(f"  Valid windows (seg1 within data): {valid_windows:,}")
    print(f"  Excluded (insufficient history): {total_windows - valid_windows:,}")
    
    if valid_windows > 0:
        users_valid = diagnostics.get('users_with_valid_windows', 0) or 0
        avg_days_seg1 = diagnostics.get('avg_days_present_seg1', 0) or 0
        avg_days_seg2 = diagnostics.get('avg_days_present_seg2', 0) or 0
        print(f"  Users with valid windows: {users_valid:,}")
        print(f"  Avg days present: seg1={avg_days_seg1:.1f}, seg2={avg_days_seg2:.1f} (expected {config.seg1_days})")
    
    return df_valid_windows, diagnostics


# =============================================================================
# SCORING & METRICS
# =============================================================================

def compute_segment_metrics(
    windowed_df: DataFrame,
    config: PipelineConfig
) -> Tuple[DataFrame, dict]:
    """
    Compute coverage percentages, autobolus percentages, and quality scores.
    
    Metrics Computed
    ----------------
    - coverage_seg1/2: total_rows / expected_samples (data completeness)
    - autobolus_pct_seg1/2: autobolus_count / recommendation_count
    - l2_score_tb_to_ab: distance from ideal TB→AB transition
    - l2_score_ab_to_tb: distance from ideal AB→TB transition
    
    Parameters
    ----------
    windowed_df : DataFrame
        Output of compute_sliding_windows
    config : PipelineConfig
        Contains samples_per_segment for coverage denominator
        
    Returns
    -------
    Tuple[DataFrame, dict]
        - Original columns plus computed metrics
        - Diagnostics dict
        
    Notes
    -----
    L2 Score for TB→AB measures distance from ideal transition point (0%, 100%):
        sqrt((1 - autobolus_pct_seg1)^2 + (autobolus_pct_seg2)^2)
    Higher score = better TB→AB transition (seg1 low, seg2 high)
    
    L2 Score for AB→TB measures distance from ideal reverse transition (100%, 0%):
        sqrt((autobolus_pct_seg1)^2 + (1 - autobolus_pct_seg2)^2)
    Higher score = better AB→TB transition (seg1 high, seg2 low)
    """
    # Expected samples per segment for coverage calculation
    expected_samples_per_segment = config.samples_per_day * config.seg1_days
    
    df_metrics = (
        windowed_df
        # Total coverage: what fraction of expected samples do we have? (includes null/null rows)
        .withColumn(
            "coverage_seg1",
            F.col("total_rows_seg1") / F.lit(expected_samples_per_segment)
        )
        .withColumn(
            "coverage_seg2",
            F.col("total_rows_seg2") / F.lit(expected_samples_per_segment)
        )
        
        # Recommendation coverage: fraction with actual recommendations (excludes null/null rows)
        # This is more meaningful for data quality since null/null rows provide no information
        .withColumn(
            "rec_coverage_seg1",
            F.col("rec_count_seg1") / F.lit(expected_samples_per_segment)
        )
        .withColumn(
            "rec_coverage_seg2",
            F.col("rec_count_seg2") / F.lit(expected_samples_per_segment)
        )
        
        # Autobolus percentage: fraction of recommendations that are autobolus
        # Use CASE to handle division by zero
        .withColumn(
            "autobolus_pct_seg1",
            F.when(
                F.col("rec_count_seg1") > 0,
                F.col("autobolus_seg1") / F.col("rec_count_seg1")
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "autobolus_pct_seg2",
            F.when(
                F.col("rec_count_seg2") > 0,
                F.col("autobolus_seg2") / F.col("rec_count_seg2")
            ).otherwise(F.lit(None))
        )
        
        # L2 score for Temp Basal → Autobolus transition
        # High score when seg1 is LOW autobolus and seg2 is HIGH autobolus
        # Ideal: (0%, 100%) → score = sqrt(1 + 1) = 1.414
        .withColumn(
            "l2_score_tb_to_ab",
            F.when(
                F.col("autobolus_pct_seg1").isNotNull() & F.col("autobolus_pct_seg2").isNotNull(),
                F.sqrt(
                    F.pow(1.0 - F.col("autobolus_pct_seg1"), 2) +
                    F.pow(F.col("autobolus_pct_seg2"), 2)
                )
            ).otherwise(F.lit(None))
        )
        
        # L2 score for Autobolus → Temp Basal transition
        # High score when seg1 is HIGH autobolus and seg2 is LOW autobolus
        # Ideal: (100%, 0%) → score = sqrt(1 + 1) = 1.414
        .withColumn(
            "l2_score_ab_to_tb",
            F.when(
                F.col("autobolus_pct_seg1").isNotNull() & F.col("autobolus_pct_seg2").isNotNull(),
                F.sqrt(
                    F.pow(F.col("autobolus_pct_seg1"), 2) +
                    F.pow(1.0 - F.col("autobolus_pct_seg2"), 2)
                )
            ).otherwise(F.lit(None))
        )
    )
    
    # Compute diagnostics
    diagnostics = (
        df_metrics
        .groupBy()
        .agg(
            F.count("*").alias("total_windows"),
            
            # Total coverage stats (includes null/null rows)
            F.avg("coverage_seg1").alias("avg_coverage_seg1"),
            F.avg("coverage_seg2").alias("avg_coverage_seg2"),
            F.sum(F.when(F.col("coverage_seg1") >= config.min_coverage, 1).otherwise(0)).alias("windows_good_coverage_seg1"),
            F.sum(F.when(F.col("coverage_seg2") >= config.min_coverage, 1).otherwise(0)).alias("windows_good_coverage_seg2"),
            F.sum(
                F.when(
                    (F.col("coverage_seg1") >= config.min_coverage) & 
                    (F.col("coverage_seg2") >= config.min_coverage), 
                    1
                ).otherwise(0)
            ).alias("windows_good_coverage_both"),
            
            # Recommendation coverage stats (excludes null/null rows - used for filtering)
            F.avg("rec_coverage_seg1").alias("avg_rec_coverage_seg1"),
            F.avg("rec_coverage_seg2").alias("avg_rec_coverage_seg2"),
            F.sum(F.when(F.col("rec_coverage_seg1") >= config.min_coverage, 1).otherwise(0)).alias("windows_good_rec_coverage_seg1"),
            F.sum(F.when(F.col("rec_coverage_seg2") >= config.min_coverage, 1).otherwise(0)).alias("windows_good_rec_coverage_seg2"),
            F.sum(
                F.when(
                    (F.col("rec_coverage_seg1") >= config.min_coverage) & 
                    (F.col("rec_coverage_seg2") >= config.min_coverage), 
                    1
                ).otherwise(0)
            ).alias("windows_good_rec_coverage_both"),
            
            # Autobolus pct stats
            F.avg("autobolus_pct_seg1").alias("avg_autobolus_pct_seg1"),
            F.avg("autobolus_pct_seg2").alias("avg_autobolus_pct_seg2"),
            
            # L2 score stats
            F.avg("l2_score_tb_to_ab").alias("avg_l2_tb_to_ab"),
            F.max("l2_score_tb_to_ab").alias("max_l2_tb_to_ab"),
            F.avg("l2_score_ab_to_tb").alias("avg_l2_ab_to_tb"),
            F.max("l2_score_ab_to_tb").alias("max_l2_ab_to_tb"),
            
            # Null counts
            F.sum(F.when(F.col("autobolus_pct_seg1").isNull(), 1).otherwise(0)).alias("null_pct_seg1"),
            F.sum(F.when(F.col("autobolus_pct_seg2").isNull(), 1).otherwise(0)).alias("null_pct_seg2"),
        )
        .collect()[0]
        .asDict()
    )
    
    diagnostics["expected_samples_per_segment"] = expected_samples_per_segment
    diagnostics["min_coverage_threshold"] = config.min_coverage
    
    # Log summary
    print(f"Segment metrics computed:")
    print(f"  Expected samples per segment: {expected_samples_per_segment:,}")
    
    avg_cov_seg1 = diagnostics.get('avg_coverage_seg1') or 0
    avg_cov_seg2 = diagnostics.get('avg_coverage_seg2') or 0
    avg_rec_cov_seg1 = diagnostics.get('avg_rec_coverage_seg1') or 0
    avg_rec_cov_seg2 = diagnostics.get('avg_rec_coverage_seg2') or 0
    avg_ab_seg1 = diagnostics.get('avg_autobolus_pct_seg1') or 0
    avg_ab_seg2 = diagnostics.get('avg_autobolus_pct_seg2') or 0
    avg_l2_tb = diagnostics.get('avg_l2_tb_to_ab') or 0
    max_l2_tb = diagnostics.get('max_l2_tb_to_ab') or 0
    avg_l2_ab = diagnostics.get('avg_l2_ab_to_tb') or 0
    max_l2_ab = diagnostics.get('max_l2_ab_to_tb') or 0
    
    print(f"  Avg total coverage: seg1={avg_cov_seg1:.1%}, seg2={avg_cov_seg2:.1%}")
    print(f"  Avg rec coverage:   seg1={avg_rec_cov_seg1:.1%}, seg2={avg_rec_cov_seg2:.1%}")
    print(f"  Windows with good rec coverage (>={config.min_coverage:.0%}): {diagnostics.get('windows_good_rec_coverage_both') or 0:,} / {diagnostics.get('total_windows') or 0:,}")
    print(f"  Avg autobolus %: seg1={avg_ab_seg1:.1%}, seg2={avg_ab_seg2:.1%}")
    print(f"  L2 scores (TB→AB): avg={avg_l2_tb:.3f}, max={max_l2_tb:.3f}")
    print(f"  L2 scores (AB→TB): avg={avg_l2_ab:.3f}, max={max_l2_ab:.3f}")
    
    if diagnostics['null_pct_seg1'] > 0 or diagnostics['null_pct_seg2'] > 0:
        print(f"  ⚠️  Windows with null %: seg1={diagnostics['null_pct_seg1']}, seg2={diagnostics['null_pct_seg2']}")
    
    return df_metrics, diagnostics


def classify_transition_type(
    row_autobolus_pct_seg1: float,
    row_autobolus_pct_seg2: float,
    config: PipelineConfig
) -> TransitionType:
    """
    Classify whether a window represents a valid transition and which direction.
    
    Parameters
    ----------
    row_autobolus_pct_seg1 : float
        Autobolus percentage in "before" segment
    row_autobolus_pct_seg2 : float
        Autobolus percentage in "after" segment
    config : PipelineConfig
        Contains autobolus_low and autobolus_high thresholds
        
    Returns
    -------
    TransitionType
        TEMP_BASAL_TO_AUTOBOLUS if seg1 < low AND seg2 > high
        AUTOBOLUS_TO_TEMP_BASAL if seg1 > high AND seg2 < low
        NONE otherwise
    """
    if row_autobolus_pct_seg1 is None or row_autobolus_pct_seg2 is None:
        return TransitionType.NONE
    
    if row_autobolus_pct_seg1 < config.autobolus_low and row_autobolus_pct_seg2 > config.autobolus_high:
        return TransitionType.TEMP_BASAL_TO_AUTOBOLUS
    elif row_autobolus_pct_seg1 > config.autobolus_high and row_autobolus_pct_seg2 < config.autobolus_low:
        return TransitionType.AUTOBOLUS_TO_TEMP_BASAL
    else:
        return TransitionType.NONE


# =============================================================================
# FILTERING & VALIDATION  
# =============================================================================

def filter_valid_windows(
    scored_df: DataFrame,
    config: PipelineConfig
) -> Tuple[DataFrame, dict]:
    """
    Apply data quality and transition validity filters.
    
    Filters Applied
    ---------------
    1. Coverage >= min_coverage for BOTH segments
    2. recommendation_count > 0 for both segments (non-null autobolus_pct)
    3. Window represents a valid transition (seg1/seg2 in opposite modes)
    
    Parameters
    ----------
    scored_df : DataFrame
        Output of compute_segment_metrics
    config : PipelineConfig
        Thresholds for filtering
        
    Returns
    -------
    Tuple[DataFrame, dict]
        - Filtered DataFrame with transition_type column
        - Diagnostics dict with filter statistics
    """
    total_before = scored_df.count()
    
    # Track exclusions at each step
    filter_stats = {"total_windows_input": total_before}
    
    # -------------------------------------------------------------------------
    # Filter 1: Recommendation Coverage >= min_coverage for BOTH segments
    # Uses rec_coverage (excludes null/null rows) rather than total coverage
    # This ensures we have actual recommendations, not just rows logged
    # -------------------------------------------------------------------------
    df_coverage = scored_df.filter(
        (F.col("rec_coverage_seg1") >= config.min_coverage) &
        (F.col("rec_coverage_seg2") >= config.min_coverage)
    )
    after_coverage = df_coverage.count()
    filter_stats["after_rec_coverage_filter"] = after_coverage
    filter_stats["excluded_low_rec_coverage"] = total_before - after_coverage
    
    # -------------------------------------------------------------------------
    # Filter 2: Valid autobolus percentages (non-null, meaning rec_count > 0)
    # -------------------------------------------------------------------------
    df_valid_pct = df_coverage.filter(
        F.col("autobolus_pct_seg1").isNotNull() &
        F.col("autobolus_pct_seg2").isNotNull()
    )
    after_valid_pct = df_valid_pct.count()
    filter_stats["after_valid_pct_filter"] = after_valid_pct
    filter_stats["excluded_null_pct"] = after_coverage - after_valid_pct
    
    # -------------------------------------------------------------------------
    # Classify transition type
    # -------------------------------------------------------------------------
    df_classified = df_valid_pct.withColumn(
        "transition_type",
        F.when(
            # TB → AB: seg1 below low threshold, seg2 above high threshold
            (F.col("autobolus_pct_seg1") < config.autobolus_low) &
            (F.col("autobolus_pct_seg2") > config.autobolus_high),
            F.lit(TransitionType.TEMP_BASAL_TO_AUTOBOLUS.value)
        )
        .when(
            # AB → TB: seg1 above high threshold, seg2 below low threshold
            (F.col("autobolus_pct_seg1") > config.autobolus_high) &
            (F.col("autobolus_pct_seg2") < config.autobolus_low),
            F.lit(TransitionType.AUTOBOLUS_TO_TEMP_BASAL.value)
        )
        .otherwise(F.lit(TransitionType.NONE.value))
    )
    
    # -------------------------------------------------------------------------
    # Filter 3: Only keep valid transitions (exclude NONE)
    # -------------------------------------------------------------------------
    df_transitions = df_classified.filter(
        F.col("transition_type") != TransitionType.NONE.value
    )
    after_transition = df_transitions.count()
    filter_stats["after_transition_filter"] = after_transition
    filter_stats["excluded_no_transition"] = after_valid_pct - after_transition
    
    # -------------------------------------------------------------------------
    # Count by transition type
    # -------------------------------------------------------------------------
    transition_counts = (
        df_transitions
        .groupBy("transition_type")
        .count()
        .collect()
    )
    filter_stats["transition_counts"] = {row["transition_type"]: row["count"] for row in transition_counts}
    
    # Count distinct users
    users_with_transitions = df_transitions.select("_userId").distinct().count()
    filter_stats["users_with_valid_transitions"] = users_with_transitions
    
    # Build diagnostics
    diagnostics = {
        "filters": filter_stats,
        "config": {
            "min_coverage": config.min_coverage,
            "autobolus_low": config.autobolus_low,
            "autobolus_high": config.autobolus_high,
        }
    }
    
    # Log summary
    print(f"Filter valid windows:")
    print(f"  Input windows: {total_before:,}")
    print(f"  After rec_coverage filter (>={config.min_coverage:.0%}): {after_coverage:,} (-{total_before - after_coverage:,})")
    print(f"  After valid % filter (non-null): {after_valid_pct:,} (-{after_coverage - after_valid_pct:,})")
    print(f"  After transition filter: {after_transition:,} (-{after_valid_pct - after_transition:,})")
    print(f"\nTransition breakdown:")
    for t_type, count in filter_stats["transition_counts"].items():
        print(f"  {t_type}: {count:,}")
    print(f"\nUsers with valid transitions: {users_with_transitions:,}")
    
    return df_transitions, diagnostics


def compute_exclusion_summary(
    before_df: DataFrame,
    after_df: DataFrame,
    stage_name: str
) -> dict:
    """
    Compute summary statistics for regulatory documentation of exclusions.
    
    Parameters
    ----------
    before_df : DataFrame
        Data before filtering
    after_df : DataFrame
        Data after filtering
    stage_name : str
        Human-readable name for this filtering stage
        
    Returns
    -------
    dict
        Keys: stage_name, rows_before, rows_after, rows_excluded,
              users_before, users_after, users_excluded
    """
    # TODO: Implement
    raise NotImplementedError


# =============================================================================
# RANKING & SELECTION
# =============================================================================

def select_best_windows_per_user(
    valid_windows_df: DataFrame
) -> Tuple[DataFrame, dict]:
    """
    Select the single best transition window per user per direction.
    
    Selection Criteria
    ------------------
    - For TB→AB: Window with HIGHEST l2_score_tb_to_ab (best separation)
    - For AB→TB: Window with HIGHEST l2_score_ab_to_tb (best separation)
    
    A user may appear in results twice if they have valid transitions
    in both directions.
    
    Parameters
    ----------
    valid_windows_df : DataFrame
        Filtered valid transition windows with transition_type
        
    Returns
    -------
    Tuple[DataFrame, dict]
        - At most 2 rows per user (one per direction)
        - Diagnostics dict
    """
    # Add the appropriate L2 score column based on transition type
    df_with_score = valid_windows_df.withColumn(
        "l2_score",
        F.when(
            F.col("transition_type") == TransitionType.TEMP_BASAL_TO_AUTOBOLUS.value,
            F.col("l2_score_tb_to_ab")
        ).otherwise(
            F.col("l2_score_ab_to_tb")
        )
    )
    
    # Rank windows within each user + transition_type by L2 score (descending)
    # Higher L2 = better transition in both directions
    window_spec = (
        Window
        .partitionBy("_userId", "transition_type")
        .orderBy(F.desc("l2_score"))
    )
    
    df_ranked = df_with_score.withColumn(
        "rank",
        F.row_number().over(window_spec)
    )
    
    # Keep only the best window per user per transition type
    df_best = df_ranked.filter(F.col("rank") == 1).drop("rank")
    
    # Select relevant columns for output
    output_columns = [
        "_userId",
        "transition_type",
        "day",
        "seg1_start",
        "seg1_end",
        "seg2_start",
        "seg2_end",
        "autobolus_pct_seg1",
        "autobolus_pct_seg2",
        "coverage_seg1",
        "coverage_seg2",
        "l2_score",
        "l2_score_tb_to_ab",
        "l2_score_ab_to_tb",
        "days_present_seg1",
        "days_present_seg2",
        "total_rows_seg1",
        "total_rows_seg2",
    ]
    
    # Only select columns that exist
    existing_columns = [c for c in output_columns if c in df_best.columns]
    df_output = df_best.select(existing_columns)
    
    # Compute diagnostics
    diagnostics = (
        df_output
        .groupBy()
        .agg(
            F.count("*").alias("total_best_windows"),
            F.countDistinct("_userId").alias("users_with_best_windows"),
            F.sum(
                F.when(F.col("transition_type") == TransitionType.TEMP_BASAL_TO_AUTOBOLUS.value, 1).otherwise(0)
            ).alias("tb_to_ab_count"),
            F.sum(
                F.when(F.col("transition_type") == TransitionType.AUTOBOLUS_TO_TEMP_BASAL.value, 1).otherwise(0)
            ).alias("ab_to_tb_count"),
            F.avg("l2_score").alias("avg_l2_score"),
            F.min("l2_score").alias("min_l2_score"),
            F.max("l2_score").alias("max_l2_score"),
        )
        .collect()[0]
        .asDict()
    )
    
    # Log summary
    print(f"Select best windows per user:")
    print(f"  Total best windows: {diagnostics.get('total_best_windows') or 0:,}")
    print(f"  Users with transitions: {diagnostics.get('users_with_best_windows') or 0:,}")
    print(f"  TB→AB transitions: {diagnostics.get('tb_to_ab_count') or 0:,}")
    print(f"  AB→TB transitions: {diagnostics.get('ab_to_tb_count') or 0:,}")
    
    min_l2 = diagnostics.get('min_l2_score') or 0
    max_l2 = diagnostics.get('max_l2_score') or 0
    avg_l2 = diagnostics.get('avg_l2_score') or 0
    print(f"  L2 score range: {min_l2:.3f} - {max_l2:.3f} (avg: {avg_l2:.3f})")
    
    return df_output, diagnostics


def pivot_to_user_summary(
    best_windows_df: DataFrame
) -> Tuple[DataFrame, dict]:
    """
    Pivot best windows to one row per user with both transition directions.
    
    Matches the output format of the original SQL query.
    
    Parameters
    ----------
    best_windows_df : DataFrame
        Output of select_best_windows_per_user
        
    Returns
    -------
    Tuple[DataFrame, dict]
        - One row per user with columns for both TB→AB and AB→TB transitions
        - Diagnostics dict
    """
    tb_to_ab = TransitionType.TEMP_BASAL_TO_AUTOBOLUS.value
    ab_to_tb = TransitionType.AUTOBOLUS_TO_TEMP_BASAL.value
    
    # Split into two DataFrames by transition type
    df_tb_to_ab = (
        best_windows_df
        .filter(F.col("transition_type") == tb_to_ab)
        .select(
            F.col("_userId"),
            F.col("seg1_start").alias("tb_to_ab_seg1_start"),
            F.col("seg1_end").alias("tb_to_ab_seg1_end"),
            F.col("seg2_start").alias("tb_to_ab_seg2_start"),
            F.col("seg2_end").alias("tb_to_ab_seg2_end"),
            F.col("autobolus_pct_seg1").alias("tb_to_ab_pct_seg1"),
            F.col("autobolus_pct_seg2").alias("tb_to_ab_pct_seg2"),
            F.col("coverage_seg1").alias("tb_to_ab_coverage_seg1"),
            F.col("coverage_seg2").alias("tb_to_ab_coverage_seg2"),
            F.col("l2_score").alias("tb_to_ab_l2_score"),
        )
    )
    
    df_ab_to_tb = (
        best_windows_df
        .filter(F.col("transition_type") == ab_to_tb)
        .select(
            F.col("_userId"),
            F.col("seg1_start").alias("ab_to_tb_seg1_start"),
            F.col("seg1_end").alias("ab_to_tb_seg1_end"),
            F.col("seg2_start").alias("ab_to_tb_seg2_start"),
            F.col("seg2_end").alias("ab_to_tb_seg2_end"),
            F.col("autobolus_pct_seg1").alias("ab_to_tb_pct_seg1"),
            F.col("autobolus_pct_seg2").alias("ab_to_tb_pct_seg2"),
            F.col("coverage_seg1").alias("ab_to_tb_coverage_seg1"),
            F.col("coverage_seg2").alias("ab_to_tb_coverage_seg2"),
            F.col("l2_score").alias("ab_to_tb_l2_score"),
        )
    )
    
    # Get all unique users
    all_users = best_windows_df.select("_userId").distinct()
    
    # Full outer join to get one row per user
    df_summary = (
        all_users
        .join(df_tb_to_ab, on="_userId", how="left")
        .join(df_ab_to_tb, on="_userId", how="left")
        .orderBy("_userId")
    )
    
    # Compute diagnostics
    total_users = df_summary.count()
    
    users_with_both = df_summary.filter(
        F.col("tb_to_ab_l2_score").isNotNull() & 
        F.col("ab_to_tb_l2_score").isNotNull()
    ).count()
    
    users_tb_only = df_summary.filter(
        F.col("tb_to_ab_l2_score").isNotNull() & 
        F.col("ab_to_tb_l2_score").isNull()
    ).count()
    
    users_ab_only = df_summary.filter(
        F.col("tb_to_ab_l2_score").isNull() & 
        F.col("ab_to_tb_l2_score").isNotNull()
    ).count()
    
    diagnostics = {
        "total_users": total_users,
        "users_with_both_transitions": users_with_both,
        "users_tb_to_ab_only": users_tb_only,
        "users_ab_to_tb_only": users_ab_only,
    }
    
    # Log summary
    print(f"Pivot to user summary:")
    print(f"  Total users: {total_users:,}")
    print(f"  Users with TB→AB only: {users_tb_only:,}")
    print(f"  Users with AB→TB only: {users_ab_only:,}")
    print(f"  Users with BOTH transitions: {users_with_both:,}")
    
    return df_summary, diagnostics


# =============================================================================
# VALIDATION HELPERS
# =============================================================================

def validate_dataframe(
    df: DataFrame,
    expected_columns: List[str],
    stage_name: str,
    min_rows: int = 1
) -> None:
    """
    Validate DataFrame meets expectations, raise descriptive error if not.
    
    Parameters
    ----------
    df : DataFrame
        DataFrame to validate
    expected_columns : List[str]
        Columns that must be present
    stage_name : str
        Name for error messages
    min_rows : int
        Minimum expected row count
        
    Raises
    ------
    ValueError
        If validation fails
    """
    # TODO: Implement
    raise NotImplementedError


def log_stage_statistics(
    df: DataFrame,
    stage_name: str,
    group_col: str = "_userId"
) -> dict:
    """
    Log summary statistics for a pipeline stage.
    
    Parameters
    ----------
    df : DataFrame
        Current DataFrame
    stage_name : str
        Human-readable stage name
    group_col : str
        Column to count distinct values of (usually _userId)
        
    Returns
    -------
    dict
        Statistics including row_count, distinct_users, null_counts per column
    """
    # TODO: Implement
    raise NotImplementedError


def validate_known_user(
    df: DataFrame,
    user_id: str,
    expected_values: dict,
    stage_name: str
) -> bool:
    """
    Validate that a known test user has expected values at this stage.
    
    For regression testing with known ground-truth users.
    
    Parameters
    ----------
    df : DataFrame
        Current DataFrame
    user_id : str
        User ID to check
    expected_values : dict
        Column name → expected value mapping
    stage_name : str
        For error messages
        
    Returns
    -------
    bool
        True if validation passes
        
    Raises
    ------
    AssertionError
        If values don't match expected
    """
    # TODO: Implement
    raise NotImplementedError


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def run_pipeline(
    spark: SparkSession,
    config: Optional[PipelineConfig] = None,
    run_diagnostics: bool = True,
    test_users: Optional[List[str]] = None
) -> Tuple[DataFrame, dict]:
    """
    Execute the full transition detection pipeline.
    
    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    config : PipelineConfig, optional
        Configuration (uses defaults if not provided)
    run_diagnostics : bool
        If True, run counts and validation at each stage
    test_users : List[str], optional
        User IDs to trace through pipeline for debugging
        
    Returns
    -------
    Tuple[DataFrame, dict]
        - Final user summary DataFrame
        - Pipeline metadata including stage statistics and exclusion summaries
    """
    if config is None:
        config = PipelineConfig()
    
    metadata = {
        "config": {
            "seg1_days": config.seg1_days,
            "seg2_days": config.seg2_days,
            "gap_days": config.gap_days,
            "min_coverage": config.min_coverage,
            "samples_per_day": config.samples_per_day,
            "autobolus_low": config.autobolus_low,
            "autobolus_high": config.autobolus_high,
            "source_table": config.source_table,
            "reason_filter": config.reason_filter,
        },
        "stages": {},
        "exclusions": []
    }
    
    print("=" * 60)
    print("TRANSITION DETECTION PIPELINE")
    print("=" * 60)
    print(f"\nConfiguration:")
    for key, value in metadata["config"].items():
        print(f"  {key}: {value}")
    print()
    
    # =========================================================================
    # Stage 1: Load and clean
    # =========================================================================
    print("-" * 60)
    print("Stage 1: Loading base recommendations")
    print("-" * 60)
    
    df_base, load_diagnostics = load_base_recommendations(
        spark, config, run_diagnostics=run_diagnostics
    )
    metadata["stages"]["1_load"] = load_diagnostics
    
    # Optional: check for duplicate timestamps
    if run_diagnostics:
        dup_check = check_for_duplicate_timestamps(df_base, sample_fraction=0.01)
        metadata["stages"]["1_load"]["duplicate_check"] = dup_check
    
    # Trace test users if specified
    if test_users and run_diagnostics:
        print(f"\nTracing test users through Stage 1:")
        for user_id in test_users:
            count = df_base.filter(F.col("_userId") == user_id).count()
            print(f"  {user_id}: {count:,} rows")
    
    # =========================================================================
    # Stage 1b: Deduplicate
    # =========================================================================
    print("\n" + "-" * 60)
    print("Stage 1b: Deduplicating recommendations")
    print("-" * 60)
    
    df_deduped, dedup_diagnostics = deduplicate_recommendations(
        df_base, run_diagnostics=run_diagnostics
    )
    metadata["stages"]["1b_dedup"] = dedup_diagnostics
    
    # Trace test users
    if test_users and run_diagnostics:
        print(f"\nTracing test users through Stage 1b:")
        for user_id in test_users:
            count_before = df_base.filter(F.col("_userId") == user_id).count()
            count_after = df_deduped.filter(F.col("_userId") == user_id).count()
            removed = count_before - count_after
            print(f"  {user_id}: {count_before:,} → {count_after:,} ({removed:,} duplicates removed)")
    
    # =========================================================================
    # Stage 2: Classify recommendations
    # =========================================================================
    print("\n" + "-" * 60)
    print("Stage 2: Classifying recommendation types")
    print("-" * 60)
    
    df_classified, classify_diagnostics = classify_recommendation_type(df_deduped)
    metadata["stages"]["2_classify"] = classify_diagnostics
    
    # Trace test users
    if test_users and run_diagnostics:
        print(f"\nTracing test users through Stage 2:")
        for user_id in test_users:
            user_df = df_classified.filter(F.col("_userId") == user_id)
            breakdown = (
                user_df
                .groupBy("is_autobolus")
                .count()
                .collect()
            )
            print(f"  {user_id}: {dict((r['is_autobolus'], r['count']) for r in breakdown)}")
    
    # =========================================================================
    # Stage 3: Daily aggregation
    # =========================================================================
    print("\n" + "-" * 60)
    print("Stage 3: Computing daily aggregates")
    print("-" * 60)
    
    df_daily, daily_diagnostics = compute_daily_aggregates(df_classified)
    metadata["stages"]["3_daily"] = daily_diagnostics
    
    # Trace test users
    if test_users and run_diagnostics:
        print(f"\nTracing test users through Stage 3:")
        for user_id in test_users:
            user_days = df_daily.filter(F.col("_userId") == user_id).count()
            user_sample = (
                df_daily
                .filter(F.col("_userId") == user_id)
                .orderBy("day")
                .limit(5)
                .collect()
            )
            print(f"  {user_id}: {user_days} days")
            for row in user_sample:
                ab_pct = row['autobolus_count'] / row['recommendation_count'] * 100 if row['recommendation_count'] > 0 else 0
                print(f"    {row['day']}: {row['autobolus_count']}/{row['recommendation_count']} ({ab_pct:.0f}% AB), {row['total_rows']} total rows")
    
    # =========================================================================
    # Stage 4: User bounds
    # =========================================================================
    print("\n" + "-" * 60)
    print("Stage 4: Computing user bounds")
    print("-" * 60)
    
    df_bounds, bounds_diagnostics = compute_user_bounds(df_daily, config)
    metadata["stages"]["4_bounds"] = bounds_diagnostics
    
    # Join bounds back to daily data for downstream use
    df_daily_with_bounds = df_daily.join(df_bounds, on="_userId", how="inner")
    
    # Trace test users
    if test_users and run_diagnostics:
        print(f"\nTracing test users through Stage 4:")
        for user_id in test_users:
            user_bounds = df_bounds.filter(F.col("_userId") == user_id).collect()
            if user_bounds:
                row = user_bounds[0]
                print(f"  {user_id}: {row['first_day']} to {row['last_day']} ({row['total_days']} days, {row['calendar_span']} calendar span)")
    
    # =========================================================================
    # Stage 5: Sliding windows
    # =========================================================================
    print("\n" + "-" * 60)
    print("Stage 5: Computing sliding windows")
    print("-" * 60)
    
    df_windows, windows_diagnostics = compute_sliding_windows(df_daily_with_bounds, config)
    metadata["stages"]["5_windows"] = windows_diagnostics
    
    # Trace test users
    if test_users and run_diagnostics:
        print(f"\nTracing test users through Stage 5:")
        for user_id in test_users:
            user_windows = df_windows.filter(F.col("_userId") == user_id)
            window_count = user_windows.count()
            if window_count > 0:
                # Show first and last window
                first_window = user_windows.orderBy("day").first()
                last_window = user_windows.orderBy(F.desc("day")).first()
                print(f"  {user_id}: {window_count} valid windows")
                print(f"    First: day={first_window['day']}, seg1=[{first_window['seg1_start']}, {first_window['seg1_end']}], seg2=[{first_window['seg2_start']}, {first_window['seg2_end']}]")
                print(f"    Last:  day={last_window['day']}, seg1=[{last_window['seg1_start']}, {last_window['seg1_end']}], seg2=[{last_window['seg2_start']}, {last_window['seg2_end']}]")
            else:
                print(f"  {user_id}: 0 valid windows (insufficient history)")
    
    # =========================================================================
    # Stage 6: Compute metrics
    # =========================================================================
    print("\n" + "-" * 60)
    print("Stage 6: Computing segment metrics")
    print("-" * 60)
    
    df_scored, metrics_diagnostics = compute_segment_metrics(df_windows, config)
    metadata["stages"]["6_metrics"] = metrics_diagnostics
    
    # Trace test users
    if test_users and run_diagnostics:
        print(f"\nTracing test users through Stage 6:")
        for user_id in test_users:
            # Get a sample window with highest L2 score
            user_best = (
                df_scored
                .filter(F.col("_userId") == user_id)
                .orderBy(F.desc("l2_score_tb_to_ab"))
                .limit(1)
                .collect()
            )
            if user_best:
                row = user_best[0]
                cov_seg1 = row['coverage_seg1'] or 0
                cov_seg2 = row['coverage_seg2'] or 0
                ab_seg1 = row['autobolus_pct_seg1'] or 0
                ab_seg2 = row['autobolus_pct_seg2'] or 0
                l2_tb = row['l2_score_tb_to_ab'] or 0
                l2_ab = row['l2_score_ab_to_tb'] or 0
                print(f"  {user_id}: Best TB→AB window at day={row['day']}")
                print(f"    Coverage: seg1={cov_seg1:.1%}, seg2={cov_seg2:.1%}")
                print(f"    Autobolus %: seg1={ab_seg1:.1%}, seg2={ab_seg2:.1%}")
                print(f"    L2 scores: TB→AB={l2_tb:.3f}, AB→TB={l2_ab:.3f}")
    
    # =========================================================================
    # Stage 7: Filter valid windows
    # =========================================================================
    print("\n" + "-" * 60)
    print("Stage 7: Filtering valid transition windows")
    print("-" * 60)
    
    df_valid, filter_diagnostics = filter_valid_windows(df_scored, config)
    metadata["stages"]["7_filter"] = filter_diagnostics
    
    # Trace test users
    if test_users and run_diagnostics:
        print(f"\nTracing test users through Stage 7:")
        for user_id in test_users:
            user_transitions = (
                df_valid
                .filter(F.col("_userId") == user_id)
                .groupBy("transition_type")
                .count()
                .collect()
            )
            if user_transitions:
                counts = {row["transition_type"]: row["count"] for row in user_transitions}
                print(f"  {user_id}: {counts}")
            else:
                print(f"  {user_id}: No valid transitions")
    
    # =========================================================================
    # Stage 8: Select best per user
    # =========================================================================
    print("\n" + "-" * 60)
    print("Stage 8: Selecting best windows per user")
    print("-" * 60)
    
    df_best, best_diagnostics = select_best_windows_per_user(df_valid)
    metadata["stages"]["8_best"] = best_diagnostics
    
    # Trace test users
    if test_users and run_diagnostics:
        print(f"\nTracing test users through Stage 8:")
        for user_id in test_users:
            user_best = df_best.filter(F.col("_userId") == user_id).collect()
            if user_best:
                for row in user_best:
                    l2 = row['l2_score'] or 0
                    print(f"  {user_id}: {row['transition_type']}, L2={l2:.3f}, seg1=[{row['seg1_start']}, {row['seg1_end']}]")
            else:
                print(f"  {user_id}: No best windows selected")
    
    # =========================================================================
    # Stage 9: Pivot to summary
    # =========================================================================
    print("\n" + "-" * 60)
    print("Stage 9: Pivoting to user summary")
    print("-" * 60)
    
    df_summary, summary_diagnostics = pivot_to_user_summary(df_best)
    metadata["stages"]["9_summary"] = summary_diagnostics
    
    # Trace test users
    if test_users and run_diagnostics:
        print(f"\nTracing test users through Stage 9:")
        for user_id in test_users:
            user_summary = df_summary.filter(F.col("_userId") == user_id).collect()
            if user_summary:
                row = user_summary[0]
                has_tb = row['tb_to_ab_l2_score'] is not None
                has_ab = row['ab_to_tb_l2_score'] is not None
                print(f"  {user_id}: TB→AB={'Yes' if has_tb else 'No'}, AB→TB={'Yes' if has_ab else 'No'}")
            else:
                print(f"  {user_id}: Not in final summary")
    
    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    
    df_final = df_summary
    
    if run_diagnostics:
        final_count = df_final.count()
        print(f"\nFinal output: {final_count:,} users with valid transitions")
        metadata["final_row_count"] = final_count
        metadata["final_user_count"] = final_count  # One row per user now
    
    return df_final, metadata


# =============================================================================
# ENTRY POINT FOR DATABRICKS
# =============================================================================

if __name__ == "__main__":
    # When run directly or via %run in Databricks, execute with defaults
    # Assumes `spark` is already available in the Databricks environment
    
    try:
        # Check if spark session exists (Databricks provides this automatically)
        spark
    except NameError:
        # Running locally - create a spark session
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder
            .appName("TransitionDetection")
            .master("local[*]")
            .getOrCreate()
        )
        print("Created local SparkSession")
    
    # Run with default configuration, but use sample table
    config = PipelineConfig(
        source_table="bddp_sample_all"
    )
    
    print("\n" + "=" * 60)
    print("Running Transition Detection Pipeline")
    print("=" * 60 + "\n")
    
    try:
        results_df, metadata = run_pipeline(
            spark=spark,
            config=config,
            run_diagnostics=True,
            test_users=None  # Add specific user IDs here for debugging
        )
        
        print("\n\nMetadata summary:")
        print(f"  Stages completed: {list(metadata['stages'].keys())}")
        
        # Display sample results (works in Databricks)
        try:
            display(results_df.limit(20))
        except NameError:
            # Not in Databricks - use show()
            results_df.show(20, truncate=False)
            
    except ValueError as e:
        print(f"\n❌ Pipeline failed: {e}")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        raise
