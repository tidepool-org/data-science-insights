CATALOG = "dev.fda_510k_rwd"

# PLN-1001 Analysis 6 (RPT-1001 Rev 02 §4.4): a 14-day segment of 100%
# autobolus use beginning at least 30 days after the user's first autobolus
# day. Every one of the 14 calendar days must be an autobolus day, so a day
# with no Loop dosing data fails the window (MC, 2026-10-30). Until then a
# window passed when every day that had dosing data was an autobolus day,
# however few such days there were, and it could start 28 days after the
# first autobolus day.
SEGMENT_DAYS = 14
MIN_DAYS_AFTER_FIRST_AUTOBOLUS = 30


def run(
    spark,
    output_table=f"{CATALOG}.stable_autobolus_segments",
    loop_recommendations_table=f"{CATALOG}.loop_recommendations",
    user_dates_table="dev.default.bddp_user_dates",
    user_gender_table="dev.default.user_gender",
    min_autobolus_count=3,
):
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS

    WITH daily_flags AS (
      SELECT
        _userId,
        day,
        CASE
          WHEN GREATEST(
            COALESCE(dd_autobolus_count, 0),
            COALESCE(hk_autobolus_count, 0)
          ) >= {min_autobolus_count}
          THEN 1 ELSE 0
        END AS is_autobolus
      FROM {loop_recommendations_table}
    ),

    first_autobolus AS (
      SELECT
        _userId,
        MIN(day) AS first_ab_day
      FROM daily_flags
      WHERE is_autobolus = 1
      GROUP BY _userId
    ),

    sliding_window AS (
      SELECT
        df._userId,
        df.day AS segment_end,
        DATE_SUB(df.day, {SEGMENT_DAYS - 1}) AS segment_start,
        fa.first_ab_day,

        SUM(df.is_autobolus) OVER w AS autobolus_days,
        COUNT(*) OVER w AS days_with_data

      FROM daily_flags df
      LEFT JOIN first_autobolus fa ON df._userId = fa._userId
      WINDOW w AS (
        PARTITION BY df._userId
        ORDER BY df.day
        RANGE BETWEEN INTERVAL {SEGMENT_DAYS - 1} DAYS PRECEDING AND CURRENT ROW
      )
    ),

    scored AS (
      SELECT
        s._userId,
        s.segment_start,
        s.segment_end,
        s.first_ab_day,
        s.autobolus_days,
        s.days_with_data,
        ROUND(s.days_with_data * 1.0 / {SEGMENT_DAYS}, 3) AS coverage,
        ROUND(s.autobolus_days * 1.0 / s.days_with_data, 4) AS autobolus_pct,
        DATEDIFF(s.segment_start, s.first_ab_day) AS days_since_first_ab

      FROM sliding_window s
    )

    SELECT
      sc._userId,
      sc.segment_start,
      sc.segment_end,
      sc.first_ab_day,
      sc.days_since_first_ab,
      sc.coverage,
      sc.autobolus_pct,
      sc.days_with_data,

      d.dob,
      d.diagnosis_date,
      g.gender,
      ROUND(DATEDIFF(sc.segment_start, d.dob) / 365.25, 1) AS age_years,
      ROUND(DATEDIFF(sc.segment_start, d.diagnosis_date) / 365.25, 1) AS years_lwd

    FROM scored sc
    LEFT JOIN {user_dates_table} d ON sc._userId = d.userid
    LEFT JOIN {user_gender_table} g ON sc._userId = g.userid
    -- Every calendar day of the window is an autobolus day (loop_recommendations
    -- holds one row per user-day, so 14 autobolus rows in a 14-day range are 14
    -- days), and the window starts at least 30 days after the first autobolus day.
    WHERE sc.autobolus_days = {SEGMENT_DAYS}
      AND sc.days_since_first_ab >= {MIN_DAYS_AFTER_FIRST_AUTOBOLUS}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sc._userId ORDER BY sc.segment_start ASC) <= 1
    ORDER BY sc._userId, sc.segment_start
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
