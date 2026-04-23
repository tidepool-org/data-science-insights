import argparse


CATALOG = "dev.fda_510k_rwd"


def run(
    spark,
    output_table=f"{CATALOG}.autobolus_event_times",
    durability_table=f"{CATALOG}.autobolus_durability",
    loop_recommendations_table=f"{CATALOG}.loop_recommendations",
    min_autobolus_count=3,
):
    spark.sql(f"""
    CREATE OR REPLACE TABLE {output_table} AS

    WITH
    params AS (
      SELECT
        0.20 AS discontinuation_threshold,
        4 AS trailing_weeks
    ),

    daily_flags AS (
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

    weekly_usage AS (
      SELECT
        d._userId,
        CAST(FLOOR(DATEDIFF(df.day, d.adoption_date) / 7) AS INT)
            AS week_post_adoption,
        SUM(df.is_autobolus) * 1.0 / NULLIF(COUNT(*), 0)
            AS autobolus_pct
      FROM {durability_table} d
      JOIN daily_flags df
        ON d._userId = df._userId
        AND df.day >= d.adoption_date
      GROUP BY d._userId,
               CAST(FLOOR(DATEDIFF(df.day, d.adoption_date) / 7) AS INT)
    ),

    trailing_avg AS (
      SELECT
        _userId,
        week_post_adoption,
        autobolus_pct,
        AVG(autobolus_pct) OVER (
          PARTITION BY _userId
          ORDER BY week_post_adoption
          ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS trailing_ab_pct,
        COUNT(*) OVER (
          PARTITION BY _userId
          ORDER BY week_post_adoption
          ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS weeks_in_window
      FROM weekly_usage
    ),

    permanent_check AS (
      SELECT
        _userId,
        week_post_adoption,
        autobolus_pct,
        trailing_ab_pct,
        weeks_in_window,
        weeks_in_window = p.trailing_weeks AS is_window_complete,
        MAX(trailing_ab_pct) OVER (
          PARTITION BY _userId
          ORDER BY week_post_adoption
          ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS max_trailing_from_here
      FROM trailing_avg
      CROSS JOIN params p
    ),

    events AS (
      SELECT
        _userId,
        MIN(week_post_adoption) AS event_week
      FROM permanent_check
      CROSS JOIN params p
      WHERE is_window_complete
        AND max_trailing_from_here <= p.discontinuation_threshold
      GROUP BY _userId
    )

    SELECT
      pc._userId,
      pc.week_post_adoption,
      pc.autobolus_pct,
      pc.trailing_ab_pct,
      pc.weeks_in_window,
      pc.is_window_complete,
      COALESCE(pc.week_post_adoption = e.event_week, FALSE) AS is_event_week,
      e.event_week IS NOT NULL AS has_discontinuation,
      e.event_week
    FROM permanent_check pc
    LEFT JOIN events e ON pc._userId = e._userId
    ORDER BY pc._userId, pc.week_post_adoption
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
