import argparse


CATALOG = "dev.fda_510k_rwd"


def run(
    spark,
    output_table=f"{CATALOG}.autobolus_durability",
    loop_recommendations_table=f"{CATALOG}.loop_recommendations",
    user_dates_table="dev.default.bddp_user_dates",
    user_gender_table="dev.default.user_gender",
):
    spark.sql(f"""
    CREATE OR REPLACE TABLE {output_table} AS

    WITH
    params AS (
      SELECT
        0.80 AS adoption_threshold,
        3 AS adoption_window_days,
        56 AS min_followup_days,
        28 AS final_period_days,
        0.80 AS discontinuation_threshold,
        0.70 AS min_coverage,
        288 AS samples_per_day,
        288 * 28 AS samples_per_final_period
    ),

    daily_agg AS (
      SELECT
        _userId,
        day,
        SUM(is_autobolus) AS autobolus_rows,
        COUNT(is_autobolus) AS recommendation_rows,
        COUNT(*) AS total_rows
      FROM {loop_recommendations_table}
      GROUP BY _userId, day
    ),

    user_bounds AS (
      SELECT
        _userId,
        MIN(day) AS first_day,
        MAX(day) AS last_day
      FROM daily_agg
      GROUP BY _userId
    ),

    rolling_adoption AS (
      SELECT
        d._userId,
        d.day,
        u.last_day,

        SUM(d.autobolus_rows) OVER w AS ab_in_window,
        SUM(d.recommendation_rows) OVER w AS rec_in_window,
        COUNT(*) OVER w AS days_with_data

      FROM daily_agg d
      JOIN user_bounds u ON d._userId = u._userId
      WINDOW w AS (
        PARTITION BY d._userId
        ORDER BY d.day
        RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW
      )
    ),

    adoption AS (
      SELECT
        _userId,
        DATE_SUB(MIN(day), 2) AS adoption_date,
        last_day
      FROM rolling_adoption
      CROSS JOIN params p
      WHERE
        days_with_data = 3
        AND rec_in_window > 0
        AND ab_in_window * 1.0 / rec_in_window >= p.adoption_threshold
      GROUP BY _userId, last_day
    ),

    post_adoption_usage AS (
      SELECT
        a._userId,
        SUM(
          CASE
            WHEN d.recommendation_rows > 0
             AND d.autobolus_rows * 1.0 / d.recommendation_rows > 0.50
            THEN 1
            ELSE 0
          END
        ) AS autobolus_days
      FROM adoption a
      JOIN daily_agg d
        ON a._userId = d._userId
        AND d.day BETWEEN a.adoption_date AND a.last_day
      GROUP BY a._userId
    ),

    final_period AS (
      SELECT
        a._userId,
        SUM(d.autobolus_rows) AS final_autobolus_rows,
        SUM(d.recommendation_rows) AS final_rec_rows,
        SUM(d.total_rows) AS final_total_rows,
        COUNT(*) AS final_days_with_data
      FROM adoption a
      JOIN daily_agg d
        ON a._userId = d._userId
        AND d.day BETWEEN DATE_SUB(a.last_day, 27) AND a.last_day
      GROUP BY a._userId
    )

    SELECT
      u._userId,
      u.first_day,
      u.last_day,

      a.adoption_date,
      a.adoption_date IS NOT NULL AS is_adopted,

      DATEDIFF(u.last_day, a.adoption_date) AS days_post_adoption,
      ROUND(DATEDIFF(u.last_day, a.adoption_date) / 7.0, 1) AS weeks_post_adoption,
      DATEDIFF(u.last_day, a.adoption_date) >= p.min_followup_days AS has_min_followup,

      f.final_days_with_data,
      ROUND(f.final_total_rows * 1.0 / p.samples_per_final_period, 3) AS final_period_coverage,
      f.final_rec_rows > 0
        AND f.final_total_rows * 1.0 / p.samples_per_final_period >= p.min_coverage
        AS has_final_coverage,
      ROUND(f.final_autobolus_rows * 1.0 / NULLIF(f.final_rec_rows, 0), 4) AS final_autobolus_pct,
      CASE
        WHEN f.final_rec_rows > 0
         AND f.final_autobolus_rows * 1.0 / NULLIF(f.final_rec_rows, 0) <= (1 - p.discontinuation_threshold)
        THEN 1
        WHEN f.final_rec_rows > 0
        THEN 0
        ELSE NULL
      END AS is_discontinued,

      pau.autobolus_days,

      dd.dob,
      dd.diagnosis_date,
      g.gender,
      ROUND(DATEDIFF(a.adoption_date, dd.dob) / 365.25, 1) AS age_at_adoption,
      ROUND(DATEDIFF(a.adoption_date, dd.diagnosis_date) / 365.25, 1) AS years_lwd_at_adoption,
      ROUND(DATEDIFF(a.adoption_date, dd.dob) / 365.25, 1) > 6 OR dd.dob IS NULL AS is_age_eligible

    FROM user_bounds u
    CROSS JOIN params p
    LEFT JOIN adoption a ON u._userId = a._userId
    LEFT JOIN post_adoption_usage pau ON u._userId = pau._userId
    LEFT JOIN final_period f ON u._userId = f._userId
    LEFT JOIN {user_dates_table} dd ON u._userId = dd.userid
    LEFT JOIN {user_gender_table} g ON u._userId = g.userid
    ORDER BY u._userId
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
