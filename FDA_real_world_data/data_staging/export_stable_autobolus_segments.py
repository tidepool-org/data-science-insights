import argparse


CATALOG = "dev.fda_510k_rwd"


def run(
    spark,
    output_table=f"{CATALOG}.stable_autobolus_segments",
    loop_recommendations_table=f"{CATALOG}.loop_recommendations",
    user_dates_table="dev.default.bddp_user_dates",
    user_gender_table="dev.default.user_gender",
):
    spark.sql(f"""
    CREATE OR REPLACE TABLE {output_table} AS

    WITH params AS (
      SELECT
        14 AS segment_days,
        0.70 AS min_coverage,
        288 AS samples_per_day,
        288 * 14 AS samples_per_segment
    ),

    first_autobolus AS (
      SELECT
        _userId,
        MIN(day) AS first_ab_day
      FROM {loop_recommendations_table}
      WHERE is_autobolus = 1
      GROUP BY _userId
    ),

    daily_agg AS (
      SELECT
        b._userId,
        b.day,
        f.first_ab_day,
        SUM(b.is_autobolus) AS autobolus_rows,
        COUNT(b.is_autobolus) AS recommendation_rows,
        COUNT(*) AS total_rows
      FROM {loop_recommendations_table} b
      LEFT JOIN first_autobolus f ON b._userId = f._userId
      GROUP BY b._userId, b.day, f.first_ab_day
    ),

    sliding_window AS (
      SELECT
        _userId,
        day AS segment_end,
        DATE_SUB(day, 13) AS segment_start,
        first_ab_day,

        SUM(autobolus_rows) OVER w AS autobolus_total,
        SUM(recommendation_rows) OVER w AS rec_total,
        SUM(total_rows) OVER w AS rows_total,
        COUNT(*) OVER w AS days_with_data

      FROM daily_agg
      WINDOW w AS (
        PARTITION BY _userId
        ORDER BY day
        RANGE BETWEEN INTERVAL 13 DAYS PRECEDING AND CURRENT ROW
      )
    ),

    scored AS (
      SELECT
        s._userId,
        s.segment_start,
        s.segment_end,
        s.first_ab_day,
        s.days_with_data,
        ROUND(s.rows_total * 1.0 / p.samples_per_segment, 3) AS coverage,
        ROUND(s.autobolus_total * 1.0 / NULLIF(s.rec_total, 0), 4) AS autobolus_pct,
        DATEDIFF(s.segment_start, s.first_ab_day) AS days_since_first_ab,
        ROW_NUMBER() OVER (PARTITION BY s._userId ORDER BY s.segment_start ASC) AS rn

      FROM sliding_window s
      CROSS JOIN params p
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
      sc.rn,

      d.dob,
      d.diagnosis_date,
      g.gender,
      ROUND(DATEDIFF(sc.segment_start, d.dob) / 365.25, 1) AS age_years,
      ROUND(DATEDIFF(sc.segment_start, d.diagnosis_date) / 365.25, 1) AS years_lwd

    FROM scored sc
    LEFT JOIN {user_dates_table} d ON sc._userId = d.userid
    LEFT JOIN {user_gender_table} g ON sc._userId = g.userid
    ORDER BY sc._userId, sc.segment_start
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
