CATALOG = "dev.fda_510k_rwd"


def run(
    spark,
    output_table=f"{CATALOG}.stable_autobolus_segments",
    loop_recommendations_table=f"{CATALOG}.loop_recommendations",
    user_dates_table="dev.default.bddp_user_dates",
    user_gender_table="dev.default.user_gender",
    min_autobolus_count=3,
):
    spark.sql(f"""
    CREATE OR REPLACE TABLE {output_table} AS

    WITH params AS (
      SELECT
        14 AS segment_days,
        0.70 AS min_coverage
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
        DATE_SUB(df.day, 13) AS segment_start,
        fa.first_ab_day,

        SUM(df.is_autobolus) OVER w AS autobolus_days,
        COUNT(*) OVER w AS days_with_data

      FROM daily_flags df
      LEFT JOIN first_autobolus fa ON df._userId = fa._userId
      WINDOW w AS (
        PARTITION BY df._userId
        ORDER BY df.day
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
        ROUND(s.days_with_data * 1.0 / p.segment_days, 3) AS coverage,
        ROUND(s.autobolus_days * 1.0 / s.days_with_data, 4) AS autobolus_pct,
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
    WHERE sc.autobolus_pct = 1.0
      AND sc.days_since_first_ab >= 28
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sc._userId ORDER BY sc.segment_start ASC) <= 1 
    ORDER BY sc._userId, sc.segment_start
    """) 


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
