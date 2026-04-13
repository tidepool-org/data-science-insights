"""Compare three methods of identifying autobolus user-days.

Method 1: bolus subType = 'automated'
Method 2: recommendedBolus IS NOT NULL
Method 3: bolus matched to dosingDecision with reason='loop' within 30 seconds
"""

import pandas as pd


SOURCE_TABLE = "dev.default.bddp_sample_all_2"


def get_automated_subtype_days(spark, input_table=SOURCE_TABLE):
    """User-days with at least one bolus where subType = 'automated'."""
    return spark.sql(f"""
        SELECT DISTINCT
            `_userId`,
            LEFT(time_string, 10) AS day
        FROM {input_table}
        WHERE type = 'bolus'
          AND subType = 'automated'
    """)


def get_recommended_bolus_days(spark, input_table=SOURCE_TABLE):
    """User-days with at least one record where recommendedBolus IS NOT NULL."""
    return spark.sql(f"""
        SELECT DISTINCT
            `_userId`,
            LEFT(time_string, 10) AS day
        FROM {input_table}
        WHERE recommendedBolus IS NOT NULL
    """)


def get_loop_decision_matched_days(spark, input_table=SOURCE_TABLE):
    """User-days with a bolus matched to a dosingDecision with reason='loop' within 30s."""
    return spark.sql(f"""
        WITH boluses AS (
            SELECT
                `_userId`,
                time_string,
                TRY_CAST(time_string AS TIMESTAMP) AS bolus_ts
            FROM {input_table}
            WHERE type = 'bolus'
        ),
        loop_decisions AS (
            SELECT
                `_userId`,
                TRY_CAST(time_string AS TIMESTAMP) AS dd_ts
            FROM {input_table}
            WHERE type = 'dosingDecision'
              AND reason = 'loop'
        )
        SELECT DISTINCT
            b.`_userId`,
            LEFT(b.time_string, 10) AS day
        FROM boluses b
        INNER JOIN loop_decisions dd
            ON b.`_userId` = dd.`_userId`
            AND TIMESTAMPDIFF(SECOND, dd.dd_ts, b.bolus_ts) BETWEEN 0 AND 30
    """)


def compare_methods(spark, input_table=SOURCE_TABLE):
    automated = get_automated_subtype_days(spark, input_table)
    recommended = get_recommended_bolus_days(spark, input_table)
    loop_matched = get_loop_decision_matched_days(spark, input_table)

    automated.createOrReplaceTempView("method_automated")
    recommended.createOrReplaceTempView("method_recommended")
    loop_matched.createOrReplaceTempView("method_loop_matched")

    comparison = spark.sql("""
        SELECT
            COALESCE(a._userId, r._userId, l._userId) AS _userId,
            COALESCE(a.day, r.day, l.day) AS day,
            a.day IS NOT NULL AS has_automated_subtype,
            r.day IS NOT NULL AS has_recommended_bolus,
            l.day IS NOT NULL AS has_loop_decision_match
        FROM method_automated a
        FULL OUTER JOIN method_recommended r
            ON a._userId = r._userId AND a.day = r.day
        FULL OUTER JOIN method_loop_matched l
            ON COALESCE(a._userId, r._userId) = l._userId
            AND COALESCE(a.day, r.day) = l.day
    """).toPandas()

    print_report(comparison)
    return comparison


def print_report(df):
    total = len(df)
    all_agree = (
        df["has_automated_subtype"]
        & df["has_recommended_bolus"]
        & df["has_loop_decision_match"]
    ).sum()

    print(f"Total unique user-days across all methods: {total}")
    print(f"User-days where all 3 methods agree:       {all_agree} ({all_agree / total * 100:.1f}%)")
    print()

    # Per-method counts
    n_auto = df["has_automated_subtype"].sum()
    n_rec = df["has_recommended_bolus"].sum()
    n_loop = df["has_loop_decision_match"].sum()
    print(f"Method 1 (subType='automated'):        {n_auto}")
    print(f"Method 2 (recommendedBolus NOT NULL):   {n_rec}")
    print(f"Method 3 (loop decision match):         {n_loop}")
    print()

    # Pairwise overlap
    auto_and_rec = (df["has_automated_subtype"] & df["has_recommended_bolus"]).sum()
    auto_and_loop = (df["has_automated_subtype"] & df["has_loop_decision_match"]).sum()
    rec_and_loop = (df["has_recommended_bolus"] & df["has_loop_decision_match"]).sum()
    print("Pairwise overlap:")
    print(f"  automated & recommended:   {auto_and_rec}")
    print(f"  automated & loop_matched:  {auto_and_loop}")
    print(f"  recommended & loop_matched: {rec_and_loop}")
    print()

    # Exclusive to each method
    only_auto = (df["has_automated_subtype"] & ~df["has_recommended_bolus"] & ~df["has_loop_decision_match"]).sum()
    only_rec = (~df["has_automated_subtype"] & df["has_recommended_bolus"] & ~df["has_loop_decision_match"]).sum()
    only_loop = (~df["has_automated_subtype"] & ~df["has_recommended_bolus"] & df["has_loop_decision_match"]).sum()
    print("Exclusive to one method only:")
    print(f"  Only automated:     {only_auto}")
    print(f"  Only recommended:   {only_rec}")
    print(f"  Only loop_matched:  {only_loop}")
    print()

    # Disagreement breakdown
    combos = df.groupby([
        "has_automated_subtype",
        "has_recommended_bolus",
        "has_loop_decision_match",
    ]).size().reset_index(name="count").sort_values("count", ascending=False)
    print("Full combination breakdown:")
    print(combos.to_string(index=False))


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    comparison = compare_methods(spark)
