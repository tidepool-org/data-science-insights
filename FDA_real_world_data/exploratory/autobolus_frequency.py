import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd


def get_autobolus_counts(spark):
    return spark.sql("""
    WITH recommendation_deduped AS (
        SELECT DISTINCT
            `_userId`,
            time_string,
            recommendedBolus,
            recommendedBasal
        FROM bddp_sample_all_2
        WHERE reason = 'loop'
            AND (recommendedBolus IS NOT NULL OR recommendedBasal IS NOT NULL)
    ),

    direct_autobolus AS (
        SELECT
            `_userId`,
            LEFT(time_string, 10) AS day,
            SUM(CASE WHEN recommendedBolus IS NOT NULL THEN 1 ELSE 0 END) AS autobolus_count
        FROM recommendation_deduped
        GROUP BY `_userId`, LEFT(time_string, 10)
    ),

    loop_user_days AS (
        SELECT DISTINCT
            `_userId`,
            LEFT(time_string, 10) AS day
        FROM bddp_sample_all_2
        WHERE get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
    ),

    healthkit_deduped AS (
        SELECT DISTINCT
            `_userId`,
            time_string
        FROM bddp_sample_all_2
        WHERE type = 'bolus'
            AND CAST(
                get_json_object(payload, "$['com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued']")
                AS DOUBLE
            ) = 1
    ),

    healthkit_daily AS (
        SELECT
            `_userId`,
            LEFT(time_string, 10) AS day,
            COUNT(*) AS autobolus_count
        FROM healthkit_deduped
        GROUP BY `_userId`, LEFT(time_string, 10)
    ),

    healthkit_autobolus AS (
        SELECT
            l.`_userId`,
            l.day,
            COALESCE(d.autobolus_count, 0) AS autobolus_count
        FROM loop_user_days l
        LEFT JOIN healthkit_daily d
            ON l.`_userId` = d.`_userId`
            AND l.day = d.day
    )

    SELECT 'direct' AS source, autobolus_count FROM direct_autobolus WHERE autobolus_count <= 288
    UNION ALL
    SELECT 'healthkit' AS source, autobolus_count FROM healthkit_autobolus WHERE autobolus_count <= 288
    """).toPandas()


def get_duplicate_counts(spark):
    return spark.sql("""
    WITH direct_dups AS (
        SELECT
            `_userId`,
            time_string,
            COUNT(*) AS n
        FROM bddp_sample_all_2
        WHERE reason = 'loop'
            AND (recommendedBolus IS NOT NULL OR recommendedBasal IS NOT NULL)
        GROUP BY `_userId`, time_string
        HAVING COUNT(*) > 1
    ),

    healthkit_dups AS (
        SELECT
            `_userId`,
            time_string,
            COUNT(*) AS n
        FROM bddp_sample_all_2
        WHERE type = 'bolus'
            AND CAST(
                get_json_object(payload, "$['com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued']")
                AS DOUBLE
            ) = 1
        GROUP BY `_userId`, time_string
        HAVING COUNT(*) > 1
    )

    SELECT 'direct' AS source, COUNT(*) AS dup_timestamps, SUM(n) AS dup_rows FROM direct_dups
    UNION ALL
    SELECT 'healthkit' AS source, COUNT(*) AS dup_timestamps, SUM(n) AS dup_rows FROM healthkit_dups
    """).toPandas()


def get_users_per_day(spark):
    return spark.sql("""
    WITH recommendation_deduped AS (
        SELECT DISTINCT
            `_userId`,
            time_string,
            recommendedBolus
        FROM bddp_sample_all_2
        WHERE reason = 'loop'
            AND recommendedBolus IS NOT NULL
    ),

    direct_daily AS (
        SELECT
            LEFT(time_string, 10) AS day,
            COUNT(DISTINCT `_userId`) AS n_users
        FROM recommendation_deduped
        GROUP BY LEFT(time_string, 10)
    ),

    healthkit_deduped AS (
        SELECT DISTINCT
            `_userId`,
            time_string
        FROM bddp_sample_all_2
        WHERE type = 'bolus'
            AND CAST(
                get_json_object(payload, "$['com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued']")
                AS DOUBLE
            ) = 1
    ),

    healthkit_daily AS (
        SELECT
            LEFT(time_string, 10) AS day,
            COUNT(DISTINCT `_userId`) AS n_users
        FROM healthkit_deduped
        GROUP BY LEFT(time_string, 10)
    )

    SELECT 'direct' AS source, day, n_users FROM direct_daily
    UNION ALL
    SELECT 'healthkit' AS source, day, n_users FROM healthkit_daily
    """).toPandas()


def plot_histograms(df):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    direct = df[df["source"] == "direct"]["autobolus_count"]
    healthkit = df[df["source"] == "healthkit"]["autobolus_count"]

    axes[0].hist(direct, bins=50, edgecolor="black")
    axes[0].set_title("Autobolus per Day (direct)")
    axes[0].set_xlabel("Autobolus count per user-day")
    axes[0].set_ylabel("Frequency")

    axes[1].hist(healthkit, bins=50, edgecolor="black")
    axes[1].set_title("Autobolus per Day (healthkit)")
    axes[1].set_xlabel("Autobolus count per user-day")

    plt.tight_layout()
    plt.show()


def plot_users_per_day(df):
    df = df.copy()
    df["day"] = pd.to_datetime(df["day"])

    fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

    for source in ["direct", "healthkit"]:
        subset = df[df["source"] == source].sort_values("day")
        axes[0].plot(subset["day"].values, subset["n_users"].values, label=source)

    total = df.groupby("day")["n_users"].sum().reset_index().sort_values("day")
    axes[1].plot(total["day"].values, total["n_users"].values, label="total", color="green")

    axes[0].set_title("Users with Autobolus per Calendar Day")
    axes[0].set_ylabel("Number of users")
    axes[0].legend()

    axes[1].set_title("Total Users per Calendar Day (direct + healthkit)")
    axes[1].set_xlabel("Day")
    axes[1].set_ylabel("Number of users")
    axes[1].legend()

    axes[1].xaxis.set_major_locator(mdates.MonthLocator())
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate(rotation=45)
    plt.tight_layout()
    plt.show()


def run(spark):
    # dup_df = get_duplicate_counts(spark)
    # print("Duplicate timestamps (same user + same time_string):")
    # print(dup_df.to_string(index=False))

    # counts_df = get_autobolus_counts(spark)
    # plot_histograms(counts_df)

    users_df = get_users_per_day(spark)
    plot_users_per_day(users_df)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
