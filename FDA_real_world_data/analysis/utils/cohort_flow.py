"""Cohort flow tables counted from a cohort table's criterion columns.

Execution plan 2026-10-27, Step 3 item 2: one flow table per analysis
population, counted from the staged cohort table's boolean columns so each
stage applies exactly the predicate the analysis applies, split by diagnosis
group (MC, 2026-10-29), and written for the corrected vintage beside the
issued one (item 3).

A flow is a list of stages. Each stage names itself, describes its criterion
in the words the report uses, and gives the criterion as a function of the
cohort frame returning a boolean Series. Stages are cumulative: a row counts
at a stage when it passes that stage and every stage before it. The counts
are users (distinct `_userId`) and units (rows: windows, activations,
segments) per diagnosis group, plus two totals, `all` (every group) and
`primary` (the type-1 cohort, cohort_constants.PRIMARY_DIAGNOSIS_GROUPS).

The functions are pure pandas so the offline test can pin them on a
hand-built frame (testing/analysis/test_cohort_flow.py).
"""

from collections import namedtuple

import pandas as pd

from .data_loading import DIAGNOSIS_GROUPS, PRIMARY_DIAGNOSIS_GROUPS

# A stage of a flow: the name a table prints, the criterion in words, and the
# criterion as a function of the cohort frame (a boolean Series).
Stage = namedtuple("Stage", ["name", "description", "criterion"])

# The two total columns beside the diagnosis groups.
ALL_GROUPS_LABEL = "all"
PRIMARY_LABEL = "primary"
GROUP_COLUMN = "diagnosis_group"
USER_COLUMN = "_userId"


def every_row(frame):
    """The first stage of a flow: every row of the cohort table."""
    return pd.Series(True, index=frame.index)


def count_flow(frame, stages, unit="rows"):
    """Count a cumulative flow over `frame`, one row per stage and group.

    Returns a long frame with columns `stage`, `description`, `group`,
    `n_users`, and `n_<unit>` (the rows passing), for every diagnosis group
    in DIAGNOSIS_GROUPS plus `all` and `primary`. A group with no rows at a
    stage has zeros, never a missing row, so the table's shape is fixed.
    """
    assert GROUP_COLUMN in frame.columns, f"the cohort frame carries no {GROUP_COLUMN}"
    assert USER_COLUMN in frame.columns, f"the cohort frame carries no {USER_COLUMN}"
    # A population counted in users has one count column, not two.
    unit_column = f"n_{unit}"
    columns = ["stage", "description", "group", "n_users"] + ([unit_column] if unit_column != "n_users" else [])
    keep = pd.Series(True, index=frame.index)
    rows = []
    for stage in stages:
        criterion = stage.criterion(frame).fillna(False).astype(bool)
        keep = keep & criterion
        passing = frame.loc[keep]
        memberships = [(ALL_GROUPS_LABEL, pd.Series(True, index=passing.index)),
                       (PRIMARY_LABEL, passing[GROUP_COLUMN].isin(PRIMARY_DIAGNOSIS_GROUPS))]
        memberships += [(group, passing[GROUP_COLUMN] == group) for group in DIAGNOSIS_GROUPS]
        for label, member in memberships:
            subset = passing.loc[member]
            rows.append({
                "stage": stage.name,
                "description": stage.description,
                "group": label,
                "n_users": int(subset[USER_COLUMN].nunique()),
                unit_column: int(len(subset)),
            })
    return pd.DataFrame(rows, columns=columns)


def widen_flow(long, value="n_users"):
    """One row per stage, one column per group, in flow order."""
    order = [ALL_GROUPS_LABEL, PRIMARY_LABEL, *DIAGNOSIS_GROUPS]
    stages = long.drop_duplicates("stage")[["stage", "description"]].reset_index(drop=True)
    wide = long.pivot(index="stage", columns="group", values=value).reindex(columns=order)
    wide = wide.reindex(stages["stage"]).reset_index()
    wide.insert(1, "description", stages["description"].values)
    wide.columns.name = None
    return wide


def side_by_side(current_long, issued_long, value="n_users", group=PRIMARY_LABEL):
    """One group's counts at every stage, corrected vintage beside the issued
    one, with the difference; a stage one vintage lacks is left blank.

    Execution plan Step 3 item 3: the flow shows what moved before any
    statistic is computed.
    """
    current = current_long.loc[current_long["group"] == group, ["stage", "description", value]]
    current = current.rename(columns={value: f"{value}_current"})
    if issued_long is None:
        current[f"{value}_issued"] = pd.NA
    else:
        issued = issued_long.loc[issued_long["group"] == group, ["stage", value]]
        issued = issued.rename(columns={value: f"{value}_issued"})
        current = current.merge(issued, on="stage", how="left")
    current[f"{value}_issued"] = current[f"{value}_issued"].astype("Int64")
    current["difference"] = (current[f"{value}_current"] - current[f"{value}_issued"]).astype("Int64")
    return current.reset_index(drop=True)


def assert_monotone(long, value="n_users"):
    """A flow only ever narrows: every group's count is non-increasing down
    the stages. A rise means a stage's criterion is not a subset of the
    previous ones, or the stages are out of order."""
    for group, counts in long.groupby("group", sort=False)[value]:
        values = counts.tolist()
        for previous, current in zip(values, values[1:]):
            assert current <= previous, f"flow not monotone for group {group}: {values}"


# ---------------------------------------------------------------------------
# The stages of each PLN-1001 population, from its cohort table's columns
# ---------------------------------------------------------------------------

def transition_stages(primary_gate=False):
    """The transition cohort's stages, in the order of the issued Table 6.3a.

    `primary_gate=True` inserts the "Type 1 diabetes" stage the issued table
    carries (the report's funnel narrows to the primary cohort there); the
    by-group flow leaves it out, since the groups are its columns.
    """
    stages = [
        Stage("Valid TB→AB transition segment",
              "At least the build's threshold of the 14 days of each period of the right kind "
              "(temp-basal days, then autobolus days; 12 for the primary build); staging age "
              "gate (> 6 y at segment start or DOB unknown)",
              every_row),
        Stage("Glycemic endpoints computed",
              "Segments with glycemic endpoints computed for ≥1 of the two 14-day periods "
              "(≥1 CGM reading in the period)",
              lambda f: f["has_endpoints_seg1"] | f["has_endpoints_seg2"]),
        Stage("Analysis cohort gate",
              "Known Loop version below 3.4.0, or unknown version and segment ending before "
              "2024-07-13; age ≥ 6 y at segment start or unknown",
              lambda f: f["passes_loop_version_gate"] & f["passes_age_gate"]),
    ]
    if primary_gate:
        stages.append(Stage("Type 1 diabetes",
                            "Diagnosis group type 1 (a JAEB member whose profile reads LADA included)",
                            lambda f: f["is_primary_diagnosis"]))
    stages += [
        Stage("CGM coverage filter",
              "≥ 70% of expected 5-minute CGM samples in the 14-day period (≥ 2,822 readings); "
              "segments keep counting here while ≥1 period survives",
              lambda f: (f["has_endpoints_seg1"] & f["passes_cgm_coverage_seg1"])
              | (f["has_endpoints_seg2"] & f["passes_cgm_coverage_seg2"])),
        Stage("Guardrail exclusion",
              "No pump-settings guardrail violation during the segment",
              lambda f: f["no_guardrail_violation"]),
        Stage("Paired TB/AB halves",
              "Both the temp-basal and autobolus 14-day periods pass all per-period filters",
              lambda f: f["has_endpoints_seg1"] & f["passes_cgm_coverage_seg1"]
              & f["has_endpoints_seg2"] & f["passes_cgm_coverage_seg2"]),
        Stage("Final transition cohort",
              "One segment per user, the lowest segment rank among survivors",
              lambda f: f["is_best_window"]),
    ]
    return stages


def carbohydrate_stages():
    """Analysis 8-8's population: the transition cohort with carbohydrate data
    in both periods (cohort_constants.CARB_ENTRY_MIN_GRAMS to _MAX_GRAMS)."""
    return transition_stages() + [
        Stage("Carbohydrate data in both periods",
              "At least one plausible food entry (1 to 150 g) in each 14-day period",
              lambda f: f["has_carb_data_seg1"] & f["has_carb_data_seg2"]),
    ]


def activation_stages(ab_segment="seg2"):
    """The preset-activation population (8-2, 8-3, 8-4, IR-1), counted in
    activations; `ab_segment` picks which autobolus period pairs with the
    temp-basal period for the paired-outcome stages."""
    assert ab_segment in ("seg2", "seg3"), ab_segment
    return [
        Stage("Preset activations in rank-1 windows",
              "Every preset activation in a user's rank-1 transition window, any period",
              every_row),
        Stage("Window in the preset cohort",
              "The window passes the Loop version gate, the age gate, and the guardrail check",
              lambda f: f["window_in_preset_cohort"]),
        Stage("Activation in the compared periods",
              f"The activation falls in the temp-basal period or the {ab_segment} autobolus period",
              lambda f: f["is_in_period_seg1"] | f[f"is_in_period_{ab_segment}"]),
        Stage("Starting glucose in range",
              "A CGM reading within 30 minutes before activation between 70 and 180 mg/dL",
              lambda f: f["is_starting_glucose_in_range"]),
        Stage("Preset valid by name in both periods",
              f"At least two activations of the preset name in the temp-basal period and in the "
              f"{ab_segment} autobolus period",
              lambda f: f[f"is_valid_name_only_{ab_segment}"]),
    ]


def stable_stages(with_guardrail=False):
    """Analysis 8-6's population; `with_guardrail=True` adds the guardrail
    check 8-6 does not apply, before the JAEB stage, to show what it would
    remove (docs/pipeline_map.md section 8, item 6)."""
    stages = [
        Stage("Stable-autobolus segment found",
              "The earliest 14-day window of autobolus days starting at least 30 days after the "
              "user's first autobolus day",
              every_row),
        Stage("Glycemic endpoints computed",
              "A glycemic-endpoints row for the segment (≥1 CGM reading)",
              lambda f: f["has_endpoints"]),
        Stage("CGM coverage filter",
              "≥ 70% of expected 5-minute CGM samples in the 14-day segment (≥ 2,822 readings)",
              lambda f: f["passes_cgm_coverage"]),
        Stage("Age gate",
              "Age ≥ 6 y at segment start or unknown",
              lambda f: f["passes_age_gate"]),
    ]
    if with_guardrail:
        stages.append(Stage("No guardrail violation (not applied by analysis 8-6)",
                            "No pump-settings guardrail violation during the segment",
                            lambda f: f["no_guardrail_violation"]))
    stages.append(Stage("JAEB study linkage",
                        "The user carries a JAEB participant identifier (the socioeconomic hand-off)",
                        lambda f: f["is_jaeb_linked"]))
    return stages


def ab_day_stages(primary_gate=False):
    """The autobolus-day population of IR-1002 (IR-2 and IR-3), counted on
    ab_day_cohort's user-days: a user counts at a stage with at least one
    such day, the issued Table IR-2a's stages."""
    stages = [
        Stage("Loop users in extract",
              "Every user with a day in the day table",
              every_row),
    ]
    if primary_gate:
        stages.append(Stage("Type 1 diagnosis",
                            "Diagnosis group type 1 (a JAEB member whose profile reads LADA included)",
                            lambda f: f["is_primary_diagnosis"]))
    stages += [
        Stage("≥1 eligible AB day (AB × version/date × age)",
              "An autobolus day (≥ 3 automated boluses) on a version- or date-eligible day at age ≥ 6",
              lambda f: f["is_eligible_ab_day"]),
        Stage("≥1 outcome-eligible (≥70% CGM-coverage) AB day",
              "An eligible AB day with ≥ 201 plausible CGM readings",
              lambda f: f["is_outcome_day"]),
    ]
    return stages


def extreme_preset_stages():
    """IR-6's population, counted on extreme_preset_cohort's user-periods."""
    return [
        Stage("User-periods of extreme-preset users",
              "The three periods of every preset-cohort user with an extreme activation in the window",
              every_row),
        Stage("CGM present in the period",
              "At least one plausible CGM reading in the period",
              lambda f: f["has_cgm"]),
        Stage("CGM coverage filter",
              "≥ 70% of expected 5-minute CGM samples in the 14-day period (≥ 2,822 readings)",
              lambda f: f["passes_cgm_coverage"]),
    ]


def dose_response_stages(series):
    """IR-6B's population for one series, counted on a frame of every candidate
    activation with the episode filters joined on (False where the activation
    did not survive filters 3 to 5): filters 1 and 3 to 7 of the plan."""
    return [
        Stage("Preset activations",
              "Every preset activation of the extract",
              every_row),
        Stage("In the series",
              f"Activation belongs to series {series}",
              lambda f: f[f"in_series_{series}"]),
        Stage("Single-activation day",
              "No other activation on the activation's calendar day",
              lambda f: f["pass_single_day"]),
        Stage("Window isolation",
              "The episode's window is disjoint from both neighbours' windows",
              lambda f: f["pass_isolation"]),
        Stage("Duration > 0",
              "The activation has a positive effective duration",
              lambda f: f["pass_duration"]),
        Stage("Starting glucose present",
              "A plausible CGM reading within 30 minutes before activation",
              lambda f: f["pass_start_bg"]),
        Stage("CGM coverage ≥ 70%",
              "Readings over the window reach 70% of the expected count",
              lambda f: f["pass_coverage"]),
    ]


def durability_stages():
    """Analysis 8-7's population, from autobolus_durability's columns."""
    return [
        Stage("Loop users with dosing data",
              "Every user with a day in the day table",
              every_row),
        Stage("Adopted autobolus",
              "A 3-day window with ≥ 80% autobolus days",
              lambda f: f["is_adopted"]),
        Stage("Minimum follow-up",
              "≥ 56 days of data after adoption",
              lambda f: f["has_min_followup"]),
        Stage("Final period classifiable",
              "The final 28 days reach the dosing-coverage threshold, or a terminal drop-off "
              "anchors the final period",
              lambda f: f["has_final_coverage"]),
        Stage("Age gate",
              "Age ≥ 6 y at adoption or unknown",
              lambda f: f["is_age_eligible"]),
        Stage("JAEB study linkage",
              "The user carries a JAEB participant identifier (the subgroup hand-off)",
              lambda f: f["is_jaeb_linked"]),
    ]
