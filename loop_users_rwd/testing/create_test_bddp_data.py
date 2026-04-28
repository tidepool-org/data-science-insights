"""Synthetic BDDP fixture generator for loop_users_rwd tests.

The production BDDP table has ~130 nullable-string columns; tests only need
the columns that the staging queries actually read. Each `make_*` function
returns a dict matching that sparse schema. Combine rows from multiple
makers into a single list and pass to `setup_test_table`.

Convention: every column is a string (matches BDDP). Callers set what
they care about; everything else defaults to None.
"""

import json


def _bddp_row(**kwargs):
    """Build a BDDP-shaped row from the caller's populated columns only.

    Omits keys whose value is None so pandas/Spark don't see all-None columns
    (which break type inference in spark.createDataFrame). Callers that need
    an explicit NULL on a row where OTHER rows populate the column will still
    get it — pandas fills missing keys with NaN and Spark reads that as NULL.
    """
    return {k: v for k, v in kwargs.items() if v is not None}


def dd_origin(version=None):
    """origin JSON for a dosingDecision row. `version` populates origin.version."""
    payload = {}
    if version is not None:
        payload["version"] = version
    return json.dumps(payload) if payload else None


def hk_origin(version=None, source_name="Loop"):
    """origin JSON marking the record as HealthKit-sourced from `source_name`.

    Setting source_name='Loop' is the cohort-membership signal.
    """
    payload = {"payload": {"sourceRevision": {"source": {"name": source_name}}}}
    if version is not None:
        payload["version"] = version
    return json.dumps(payload)


def hk_automated_payload():
    """payload JSON marking the record as an automated delivery.

    This is the autobolus classifier flag — used downstream on loop_boluses /
    loop_basals, NOT for cohort membership. Cohort membership uses hk_origin().
    """
    return json.dumps({"com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued": 1})


def make_dosing_decision(user_id, time_string, reason, version=None):
    """dosingDecision row. `reason ∈ {loop, normalBolus, override, …}`."""
    return _bddp_row(
        _userId=user_id,
        type="dosingDecision",
        reason=reason,
        time_string=time_string,
        origin=dd_origin(version),
    )


def make_bolus(
    user_id,
    time_string,
    subType="normal",
    normal=None,
    loop_sourced=False,
    hk_automated=False,
    version=None,
):
    """bolus row. Set loop_sourced=True to mark origin.payload.sourceRevision.source.name='Loop'
    (cohort membership). Set hk_automated=True to also set MetadataKeyAutomaticallyIssued=1
    (classifier flag — used downstream, not for cohort).
    """
    return _bddp_row(
        _userId=user_id,
        type="bolus",
        subType=subType,
        time_string=time_string,
        normal=str(normal) if normal is not None else None,
        origin=hk_origin(version) if loop_sourced else dd_origin(version),
        payload=hk_automated_payload() if hk_automated else None,
    )


def make_basal(
    user_id,
    time_string,
    deliveryType="temp",
    rate=None,
    loop_sourced=False,
    hk_automated=False,
    version=None,
):
    """basal row. See make_bolus for loop_sourced / hk_automated semantics."""
    return _bddp_row(
        _userId=user_id,
        type="basal",
        deliveryType=deliveryType,
        rate=str(rate) if rate is not None else None,
        time_string=time_string,
        origin=hk_origin(version) if loop_sourced else dd_origin(version),
        payload=hk_automated_payload() if hk_automated else None,
    )


def make_cbg(user_id, time_string, value=None):
    return _bddp_row(
        _userId=user_id,
        type="cbg",
        time_string=time_string,
        value=str(value) if value is not None else None,
    )


def make_food(user_id, time_string, carb_grams_net=None):
    nutrition = (
        json.dumps({"carbohydrate": {"net": carb_grams_net}})
        if carb_grams_net is not None else None
    )
    return _bddp_row(
        _userId=user_id,
        type="food",
        time_string=time_string,
        nutrition=nutrition,
    )


def make_pump_settings(
    user_id,
    time_string,
    basalSchedules=None,
    bgTargets=None,
    insulinSensitivities=None,
    carbRatios=None,
):
    return _bddp_row(
        _userId=user_id,
        type="pumpSettings",
        time_string=time_string,
        basalSchedules=basalSchedules,
        bgTargets=bgTargets,
        insulinSensitivities=insulinSensitivities,
        carbRatios=carbRatios,
    )


# --- Demographics fixtures ---

def make_user_dates(userid, dob=None, diagnosis_date=None):
    """bddp_user_dates row. Uses LOWERCASE `userid` on purpose to catch
    the case-convention join bug against BDDP's `_userId`."""
    return {
        "_id": f"ud_{userid}",
        "userid": userid,
        "dob": dob,
        "diagnosis_date": diagnosis_date,
    }


def make_user_gender(user_id_camel, gender):
    """user_gender row. Uses camelCase `userId` on purpose."""
    return {"userId": user_id_camel, "gender": gender}
