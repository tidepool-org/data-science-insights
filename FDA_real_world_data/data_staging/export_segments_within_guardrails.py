import pandas as pd
import json
import traceback

# =============================================================================
# GUARDRAILS (arbitrary values for now - update as needed)
# =============================================================================

MMOL_TO_MGDL = 18.016

GUARDRAILS = {
    # Correction Range
    'bg_target_max': 180.0,              # mg/dL
    'bg_target_min': 87.0,               # mg/dL

    # Blood Glucose Target Preprandial
    'bg_target_preprandial_max': 130.0,  # mg/dL
    'bg_target_preprandial_min': 67.0,   # mg/dL

    # Blood Glucose Workout
    'bg_target_workout_max': 180.0,      # mg/dL
    'bg_target_workout_min': 87.0,       # mg/dL

    # Blood Glucose Safety Limit
    'bg_safety_limit_max': 110.0,        # mg/dL
    'bg_safety_limit_min': 67.0,         # mg/dL

    # Insulin Sensitivity
    'insulin_sensitivity_max': 500.0,    # mg/dL/U
    'insulin_sensitivity_min': 10.0,     # mg/dL/U

    # Carbohydrate Ratio
    'carb_ratio_max': 150.0,             # g/U
    'carb_ratio_min': 2.0,              # g/U

    # Scheduled Basal Rates
    'basal_schedule_rate_max': 30.0,     # U/hr
    'basal_schedule_rate_min': 0.05,     # U/hr

    # Max Basal Delivery Rate
    'basal_rate_max': 30.0,              # U/hr

    # Max Bolus Delivery Limit
    'bolus_amount_max': 30.0,            # Units
}

# =============================================================================
# HELPERS
# =============================================================================

def _parse_timestamp(value):
    """Parse a timestamp value from bddp_sample_all.

    The raw `time` column is a struct {"$date": "..."} in MongoDB/Spark.
    After toPandas() it arrives as a dict, a pandas Timestamp, or a string.
    Returns a tz-aware pandas Timestamp (UTC), or pd.NaT on failure.
    """
    if isinstance(value, dict):
        value = value.get("$date")
    return pd.to_datetime(value, errors="coerce", utc=True)


def _safe_numeric(value, context=""):
    """Coerce to float, returning None for non-numeric values."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            pass
    print(f"  [WARN] _safe_numeric: got {type(value).__name__} = {repr(value)}"
          f"{f' ({context})' if context else ''}")
    return None


def _convert_mmol_to_mgdl(value, context=""):
    """Convert mmol/L to mg/dL, rounding to 1 decimal place."""
    value = _safe_numeric(value, context)
    return round(value * MMOL_TO_MGDL, 1) if value is not None else None


def parse_json_safe(val):
    """Safely parse JSON from string or return as-is if already dict."""
    if isinstance(val, dict):
        return val
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return None


# =============================================================================
# CHECK FUNCTIONS
# =============================================================================

def check_basal(basal_json):
    """Check basal rateMaximum against guardrails."""
    data = parse_json_safe(basal_json)
    if not data:
        return {'valid': None, 'rate_maximum': None, 'violation': None}

    rate_max = _safe_numeric(
        (data.get('rateMaximum') or {}).get('value'),
        context="basal.rateMaximum.value",
    )
    if rate_max is None:
        return {'valid': None, 'rate_maximum': None, 'violation': None}

    violation = rate_max > GUARDRAILS['basal_rate_max']
    return {
        'valid': not violation,
        'rate_maximum': rate_max,
        'violation': f"rateMaximum {rate_max} > {GUARDRAILS['basal_rate_max']}" if violation else None,
    }


def check_basal_schedules(schedules_json):
    """Check all basal schedule rates against guardrails."""
    data = parse_json_safe(schedules_json)
    if not data:
        return {
            'valid': None,
            'schedule_count': 0,
            'rate_min': None,
            'rate_max': None,
            'violations': [],
        }

    violations = []
    all_rates = []

    for schedule_name, entries in data.items():
        if not isinstance(entries, list):
            continue
        for i, entry in enumerate(entries):
            if not isinstance(entry, dict):
                continue
            rate = _safe_numeric(entry.get('rate'), context=f"basalSchedules {schedule_name}[{i}].rate")
            if rate is None:
                continue
            all_rates.append(rate)
            if rate > GUARDRAILS['basal_schedule_rate_max']:
                violations.append(f"{schedule_name}: rate {rate} > {GUARDRAILS['basal_schedule_rate_max']}")
            if rate < GUARDRAILS['basal_schedule_rate_min']:
                violations.append(f"{schedule_name}: rate {rate} < {GUARDRAILS['basal_schedule_rate_min']}")

    return {
        'valid': len(violations) == 0 if all_rates else None,
        'schedule_count': len(data),
        'rate_min': min(all_rates) if all_rates else None,
        'rate_max': max(all_rates) if all_rates else None,
        'violations': violations,
    }


def _check_bg_target_schedules(targets_json, guardrail_min_key, guardrail_max_key, label=""):
    """Shared logic for checking BG target schedule values against guardrails.

    Used by correction range, preprandial, and workout target checkers.
    """
    data = parse_json_safe(targets_json)
    if not data:
        return {
            'valid': None,
            'schedule_count': 0,
            'target_min': None,
            'target_max': None,
            'violations': [],
        }

    g_min = GUARDRAILS[guardrail_min_key]
    g_max = GUARDRAILS[guardrail_max_key]

    violations = []
    all_lows = []
    all_highs = []

    for schedule_name, entries in data.items():
        if not isinstance(entries, list):
            continue
        for i, entry in enumerate(entries):
            if not isinstance(entry, dict):
                continue

            ctx = f"{label} {schedule_name}[{i}]"
            target_low = _convert_mmol_to_mgdl(entry.get('low'), context=f"{ctx}.low")
            target_high = _convert_mmol_to_mgdl(entry.get('high'), context=f"{ctx}.high")

            if None in (target_low, target_high):
                continue

            all_lows.append(target_low)
            all_highs.append(target_high)

            if target_high > g_max:
                violations.append(f"{schedule_name}: target high {target_high} > {g_max}")
            if target_low < g_min:
                violations.append(f"{schedule_name}: target low {target_low} < {g_min}")

    return {
        'valid': len(violations) == 0 if (all_lows and all_highs) else None,
        'schedule_count': len(data),
        'target_min': min(all_lows) if all_lows else None,
        'target_max': max(all_highs) if all_highs else None,
        'violations': violations,
    }


def check_bg_targets(targets_json):
    """Check all BG target (correction range) values against guardrails."""
    return _check_bg_target_schedules(
        targets_json, 'bg_target_min', 'bg_target_max', label="bgTargets")


def check_bg_targets_preprandial(targets_json):
    """Check all preprandial BG target values against guardrails."""
    return _check_bg_target_schedules(
        targets_json, 'bg_target_preprandial_min', 'bg_target_preprandial_max', label="bgTargetsPreprandial")


def check_bg_targets_workout(targets_json):
    """Check all workout BG target values against guardrails."""
    return _check_bg_target_schedules(
        targets_json, 'bg_target_workout_min', 'bg_target_workout_max', label="bgTargetsWorkout")


def check_glucose_safety_limit(value):
    """Check glucose safety limit against guardrails.

    Expects a bare numeric value (mmol/L), not a JSON object.
    """
    if value is None or pd.isna(value):
        return {'valid': None, 'value': None, 'violation': None}

    limit = _convert_mmol_to_mgdl(value, context="bgSafetyLimit")
    if limit is None:
        return {'valid': None, 'value': None, 'violation': None}

    violations = []
    if limit > GUARDRAILS['bg_safety_limit_max']:
        violations.append(f"bgSafetyLimit {limit} > {GUARDRAILS['bg_safety_limit_max']}")
    if limit < GUARDRAILS['bg_safety_limit_min']:
        violations.append(f"bgSafetyLimit {limit} < {GUARDRAILS['bg_safety_limit_min']}")

    return {
        'valid': len(violations) == 0,
        'value': limit,
        'violation': '; '.join(violations) if violations else None,
    }


def check_insulin_sensitivity(sensitivity_json):
    """Check all insulin sensitivity factor values against guardrails."""
    data = parse_json_safe(sensitivity_json)
    if not data:
        return {
            'valid': None,
            'schedule_count': 0,
            'sensitivity_min': None,
            'sensitivity_max': None,
            'violations': [],
        }

    violations = []
    all_values = []

    for schedule_name, entries in data.items():
        if not isinstance(entries, list):
            continue
        for i, entry in enumerate(entries):
            if not isinstance(entry, dict):
                continue

            amount = _convert_mmol_to_mgdl(
                entry.get('amount'),
                context=f"insulinSensitivity {schedule_name}[{i}].amount",
            )
            if amount is None:
                continue

            all_values.append(amount)
            if amount > GUARDRAILS['insulin_sensitivity_max']:
                violations.append(f"{schedule_name}: sensitivity {amount} > {GUARDRAILS['insulin_sensitivity_max']}")
            if amount < GUARDRAILS['insulin_sensitivity_min']:
                violations.append(f"{schedule_name}: sensitivity {amount} < {GUARDRAILS['insulin_sensitivity_min']}")

    return {
        'valid': len(violations) == 0 if all_values else None,
        'schedule_count': len(data),
        'sensitivity_min': min(all_values) if all_values else None,
        'sensitivity_max': max(all_values) if all_values else None,
        'violations': violations,
    }


def check_bolus(bolus_json):
    """Check bolus settings against guardrails."""
    data = parse_json_safe(bolus_json)
    if not data:
        return {
            'valid': None,
            'amount_maximum': None,
            'violations': [],
        }

    violations = []

    amount_max = _safe_numeric(
        (data.get('amountMaximum') or {}).get('value'),
        context="bolus.amountMaximum.value",
    )

    if amount_max is not None and amount_max > GUARDRAILS['bolus_amount_max']:
        violations.append(f"amountMaximum {amount_max} > {GUARDRAILS['bolus_amount_max']}")

    return {
        'valid': len(violations) == 0 if amount_max is not None else None,
        'amount_maximum': amount_max,
        'violations': violations,
    }


def check_carb_ratios(ratios_json):
    """Check all carb ratio values against guardrails."""
    data = parse_json_safe(ratios_json)
    if not data:
        return {
            'valid': None,
            'schedule_count': 0,
            'ratio_min': None,
            'ratio_max': None,
            'violations': [],
        }

    violations = []
    all_ratios = []

    for schedule_name, entries in data.items():
        if not isinstance(entries, list):
            continue
        for i, entry in enumerate(entries):
            if not isinstance(entry, dict):
                continue
            amount = _safe_numeric(entry.get('amount'), context=f"carbRatios {schedule_name}[{i}].amount")
            if amount is None:
                continue
            all_ratios.append(amount)
            if amount > GUARDRAILS['carb_ratio_max']:
                violations.append(f"{schedule_name}: ratio {amount} > {GUARDRAILS['carb_ratio_max']}")
            if amount < GUARDRAILS['carb_ratio_min']:
                violations.append(f"{schedule_name}: ratio {amount} < {GUARDRAILS['carb_ratio_min']}")

    return {
        'valid': len(violations) == 0 if all_ratios else None,
        'schedule_count': len(data),
        'ratio_min': min(all_ratios) if all_ratios else None,
        'ratio_max': max(all_ratios) if all_ratios else None,
        'violations': violations,
    }


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def validate_pump_settings(df):
    """
    Validate pump settings DataFrame against guardrails.

    Expects columns: _userId, basal, basalSchedules, bgTargets,
                     bgTargetsPreprandial, bgTargetsWorkout,
                     bgSafetyLimit, insulinSensitivities,
                     bolus, carbRatios
    (adjust column names as needed based on your actual schema)
    """

    results = []
    errors = []

    for idx, row in df.iterrows():
        user_id = row.get('_userId')

        try:
            basal_check = check_basal(row.get('basal'))
            schedules_check = check_basal_schedules(row.get('basalSchedules'))
            targets_check = check_bg_targets(row.get('bgTargets'))
            preprandial_check = check_bg_targets_preprandial(row.get('bgTargetsPreprandial'))
            workout_check = check_bg_targets_workout(row.get('bgTargetsWorkout'))
            safety_check = check_glucose_safety_limit(row.get('bgSafetyLimit'))
            sensitivity_check = check_insulin_sensitivity(row.get('insulinSensitivities'))
            bolus_check = check_bolus(row.get('bolus'))
            carb_check = check_carb_ratios(row.get('carbRatios'))

        except Exception as e:
            print(f"\n{'='*60}")
            print(f"ERROR processing row {idx}, _userId={user_id}")
            print(f"Exception: {type(e).__name__}: {e}")
            print(f"{'='*60}")
            for col in ['basal', 'basalSchedules', 'bgTargets', 'bgTargetsPreprandial',
                         'bgTargetsWorkout', 'bgSafetyLimit', 'insulinSensitivities',
                         'bolus', 'carbRatios']:
                val = row.get(col)
                print(f"  {col}: type={type(val).__name__}, value={repr(val)[:200]}")
            traceback.print_exc()
            print()

            errors.append({'_userId': user_id, 'row_index': idx, 'error': str(e)})
            continue

        # Aggregate all violations
        all_violations = (
            ([basal_check['violation']] if basal_check['violation'] else []) +
            schedules_check['violations'] +
            targets_check['violations'] +
            preprandial_check['violations'] +
            workout_check['violations'] +
            ([safety_check['violation']] if safety_check['violation'] else []) +
            sensitivity_check['violations'] +
            bolus_check['violations'] +
            carb_check['violations']
        )

        validity_flags = [
            basal_check['valid'],
            schedules_check['valid'],
            targets_check['valid'],
            preprandial_check['valid'],
            workout_check['valid'],
            safety_check['valid'],
            sensitivity_check['valid'],
            bolus_check['valid'],
            carb_check['valid'],
        ]

        results.append({
            '_userId': user_id,
            'settings_time': _parse_timestamp(row.get('time')),

            # Max Basal Delivery Rate
            'basal_rate_max': basal_check['rate_maximum'],
            'basal_valid': basal_check['valid'],

            # Scheduled Basal Rates
            'basal_schedule_count': schedules_check['schedule_count'],
            'basal_schedule_rate_min': schedules_check['rate_min'],
            'basal_schedule_rate_max': schedules_check['rate_max'],
            'basal_schedules_valid': schedules_check['valid'],

            # Correction Range
            'bg_target_schedule_count': targets_check['schedule_count'],
            'bg_target_min': targets_check['target_min'],
            'bg_target_max': targets_check['target_max'],
            'bg_targets_valid': targets_check['valid'],

            # Preprandial Targets
            'bg_target_preprandial_schedule_count': preprandial_check['schedule_count'],
            'bg_target_preprandial_min': preprandial_check['target_min'],
            'bg_target_preprandial_max': preprandial_check['target_max'],
            'bg_targets_preprandial_valid': preprandial_check['valid'],

            # Workout Targets
            'bg_target_workout_schedule_count': workout_check['schedule_count'],
            'bg_target_workout_min': workout_check['target_min'],
            'bg_target_workout_max': workout_check['target_max'],
            'bg_targets_workout_valid': workout_check['valid'],

            # Glucose Safety Limit
            'glucose_safety_limit': safety_check['value'],
            'glucose_safety_limit_valid': safety_check['valid'],

            # Insulin Sensitivity
            'insulin_sensitivity_schedule_count': sensitivity_check['schedule_count'],
            'insulin_sensitivity_min': sensitivity_check['sensitivity_min'],
            'insulin_sensitivity_max': sensitivity_check['sensitivity_max'],
            'insulin_sensitivity_valid': sensitivity_check['valid'],

            # Max Bolus Delivery Limit
            'bolus_amount_max': bolus_check['amount_maximum'],
            'bolus_valid': bolus_check['valid'],

            # Carbohydrate Ratio
            'carb_ratio_schedule_count': carb_check['schedule_count'],
            'carb_ratio_min': carb_check['ratio_min'],
            'carb_ratio_max': carb_check['ratio_max'],
            'carb_ratios_valid': carb_check['valid'],

            # Overall
            'all_valid': all(v in {True, None} for v in validity_flags),
            'violation_count': len(all_violations),
            'violations': '; '.join(all_violations) if all_violations else None,
        })

    if errors:
        print(f"\n{'='*60}")
        print(f"SUMMARY: {len(errors)} rows failed out of {len(df)}")
        print(f"{'='*60}")

    results_df = pd.DataFrame(results)
    errors_df = pd.DataFrame(errors) if errors else pd.DataFrame()

    return results_df, errors_df