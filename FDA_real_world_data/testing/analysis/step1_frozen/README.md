# Step 1 frozen sources

Verbatim copies, as text so they cannot be run, of two files as they stood at commit 9f2a029 (2026-10-30), the state the Step 1 runs of record copied:

- `export_valid_transition_segments.py.txt`: the staging window search (the 0.80 box on shares of classified days) and its defaults.
- `data_loading.py.txt`: the loader's cohort constants and its version gate, as the tie-out sets of the Step 1 files reproduce them.
- `export_user_diagnosis_type.py.txt`: the diagnosis lookup's staging as the Step 1 files copied its resolution verbatim (frozen 2026-11-05, when the live script moved its LADA expression to `cohort_constants.py` and gained `diagnosis_group`).

`testing/analysis/test_dose_rule_step1.py` pins the verbatim copies in `exploratory/dose_rule_estimate.sql` and `exploratory/classification_walkthrough.sql` to these, because Step 2 of the execution plan changed the live files (12 of 14 days per period, the calendar-day score, a missing version sent to the date rule). Never edit them; they retire with the Step 1 copies (plan Step 3).
