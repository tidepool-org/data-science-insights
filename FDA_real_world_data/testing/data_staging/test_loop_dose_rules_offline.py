"""
Offline test for the dose classification and its day projection (no Spark).

Renders both scripts' SQL with a stand-in session and checks that:
- the day projection counts every rule of the classification, each for the
  record types it can recognise;
- the two scripts parse a version with the same expression;
- the classification's thresholds and Loop's app identifier are the ones the
  rule estimate measured (exploratory/dose_rule_estimate.sql);
- every rule becomes a column of the classification and a count of the day
  projection, and the day classes use the one threshold.

Runs locally and on Databricks.
"""

import importlib.util
import os
import re

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
PROJECT = os.path.normpath(os.path.join(_here, "..", ".."))


def _load(name, relative):
    spec = importlib.util.spec_from_file_location(name, os.path.join(PROJECT, relative))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


classification = _load("export_loop_dose_classification", "data_staging/export_loop_dose_classification.py")
projection = _load("export_loop_recommendations", "data_staging/export_loop_recommendations.py")


class _Session:
    def sql(self, text):
        self.text = text


def _rendered(module, **kwargs):
    session = _Session()
    module.run(session, **kwargs)
    return session.text


def test_projection_counts_every_rule_for_its_record_types():
    tags = set(classification.TAGS)
    assert set(projection.BOLUS_TAGS) | set(projection.BASAL_TAGS) == tags
    assert set(projection.BOLUS_TAGS) & set(projection.BASAL_TAGS) == {"decision_match", "health_flag_named_loop"}
    sql = _rendered(projection)
    for tag in projection.BOLUS_TAGS:
        assert f"SUM(CASE WHEN type = 'bolus' THEN {tag} ELSE 0 END) AS boluses_{tag}" in sql, tag
    for tag in projection.BASAL_TAGS:
        assert f"SUM(CASE WHEN type = 'basal' THEN {tag} ELSE 0 END) AS basals_{tag}" in sql, tag
    minimum = projection.AUTOBOLUS_DAY_MIN_BOLUSES
    assert f"c.automated_boluses >= {minimum} THEN 1 ELSE 0 END" in sql
    assert f"c.automated_boluses < {minimum} AND c.temp_basals > 0" in sql
    assert "WHERE is_automated = 1" in sql
    print("PASS: the day projection counts every rule and uses one threshold")


def test_both_scripts_parse_a_version_alike():
    for column in ("r.plugin_version", "get_json_object(origin, '$.version')"):
        assert classification.version_int_sql(column) == projection.version_int_sql(column), column
    expression = classification.version_int_sql("v")
    assert expression.startswith("CASE WHEN v RLIKE '^[0-9]+[.][0-9]+' THEN") and expression.endswith(" END")
    assert "ELSE" not in expression, "an unparsable version must stay NULL, never 0"
    print("PASS: one version expression, NULL when the version does not parse")


def test_classification_matches_the_estimates_measured_rules():
    estimate = open(os.path.join(PROJECT, "exploratory", "dose_rule_estimate.sql"), encoding="utf-8").read()
    pattern = re.search(r"RLIKE '([^']+)', FALSE\) AS hk_is_loop_bundle", estimate).group(1)
    assert classification.LOOP_APP_IDENTIFIER_PATTERN == pattern, (classification.LOOP_APP_IDENTIFIER_PATTERN, pattern)
    assert f"BETWEEN 0 AND {classification.DECISION_WINDOW_SECONDS})" in estimate
    assert "ABS(TIMESTAMPDIFF(SECOND, n.ts, d.ts)) <= 15)" in estimate and classification.USER_BOLUS_WINDOW_SECONDS == 15
    assert "plugin_version RLIKE '^3[.]'" in estimate and classification.LOOP3_VERSION_PATTERN == "^3[.]"
    print("PASS: windows, Loop's app identifier, and the Loop 3 test are the estimate's")


def test_every_rule_is_a_column_and_feeds_the_verdict():
    sql = _rendered(classification)
    assert "{" not in sql.replace("{10}", ""), "an unformatted placeholder in the SQL"
    for tag in classification.TAGS:
        assert re.search(rf"\bAS {tag}\b", sql), f"{tag} is not computed"
        assert f"MAX({tag}) AS {tag}" in sql, f"{tag} is not kept on the logical dose"
        assert f"MAX({tag})" in sql[sql.index("GREATEST("):], f"{tag} does not feed is_automated"
    assert "GROUP BY _userId, type, minute_key, units" in sql
    # today's first rule, exactly: a Loop decision 0 to 5 s before, and for a bolus no user-bolus
    # decision within 15 s and a subType other than normal
    assert "d.user_bolus_near = 0 AND d.subType IS NOT NULL AND d.subType <> 'normal'" in sql
    # today's second rule, exactly: the automatic key at 1 from a source named Loop
    assert "d.health_source_name = 'Loop' AND d.automatic_key_value = 1" in sql
    print("PASS: every rule is a column of the dose and feeds the automated verdict")


if __name__ == "__main__":
    test_projection_counts_every_rule_for_its_record_types()
    test_both_scripts_parse_a_version_alike()
    test_classification_matches_the_estimates_measured_rules()
    test_every_rule_is_a_column_and_feeds_the_verdict()
    print("test_loop_dose_rules_offline: all checks passed")
