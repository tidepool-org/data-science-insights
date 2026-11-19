"""Derived values and their staleness check (report tools plan, section 4.7). Invented values."""

import json
import os

from report_test_helpers import raises, read_file, run_tests, temporary_folder, write_file

from report.derived_values import check_derived_inputs, inputs_path_of, write_derived_values
from report.errors import StaleDerivedFile

ROWS = [{"label": "share_retained", "value": "33.333333", "description": "final cohort as a percentage of all users"}]


def test_the_helper_writes_the_values_and_the_hashes_of_their_inputs():
    with temporary_folder() as folder:
        flow = write_file(os.path.join(folder, "flow.csv"), "stage,n\nAll,120\nFinal,40\n")
        derived = os.path.join(folder, "derived.csv")
        write_derived_values(ROWS, derived, inputs=[flow])
        assert read_file(derived).splitlines() == ["label,value,description", "share_retained,33.333333,final cohort as a percentage of all users"]
        recorded = json.loads(read_file(inputs_path_of(derived)))["inputs"]
        assert [entry["file"] for entry in recorded] == ["flow.csv"] and len(recorded[0]["sha256"]) == 64
        check_derived_inputs(derived)


def test_a_changed_or_absent_input_is_stale():
    with temporary_folder() as folder:
        flow = write_file(os.path.join(folder, "flow.csv"), "stage,n\nAll,120\nFinal,40\n")
        derived = os.path.join(folder, "derived.csv")
        write_derived_values(ROWS, derived, inputs=[flow])
        write_file(flow, "stage,n\nAll,120\nFinal,43\n")
        raises(StaleDerivedFile, "is stale", lambda: check_derived_inputs(derived))
        os.remove(flow)
        raises(StaleDerivedFile, "not beside it", lambda: check_derived_inputs(derived))


def test_an_ordinary_csv_passes_and_bad_calls_are_refused():
    with temporary_folder() as folder:
        plain = write_file(os.path.join(folder, "plain.csv"), "a,b\n1,2\n")
        check_derived_inputs(plain)
        raises(ValueError, "repeat", lambda: write_derived_values(ROWS + ROWS, os.path.join(folder, "d.csv"), inputs=[plain]))
        raises(ValueError, "`inputs` is empty", lambda: write_derived_values(ROWS, os.path.join(folder, "d.csv"), inputs=[]))


if __name__ == "__main__":
    run_tests([value for name, value in sorted(globals().items()) if name.startswith("test_")], "test_derived_values")
