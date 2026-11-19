"""The analysis bundle served to the loaders in place of a Spark session.

production_runs/export_analysis_bundle.py writes each table the analyses read as one
Parquet file. The loaders in utils/data_loading.py ask a session for
`spark.table(name).toPandas()`; `LocalTables(folder)` answers the same calls from the
bundle, so an analysis runs on a laptop through the entry point it has on Databricks:

    tables = LocalTables("outputs/analysis_bundle")
    analysis.run_in_databricks(tables, suffix="")

It serves whole tables and column selections. It does not run SQL: an analysis that sends
a query raises here and names itself, which is how the remaining Spark-side work is found.

Parquet keeps the types the loaders already expect from Spark: dates come back as date
objects, decimals as Decimal objects, a boolean column holding NULLs as objects.
"""

import json
import os

import pandas as pd

MANIFEST_NAME = "bundle_manifest.json"


class BundleTable:
    """One table of the bundle, read when asked."""

    # The loaders use this to skip work the export has already done (the string casts).
    is_bundle_table = True

    def __init__(self, path, selected=None):
        self._path = path
        self._selected = selected

    @property
    def columns(self):
        if self._selected is not None:
            return list(self._selected)
        import pyarrow.parquet as parquet
        return list(parquet.read_schema(self._path).names)

    def select(self, *columns):
        names = list(columns[0]) if len(columns) == 1 and isinstance(columns[0], (list, tuple)) else list(columns)
        missing = [name for name in names if name not in self.columns]
        if missing:
            raise KeyError(f"{os.path.basename(self._path)} has no column(s) {missing}")
        return BundleTable(self._path, names)

    def toPandas(self):  # noqa: N802  (the Spark name the loaders call)
        return pd.read_parquet(self._path, columns=self._selected)


class _Catalog:
    def __init__(self, tables):
        self._tables = tables

    def tableExists(self, name):  # noqa: N802
        return os.path.exists(self._tables.path_of(name))


class LocalTables:
    """A folder of bundle files, with the part of a Spark session the loaders use."""

    # An analysis reads this to take a count the bundle carries in place of a query on
    # tables that never leave Databricks (Table 6.3a's upstream stages).
    is_analysis_bundle = True

    def __init__(self, folder):
        if not os.path.isdir(folder):
            raise FileNotFoundError(f"no analysis bundle at {folder}")
        self.folder = folder
        self.catalog = _Catalog(self)
        manifest_path = os.path.join(folder, MANIFEST_NAME)
        self.manifest = json.load(open(manifest_path)) if os.path.exists(manifest_path) else {}
        self.version = f"analysis bundle of {self.manifest.get('created_utc', 'unknown date')}"

    def path_of(self, name):
        """The file of a table, from its bare or catalog-qualified name."""
        return os.path.join(self.folder, name.split(".")[-1] + ".parquet")

    def table(self, name):
        path = self.path_of(name)
        if not os.path.exists(path):
            raise FileNotFoundError(f"{name} is not in the analysis bundle at {self.folder}; "
                                    f"add it to production_runs/export_analysis_bundle.py and export again")
        return BundleTable(path)

    def sql(self, text):
        raise NotImplementedError("the analysis bundle serves tables, not SQL; this query still needs a local form:\n"
                                  + text.strip()[:400])
