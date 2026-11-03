-- =============================================================================
-- The extract's columns: which fields the dose classifier can read
-- =============================================================================
-- Context (execution plan 2026-10-27, Step 2; MC, 2026-10-30): the Tidepool
-- plugin uploads two kinds of automated basal record. One is a temporary basal
-- Loop set; the other, since TidepoolService 43b91a0 (2023-03-16, before Loop
-- 3.2.0), is a scheduled basal Loop set automatically. Only the temporary
-- basal carries the `suppressed` field (the scheduled rate it replaced), so
-- the classifier can tell them apart only if the extract kept that field.
-- This lists every column of dev.default.bddp_sample_all_2 with its type. No
-- row of the table is read.
--
-- Run through analysis/export_sql_results_to_csv.py (in DEFAULT_JOBS with skip
-- none; or --sql bddp_extract_columns.sql --skip none). Outputs result_0 +
-- manifest.csv in analysis/outputs/bddp_extract_columns/; download to Drive
-- 510k/claude/rebuild/data/. Databricks only; seconds.
--
-- EXPORT: result_0 the extract's columns in table order, with their types.
-- =============================================================================


-- 0) The columns of the extract, in table order --------------------------------
SELECT ordinal_position, column_name, data_type
FROM dev.information_schema.columns
WHERE table_schema = 'default' AND table_name = 'bddp_sample_all_2'
ORDER BY ordinal_position;
