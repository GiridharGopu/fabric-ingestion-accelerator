# Client demo with synthetic orders

## Prepare the DEV workspace

1. Create a schema-enabled Lakehouse and a Variable Library named `vl_ingestion`. Create the String variables in [the example](../metadata/variable-library.example.json), replacing the workspace and Lakehouse GUIDs and name. Set `environment=dev`. The synthetic OneLake path does not call Key Vault.
2. Import all four notebooks from [notebooks](../notebooks/). Use a Fabric PySpark Environment with `requests` and `urllib3` available. Use the DEV identity that can read and write the test Lakehouse.
3. Run `nb_bootstrap` with `expected_environment=dev`, then `nb_poc_setup`. The latter creates the source folder, registers the synthetic dataset, and seeds its quality rules.

## Run the demo

| Demo step | What to run | Expected result |
|---|---|---|
| First delivery | Upload [orders_01.csv](../fixtures/orders_01.csv) to `Files/poc_source/orders`. Run `nb_land` with `dataset_id=poc__orders`, blank `run_group`, and `batch_id=poc_001`. Run `nb_poc_quality` for `poc_001`. | One file published. Two valid rows. Five quality rules pass. |
| Safe rerun | Rerun `nb_land` with the same parameters. | `already_succeeded`. No new published attempt. |
| Incremental delivery | Upload [orders_02.csv](../fixtures/orders_02.csv). Run Landing and quality with `poc_002`. | Only the new file lands. One valid row. |
| Quality failure | Upload [orders_bad.csv](../fixtures/orders_bad.csv). Run Landing and quality with `poc_003`. | Landing publishes raw evidence. Quality logs duplicate-key, invalid-date, and invalid-amount failures, then raises an error. |

Upload the fixtures individually in this sequence. Keep earlier files in place so the second run can demonstrate incremental discovery. Use a fresh DEV Lakehouse or unique source paths and dataset metadata for a clean repeat.

In a PySpark notebook attached to the same Lakehouse, use these Spark SQL queries to show the evidence:

```sql
SELECT batch_id, status, object_count, manifest_path
FROM ops.landing_event
WHERE dataset_id = 'poc__orders'
ORDER BY recorded_utc;

SELECT batch_id, rule_id, measured, threshold, status
FROM ops.quality_result
WHERE dataset_id = 'poc__orders'
ORDER BY batch_id, rule_id;
```

## Present the design

Show one connection row and one dataset row before running the demo. Open the published manifest in OneLake, then show the ledger and failed quality rules. For the next step, wire downstream processing to the quality notebook's success condition. Replay the published Landing batch for quality re-evaluation. Use the original batch ID when retrying a failed extraction. The included quality evaluator is batch-scoped. Validate cross-batch uniqueness, referential checks, and reconciliation in the client transformation layers.
