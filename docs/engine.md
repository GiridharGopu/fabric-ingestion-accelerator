# Microsoft Fabric metadata-driven Landing accelerator

Use this folder as a Git template. The same notebook runs each enabled source using the connection and dataset rows in Delta. New instances of supported connector patterns are configuration changes. Connector-specific behavior lives in `src/engine.py`. Run `python build_notebooks.py` after editing the engine or seed definitions and commit the regenerated notebooks.

## Client onboarding

1. Create isolated DEV, UAT and PROD workspaces, with one schema-enabled Lakehouse per environment. Set up access to source systems, storage and Key Vault for the notebook execution identity.
2. Create `vl_ingestion` from `metadata/variable-library.example.json`. Make all entries String variables. Set the appropriate active value set per workspace. The first cell binds the Lakehouse dynamically. Keep passwords and tokens in Key Vault.
3. Import `notebooks/nb_bootstrap.ipynb` and run it. Set `seed_examples=True` in DEV to insert disabled example jobs. Use Delta MERGE or your metadata deployment process to promote reviewed rows. Never duplicate a connection key or dataset ID.
4. Register and enable one DEV dataset. For a quick test, use ADLS or a OneLake shortcut with two immutable files. Set `dataset_id`, `expected_environment` and a unique stable `batch_id` in `nb_land`. For scheduled execution, toggle the input cell as the parameters cell and pass the same fields from a Fabric Notebook activity. Set `run_group` instead of `dataset_id` to run an enabled group sequentially.
5. Verify the success ledger and its manifest, then rerun with the same batch ID. The notebook should report `already_succeeded`. For an incremental file test, add another immutable file and use a new batch ID. Only the new file should land.

## Control tables

| Table | Logical key | Contents |
|---|---|---|
| `ops.connection` | connection_id + environment | Adapter name and endpoint, authentication references or storage root |
| `ops.dataset` | dataset_id | Connection reference, load mode, enabled flag, run group and source-specific options |
| `ops.landing_event` | client + environment + dataset + batch + attempt + status | Frozen extraction plan, success checkpoint, manifest path and failure type |

Use the success ledger as the publication contract. Consumers should read only attempt paths published by `succeeded` events. Each attempt writes to `Files/landing/<client>/<env>/<dataset>/<batch>/<attempt>/`. Partial attempts can be removed under a retention policy. The success event advances the checkpoint and publishes the path in the same Delta append, after the payload and manifest have been written. Configure pipeline execution so the same dataset never overlaps across groups, schedules or manual retries. Delta logging alone does not serialize the read-plan-write interval.

## Source patterns

| Adapter | Load modes | Landing output and source contract |
|---|---|---|
| REST API | Full, bounded watermark | One text payload per page. GET, Basic/Bearer/OAuth client credentials, cursor or same-origin next links. Configure server-side time filters and verify the API honors both bounds. |
| Workday RaaS | Full, bounded watermark | Report output as XML, JSON or CSV. The report must expose source prompts for incremental ranges. Use a report integration account permitted to run the report. |
| SQL Server / Azure SQL | Full, bounded watermark, native CDC | Spark JDBC writes Parquet. Watermark columns must represent UTC time. CDC retains LSN, sequence and operation fields including delete and update-before/update-after images. |
| ADLS / OneLake shortcut | Full file inventory, incremental file discovery | Byte-for-byte copies of source files. Match by prefix and glob, including CSV, Excel, JSON and Parquet, with no format parsing in Landing. |

Watermark extraction uses a closed lower and exclusive upper bound for SQL. For APIs and RaaS, verify the source semantics and use configured overlap where needed. Downstream consumers deduplicate overlapping rows and handle delete semantics. A watermark feed can only expose deletes if the source emits them. Before enabling CDC, coordinate a consistent baseline with the CDC position, supply the associated last consumed LSN as 20 hex digits, and validate CDC retention and permissions. Expired CDC positions stop the batch and require baseline recovery. This starter does not perform coordinated baseline capture.

For ADLS, incremental identity uses path, size and last modification time. Use a publish protocol where files arrive as immutable completed objects. A corrected file should arrive at a new path or with a distinguishable version; file disappearance does not emit a delete event. Inventories are bounded per batch and historical inventory is scanned. For high-volume drop zones, use a separate inventory table or storage events. HTTP extracts are bounded by page count/size and meant for batch-sized feeds. Use a managed copy or source-specific connector when throughput, authentication, networks, API snapshot semantics or database engines require it.

## Recover independently

Pass the same `dataset_id` and `batch_id` to retry one job. To retry all jobs for a group, rerun the original group and batch ID. Successful datasets skip, attempted jobs reuse the original time/LSN bounds or file list, and attempts land in fresh folders. A pending batch blocks a newer batch for that dataset. If a prior plan exists and config changed, restore its original metadata before retrying. Fix retention gaps, expired permissions or source file mutations with an operator-approved recovery. Use a new dataset ID or explicitly migrate state when the source identity or checkpoint contract changes. Re-extracting an API or DB range reproduces its bounds; stable payload contents depend on source snapshot or version support.

Inspect operations with Spark SQL.

```sql
SELECT dataset_id, batch_id, attempt_id, status, manifest_path, error_type, recorded_utc
FROM ops.landing_event
ORDER BY recorded_utc DESC;
```

The sample uses the Fabric notebook execution identity for Variable Library lookup. For unattended identities that cannot call NotebookUtils Variable Library, supply the same bindings through a supported deployment profile and validate the identity against Lakehouse, Key Vault and the sources. Provision private networking and drivers in the client environment as required.

Landing ends at raw delivery and a committed manifest. Bronze parsing, Silver deduplication and Gold models consume published batches separately.
