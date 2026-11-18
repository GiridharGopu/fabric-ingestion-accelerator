# Fabric run manual

Use the generated `.ipynb` notebooks in a Fabric **PySpark** session. The notebooks embed the code they need from `src/`. Uploading the source modules separately is unnecessary for the two demos.

## Which Python files run where?

| File | Where it is used |
|---|---|
| `src/engine.py` | Shared ingestion logic embedded in `nb_land` and `nb_public_api_end_to_end`. It expects a Fabric Spark session, NotebookUtils, and the `ops` control tables. |
| `src/poc_quality.py` | Spark quality evaluation embedded in `nb_poc_quality`. It reads the published CSV batch and records per-rule outcomes. |
| `src/public_api_demo.py` | Pure Python parsers and validation functions for USGS and World Bank JSON. They are embedded in the public API notebook and can also be tested locally. |
| `src/public_api_fabric.py` | Spark transformations and Delta publishing embedded in the public API notebook. The generator resolves its repository-local imports by embedding the dependent code first. |
| `build_notebooks.py` | Run locally or in CI from a repository checkout. Regenerates the notebooks from source and metadata. |
| `scripts/validate.py` | Run locally or in CI. Checks notebook cell syntax and example metadata keys. |

The source modules define functions and classes. Running a module alone does not launch the pipeline. Use the notebooks as the execution entry points.

## 1. Prepare Fabric DEV

1. In a workspace with Fabric capacity, create a **schema-enabled Lakehouse**. Use a test Lakehouse for this run.
2. In the same workspace, create a Variable Library named `vl_ingestion`. Use [the settings example](../metadata/variable-library.example.json) as a reference. Define every variable as **String**, including the two GUIDs.
3. Set `environment=dev`, `client_code=demo`, and `landing_root=Files/landing`. Fill in the actual `workspace_id`, `lakehouse_id`, and `lakehouse_name`. Keep `vault_url` as a placeholder for these demos, which do not retrieve secrets.
4. Download the required `.ipynb` files from [notebooks](../notebooks/) and import them into the DEV workspace. Check that they open as **PySpark** notebooks. Keep `%%configure` as the first executable cell so the notebook binds to the Lakehouse from the variable settings.
5. Use a Spark environment with [the Python dependencies](../requirements.txt) available. The public API demo also needs outbound HTTPS to `earthquake.usgs.gov` and `api.worldbank.org`. Run under a DEV identity with access to the Variable Library and write access to the Lakehouse.

## 2. Run the public API demo

| Order | Notebook | Parameters |
|---|---|---|
| 1 | [nb_bootstrap](../notebooks/nb_bootstrap.ipynb) | `expected_environment = "dev"`, `seed_examples = False` |
| 2 | [nb_public_api_end_to_end](../notebooks/nb_public_api_end_to_end.ipynb) | `batch_id = "public_api_001"` |

In each notebook, use **Run all**. After the second notebook finishes, look for the printed quality result and the displayed Gold rows. Inspect `ops.landing_event`, `bronze.public_api_payloads`, and `gold.public_api_metrics` in the Lakehouse. Run the second notebook again with the same batch ID to demonstrate reuse of successful Landing payloads.

In a PySpark notebook bound to the same Lakehouse:

```python
display(spark.sql("""
    SELECT source, geography, period, metric, value
    FROM gold.public_api_metrics
    WHERE client = 'demo' AND batch_id = 'public_api_001'
    ORDER BY source, period, metric
"""))
```

Change `demo` if you used a different client code. See the [public API walkthrough](public-api-demo.md) for the expected output and source details.

## 3. Optional CSV and quality demo

Run `nb_bootstrap` first if this is a fresh Lakehouse, then run `nb_poc_setup`. Upload the CSV fixtures individually in the order given in the [synthetic orders walkthrough](fabric-poc.md). For each delivery, run `nb_land` with `dataset_id="poc__orders"` and the corresponding batch ID, then run `nb_poc_quality` with that same batch ID. The bad fixture is expected to fail quality and write the rule outcomes to `ops.quality_result`.

## 4. Rebuild after editing the source

From the repository root on your machine:

```bash
python build_notebooks.py
python scripts/validate.py
```

Commit both the source changes and regenerated notebooks. Reimport the updated notebooks into Fabric before running them. The validation script checks syntax and metadata. Run the notebooks in a DEV workspace to verify runtime compatibility, Lakehouse access, and live API responses.

## Common blockers

| Symptom | Check |
|---|---|
| Variable lookup or Lakehouse binding fails | Check the Variable Library name and active values, confirm both GUIDs, and restart the Spark session after correcting settings. |
| `ops` tables are missing | Run `nb_bootstrap` first and verify both notebooks resolve to the same Lakehouse. |
| Python package is missing | Check the attached Spark environment for `requests` and `urllib3`. |
| API request fails | Check outbound HTTPS and source availability. Retry the same batch ID to reuse a completed source. |
| CSV quality notebook fails | Check whether the bad fixture was loaded intentionally, then inspect `ops.quality_result`. |

[Microsoft Learn notebook guide](https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook) · [NotebookUtils Variable Library](https://learn.microsoft.com/en-us/fabric/data-engineering/notebookutils/notebookutils-variable-library)
