# Live public API demo in Fabric

Run two credential-free sources through the same Landing engine, validate both payloads, and publish a Delta table you can query from a Fabric notebook or Lakehouse SQL analytics endpoint.

| Source | Historical slice | Gold metrics |
|---|---|---|
| USGS Earthquake Catalog | Worldwide magnitude 6+ events in January 2024 | Daily event count and maximum magnitude on days with events |
| World Bank Indicators | India's annual population, 2019–2023 | Annual population |

The historical filters keep the payloads small. These are full extracts of bounded historical slices. The API providers may revise historical values.

## Run it in DEV

1. Create the schema-enabled DEV Lakehouse and the `vl_ingestion` String variables as described under **Prepare the DEV workspace** in [the Fabric POC guide](fabric-poc.md). The destination values are required. For this demo, `vault_url` may stay a placeholder because both API connections use no authentication. The Spark environment needs `requests` and `urllib3`, and outbound HTTPS to `earthquake.usgs.gov` and `api.worldbank.org`.
2. Import and run [nb_bootstrap](../notebooks/nb_bootstrap.ipynb) with `expected_environment=dev` and `seed_examples=False`.
3. Import [nb_public_api_end_to_end](../notebooks/nb_public_api_end_to_end.ipynb) and select **Run all**. Keep `batch_id=public_api_001` for the first run. The notebook registers both sources, ingests them, checks their payloads, and displays the Gold metrics. The CSV demo notebooks are independent.
4. Run the same notebook again with the same batch ID. Both sources should report `already_succeeded`. The transformations replay the saved Landing payloads and replace just this client and batch in Delta. Use a new batch ID for a new API fetch. Run a given client and batch serially.

## Show the work in Fabric

| Stage | Where to look |
|---|---|
| Metadata | `ops.connection` and `ops.dataset` contain `usgs_earthquakes` and `worldbank_india_population`. |
| Landing | `ops.landing_event` records the successful attempts and manifest paths. Open a manifest and its neighboring JSON file in OneLake. |
| Bronze | `bronze.public_api_payloads` contains the JSON text, dataset ID, source path, and Landing attempt ID. |
| Quality | The notebook prints a pass or raises a validation error. It checks World Bank page completeness and five unique years, plus USGS feature count, unique event IDs, coordinates and date/magnitude bounds. A failed check prevents the current run from writing Silver or Gold. The separate CSV demo persists per-rule quality results. |
| Silver | `silver.public_api_earthquakes` and `silver.public_api_population` contain typed event and country-year records. |
| Gold | `gold.public_api_metrics` combines both metric sets with a source, geography, period, metric, and value. No join is implied between the datasets. |

Run this query from a notebook attached to the target Lakehouse:

```sql
SELECT source, geography, period, metric, value
FROM gold.public_api_metrics
WHERE client = 'demo' AND batch_id = 'public_api_001'
ORDER BY source, period, metric;
```

Replace `demo` with the Variable Library's client code. Expect five annual population rows and two daily earthquake metrics per event date. The notebook output also includes the validated event and year counts.

If extraction fails, correct the workspace connectivity or source configuration and rerun with the same batch ID. If Landing passes but validation fails, inspect the published JSON and the notebook error. The saved payload allows transformations to be replayed without an API call.

[USGS Earthquake Catalog API](https://earthquake.usgs.gov/fdsnws/event/1/) · [World Bank Indicators API](https://datahelpdesk.worldbank.org/knowledgebase/articles/898581-api-basic-call-structures)
