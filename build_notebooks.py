"""Build importable Fabric notebooks from the reviewed source and seed metadata."""
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parent
CONFIGURE = """%%configure
{
  "defaultLakehouse": {
    "name": {"variableName": "$(/**/vl_ingestion/lakehouse_name)"},
    "id": {"variableName": "$(/**/vl_ingestion/lakehouse_id)"},
    "workspaceId": {"variableName": "$(/**/vl_ingestion/workspace_id)"}
  }
}"""


def write(name, title, *blocks):
    cells = [{"cell_type": "markdown", "source": [f"# {title}"], "metadata": {}}]
    for source, tags in ((CONFIGURE, []), *blocks):
        cells.append({"cell_type": "code", "source": source.splitlines(keepends=True),
                      "metadata": {"tags": tags} if tags else {},
                      "execution_count": None, "outputs": []})
    notebook = {"cells": cells, "metadata": {
        "kernelspec": {"display_name": "Synapse PySpark", "language": "Python",
                        "name": "synapse_pyspark"}}, "nbformat": 4, "nbformat_minor": 4}
    (ROOT / "notebooks" / f"{name}.ipynb").write_text(json.dumps(notebook, indent=2) + "\n")


ddl = (ROOT / "metadata/control.sql").read_text()
examples = (ROOT / "metadata/examples.json").read_text()
write("nb_bootstrap", "Create control tables and optionally seed examples",
      ('expected_environment = "dev"\nseed_examples = False', ["parameters"]),
      (f"""
import json
import notebookutils

library = notebookutils.variableLibrary.getLibrary("vl_ingestion")
environment = library.getVariable("environment")
if environment != expected_environment or environment not in {{"dev", "uat", "prod"}}:
    raise ValueError("Unexpected environment")

for statement in {ddl!r}.split(";"):
    if statement.strip():
        spark.sql(statement)

if seed_examples:
    if environment != "dev":
        raise ValueError("Seed examples in DEV only")
    examples = json.loads({examples!r})
    connections = [(row["connection_id"], row["environment"], row["connector"],
                    json.dumps(row["options"])) for row in examples["connections"]]
    datasets = [(row["dataset_id"], row["connection_id"], row["load_mode"],
                 row["enabled"], row["run_group"], json.dumps(row["options"]))
                for row in examples["datasets"]]
    spark.createDataFrame(connections, "connection_id string, environment string, connector string, options_json string").createOrReplaceTempView("seed_connections")
    spark.createDataFrame(datasets, "dataset_id string, connection_id string, load_mode string, enabled boolean, run_group string, options_json string").createOrReplaceTempView("seed_datasets")
    spark.sql('''MERGE INTO ops.connection t USING seed_connections s
                 ON t.connection_id = s.connection_id AND t.environment = s.environment
                 WHEN NOT MATCHED THEN INSERT *''')
    spark.sql('''MERGE INTO ops.dataset t USING seed_datasets s
                 ON t.dataset_id = s.dataset_id WHEN NOT MATCHED THEN INSERT *''')
""".strip(), []))
write("nb_land", "Run metadata-driven ingestion into Landing",
      ('''expected_environment = "dev"
batch_id = ""
dataset_id = ""
run_group = ""''', ["parameters"]),
      ((ROOT / "src/engine.py").read_text(), []),
      ('''
import notebookutils
from pyspark.sql import functions as F

library = notebookutils.variableLibrary.getLibrary("vl_ingestion")
environment = library.getVariable("environment")
if environment != expected_environment:
    raise ValueError("Active environment differs from requested environment")
if bool(dataset_id) == bool(run_group):
    raise ValueError("Supply exactly one of dataset_id or run_group")
token(batch_id, r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}")

engine = Landing(spark, notebookutils, library.getVariable("client_code"), environment,
                 library.getVariable("landing_root"), library.getVariable("vault_url"))
if dataset_id:
    datasets = [dataset_id]
else:
    token(run_group)
    rows = (engine.table_rows("dataset", enabled=True, run_group=run_group)
            .select("dataset_id").distinct().orderBy("dataset_id").collect())
    datasets = [row.dataset_id for row in rows]
if not datasets:
    raise ValueError("No matching enabled datasets")

outcomes = {}
for dataset in datasets:
    try:
        outcomes[dataset] = engine.run(dataset, batch_id)
    except Exception as error:
        outcomes[dataset] = f"failed: {type(error).__name__}"
print(json.dumps(outcomes))
if any(status.startswith("failed") for status in outcomes.values()):
    raise RuntimeError("One or more datasets failed; inspect outcomes and the Landing ledger")
'''.strip(), []))

quality_ddl = (ROOT / "metadata/poc_quality.sql").read_text()
write("nb_poc_setup", "Register the synthetic Fabric POC dataset",
      ('''
import json
import notebookutils

library = notebookutils.variableLibrary.getLibrary("vl_ingestion")
if library.getVariable("environment") != "dev":
    raise ValueError("The POC setup runs in DEV")
workspace_id = library.getVariable("workspace_id")
lakehouse_id = library.getVariable("lakehouse_id")
root = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/poc_source"
if not notebookutils.fs.mkdirs("Files/poc_source/orders"):
    raise OSError("Could not prepare the synthetic source folder")
options = json.dumps({"root": root})
spark.createDataFrame([("poc_onelake", "dev", "adls", options)],
    "connection_id string, environment string, connector string, options_json string"
).createOrReplaceTempView("poc_connection")
spark.createDataFrame([("poc__orders","poc_onelake","file_incremental",True,"poc",
                       json.dumps({"prefix":"orders","glob":"*.csv"}))],
    "dataset_id string, connection_id string, load_mode string, enabled boolean, run_group string, options_json string"
).createOrReplaceTempView("poc_dataset")
spark.sql("""MERGE INTO ops.connection t USING poc_connection s
    ON t.connection_id = s.connection_id AND t.environment = s.environment
    WHEN NOT MATCHED THEN INSERT *""")
spark.sql("""MERGE INTO ops.dataset t USING poc_dataset s
    ON t.dataset_id = s.dataset_id WHEN NOT MATCHED THEN INSERT *""")
'''.strip() + f"\nfor statement in {quality_ddl!r}.split(';'):\n    if statement.strip():\n        spark.sql(statement)", []))
write("nb_poc_quality", "Evaluate the quality gate for a published CSV batch",
      ('expected_environment = "dev"\ndataset_id = "poc__orders"\nbatch_id = "poc_001"', ["parameters"]),
      ((ROOT / "src/poc_quality.py").read_text(), []),
      ('''
import notebookutils

library = notebookutils.variableLibrary.getLibrary("vl_ingestion")
environment = library.getVariable("environment")
if environment != expected_environment:
    raise ValueError("Unexpected environment")
validate_orders(spark, dataset_id, batch_id, library.getVariable("client_code"), environment)
'''.strip(), []))
