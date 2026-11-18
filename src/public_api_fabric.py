"""Run the public API POC using an existing Landing engine and Fabric session."""
import json

from engine import token
from public_api_demo import earthquake_rows, population_rows

from pyspark.sql import functions as F


def register_public_sources(engine, sources):
    spark = engine.spark
    for dataset, options in sources.items():
        connection = (dataset, "dev", "api", json.dumps({"base_url": options["base_url"]}))
        job = (dataset, dataset, "full", True, "public_api_demo",
               json.dumps({"path": options["path"], "params": options["params"]}))
        for table, row, schema, condition in (
            ("connection", connection,
             "connection_id string, environment string, connector string, options_json string",
             "t.connection_id = s.connection_id AND t.environment = s.environment"),
            ("dataset", job,
             "dataset_id string, connection_id string, load_mode string, enabled boolean, run_group string, options_json string",
             "t.dataset_id = s.dataset_id"),
        ):
            spark.createDataFrame([row], schema).createOrReplaceTempView("public_api_seed")
            spark.sql(f"""MERGE INTO ops.{table} t USING public_api_seed s ON {condition}
                          WHEN NOT MATCHED THEN INSERT *""")
            filters = ({"connection_id": dataset, "environment": "dev"} if table == "connection"
                       else {"dataset_id": dataset})
            existing = engine.one(table, **filters)
            fields = spark.table("public_api_seed").columns
            if tuple(existing[field] for field in fields) != row:
                raise ValueError(f"{dataset}: existing POC metadata differs from the demo definition")


def read_published_page(engine, dataset, batch):
    published = engine.history(dataset, "succeeded")
    events = published.where(F.col("batch_id") == batch).limit(2).collect()
    if len(events) != 1 or events[0].object_count != 1:
        raise ValueError(f"{dataset}: expected one successful Landing attempt and one payload")
    event = events[0]
    page = event.manifest_path.rsplit("/", 1)[0] + "/page_000001.json"
    chunks = engine.spark.read.text(page, wholetext=True).take(2)
    if len(chunks) != 1:
        raise ValueError(f"{dataset}: expected one landed JSON document")
    return event.attempt_id, page, chunks[0].value


def run_public_demo(engine, sources, batch):
    spark = engine.spark
    token(batch, r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}")
    if engine.environment != "dev":
        raise ValueError("Run the public API demo in DEV")
    register_public_sources(engine, sources)
    for dataset in sources:
        print(dataset, engine.run(dataset, batch))

    for schema in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    def publish(frame, table):
        scoped = frame.withColumn("client", F.lit(engine.client)).withColumn("batch_id", F.lit(batch))
        scoped.limit(0).write.format("delta").mode("ignore").saveAsTable(table)
        predicate = f"client = '{engine.client}' AND batch_id = '{batch}'"
        (scoped.write.format("delta").mode("overwrite")
         .option("replaceWhere", predicate).saveAsTable(table))

    pages = [(dataset, *read_published_page(engine, dataset, batch)) for dataset in sources]
    bronze = spark.createDataFrame(
        pages, "dataset_id string, landing_attempt_id string, source_path string, payload_json string")
    publish(bronze, "bronze.public_api_payloads")

    payloads = {dataset: json.loads(payload) for dataset, _, _, payload in pages}
    try:
        earthquakes = earthquake_rows(payloads["usgs_earthquakes"])
        population = population_rows(payloads["worldbank_india_population"])
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        print(f"Quality gate failed for {batch}: {error}")
        raise
    print(f"Quality gate passed: {len(earthquakes)} earthquake events, {len(population)} population years")

    quake_frame = spark.createDataFrame(
        earthquakes, "event_id string, event_utc timestamp, event_date date, magnitude double, "
                     "longitude double, latitude double, depth_km double")
    population_frame = spark.createDataFrame(population, "country_code string, year int, population long")
    publish(quake_frame, "silver.public_api_earthquakes")
    publish(population_frame, "silver.public_api_population")

    quake_metrics = (quake_frame.groupBy("event_date").agg(
        F.count("*").alias("earthquake_count"), F.max("magnitude").alias("max_magnitude"))
        .selectExpr("'USGS' AS source", "cast(event_date as string) AS period", "'World' AS geography",
                    "stack(2, 'earthquake_count', cast(earthquake_count as double), "
                    "'max_magnitude', max_magnitude) AS (metric, value)"))
    population_metrics = population_frame.selectExpr(
        "'World Bank' AS source", "cast(year as string) AS period", "country_code AS geography",
        "'population' AS metric", "cast(population as double) AS value")
    metrics = quake_metrics.unionByName(population_metrics)
    publish(metrics, "gold.public_api_metrics")
    return (spark.table("gold.public_api_metrics")
            .where((F.col("client") == engine.client) & (F.col("batch_id") == batch))
            .orderBy("source", "period", "metric"))
