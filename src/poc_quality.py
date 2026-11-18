"""Evaluate configured rules on a published, batch-scoped CSV delivery."""
from uuid import uuid4

from pyspark.sql import functions as F


def validate_orders(spark, dataset_id, batch_id, client, environment):
    events = (spark.table("ops.landing_event")
              .where((F.col("dataset_id") == dataset_id) & (F.col("batch_id") == batch_id)
                     & (F.col("client") == client) & (F.col("environment") == environment)
                     & (F.col("status") == "succeeded")).limit(2).collect())
    if len(events) != 1:
        raise ValueError("Expected exactly one published Landing event")
    event = events[0]
    if not event.object_count:
        print("No new files in this batch. No quality evaluation needed.")
        return
    path = event.manifest_path.rsplit("/", 1)[0]
    frame = (spark.read.option("header", True).option("mode", "FAILFAST")
             .option("pathGlobFilter", "*.csv").csv(path)).cache()
    schema = ("dataset_id string, batch_id string, landing_attempt_id string, "
              "evaluation_id string, rule_id string, measured long, threshold long, "
              "severity string, status string")
    try:
        rules = (spark.table("ops.quality_rule")
                 .where((F.col("dataset_id") == dataset_id) & F.col("enabled"))
                 .orderBy("rule_id").collect())
        if not rules or len({rule.rule_id for rule in rules}) != len(rules):
            raise ValueError("Expected enabled rules with unique rule IDs")
        evaluation = uuid4().hex
        results = []
        for rule in rules:
            if rule.severity not in {"warn", "fail"} or rule.threshold < 0:
                raise ValueError("Invalid quality rule threshold or severity")
            if rule.rule_type == "min_rows":
                measured = frame.count()
                passed = measured >= rule.threshold
            else:
                column = rule.column_name
                if column not in frame.columns:
                    raise ValueError(f"Required quality column missing: {column}")
                value = F.col(column)
                if rule.rule_type == "null_or_blank":
                    measured = frame.where(value.isNull() | (F.trim(value) == "")).count()
                elif rule.rule_type == "duplicate_keys":
                    measured = (frame.groupBy(value).agg(F.count(F.lit(1)).alias("_key_count"))
                                .where(F.col("_key_count") > 1).count())
                elif rule.rule_type in {"invalid_date", "invalid_long"}:
                    escaped = column.replace("`", "``")
                    target = "date" if rule.rule_type == "invalid_date" else "bigint"
                    measured = frame.where(F.expr(f"try_cast(`{escaped}` AS {target})").isNull()).count()
                else:
                    raise ValueError(f"Unsupported quality rule type: {rule.rule_type}")
                passed = measured <= rule.threshold
            results.append((dataset_id, batch_id, event.attempt_id, evaluation, rule.rule_id,
                            measured, rule.threshold, rule.severity,
                            "passed" if passed else rule.severity))
        (spark.createDataFrame(results, schema).withColumn("recorded_utc", F.current_timestamp())
         .write.format("delta").mode("append").saveAsTable("ops.quality_result"))
        if any(result[-1] == "fail" for result in results):
            raise ValueError("Quality gate failed. See ops.quality_result")
        print(f"Quality gate passed for {dataset_id}/{batch_id}: {evaluation}")
    finally:
        frame.unpersist()
