"""Metadata-driven batch landing for Fabric Spark."""
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import PurePosixPath
from urllib.parse import urljoin, urlsplit
from uuid import uuid4
import json
import re

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def token(value, pattern=r"[a-z][a-z0-9_]{0,79}"):
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValueError("Invalid metadata identifier")
    return value


def utc(value):
    moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        raise ValueError("Timestamp must include UTC offset")
    return moment.astimezone(timezone.utc)


def iso(moment):
    return moment.astimezone(timezone.utc).isoformat(timespec="microseconds")


def lsn(value):
    return "0x" + token(value, r"[0-9A-Fa-f]{20}")


def sql_id(value):
    return "[" + token(value, r"[A-Za-z_][A-Za-z0-9_]{0,127}") + "]"


def json_path(data, path):
    for part in path.split("."):
        data = data[part]
    return data


class Landing:
    def __init__(self, spark, utils, client, environment, root, vault_url):
        self.spark, self.utils = spark, utils
        self.client, self.environment = token(client), token(environment)
        if environment not in {"dev", "uat", "prod"}:
            raise ValueError("Unsupported environment")
        if not re.fullmatch(r"Files/[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", root):
            raise ValueError("Landing root must be beneath Lakehouse Files")
        self.root, self.vault = root, vault_url

    def secret(self, name):
        return self.utils.credentials.getSecret(self.vault, name)

    def table_rows(self, table, **filters):
        from pyspark.sql import functions as F
        frame = self.spark.table("ops." + table)
        for column, value in filters.items():
            frame = frame.where(F.col(column) == value)
        return frame

    def one(self, table, **filters):
        rows = self.table_rows(table, **filters).limit(2).collect()
        if len(rows) != 1:
            raise ValueError(f"Expected one matching row in ops.{table}")
        return rows[0]

    def history(self, dataset, status):
        return self.table_rows("landing_event", client=self.client, environment=self.environment,
                               dataset_id=dataset, status=status)

    def event(self, dataset, batch, attempt, status, plan, path=None, count=None, error=None):
        from pyspark.sql import functions as F
        row = (self.client, self.environment, dataset, batch, attempt, status,
               json.dumps(plan, sort_keys=True), path, count, error)
        schema = ("client string, environment string, dataset_id string, batch_id string, "
                  "attempt_id string, status string, plan_json string, manifest_path string, "
                  "object_count long, error_type string")
        (self.spark.createDataFrame([row], schema)
         .withColumn("recorded_utc", F.current_timestamp())
         .write.format("delta").mode("append").saveAsTable("ops.landing_event"))

    def jdbc(self, connection, query):
        return (self.spark.read.format("jdbc")
                .option("url", connection["url"])
                .option("driver", connection.get("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"))
                .option("user", connection["user"])
                .option("password", self.secret(connection["password_secret"]))
                .option("query", query)
                .option("fetchsize", "10000").load())

    def cdc_bounds(self, connection, config, previous):
        capture = token(config["capture_instance"], r"[A-Za-z_][A-Za-z0-9_]{0,99}")
        last = lsn(previous or config["initial_lsn"])
        row = self.jdbc(connection, f"""
            SELECT CONVERT(varchar(20),sys.fn_cdc_increment_lsn({last}),2) AS low,
                   CONVERT(varchar(20),sys.fn_cdc_get_max_lsn(),2) AS high,
                   CONVERT(varchar(20),sys.fn_cdc_get_min_lsn('{capture}'),2) AS minimum
        """).first()
        if not row.low or not row.high or not row.minimum or int(row.minimum, 16) == 0:
            raise ValueError("CDC capture instance or starting position is unavailable")
        if int(row.low, 16) < int(row.minimum, 16):
            raise ValueError("CDC retention gap. Coordinate a new baseline before resuming")
        if int(row.high, 16) < int(last, 16):
            raise ValueError("CDC high position is behind the committed checkpoint")
        return {"low": row.low, "high": row.high, "next_checkpoint": row.high}

    def file_status(self, path):
        hadoop_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
        filesystem = hadoop_path.getFileSystem(self.spark.sparkContext._jsc.hadoopConfiguration())
        status = filesystem.getFileStatus(hadoop_path)
        return status.getLen(), status.getModificationTime()

    def list_files(self, connection, config, dataset, mode):
        from fnmatch import fnmatchcase
        source_root = connection["root"].rstrip("/")
        if not source_root.startswith("abfss://"):
            raise ValueError("ADLS connector requires an abfss root or OneLake shortcut")
        if "?" in source_root or "#" in source_root:
            raise ValueError("Use identity-based storage access")
        prefix = config.get("prefix", "")
        if prefix:
            for part in prefix.split("/"):
                token(part, r"[A-Za-z0-9_-]+")
            source_root += "/" + prefix
        seen = set()
        if mode == "file_incremental":
            for row in self.history(dataset, "succeeded").select("plan_json").toLocalIterator():
                seen.update(item["version"] for item in json.loads(row.plan_json).get("files", []))
        pending = [source_root]
        files = []
        while pending:
            for entry in self.utils.fs.ls(pending.pop()):
                if entry.isDir:
                    if config.get("recursive", False):
                        pending.append(entry.path)
                    continue
                if not fnmatchcase(entry.name, config.get("glob", "*")):
                    continue
                if not re.fullmatch(r"[A-Za-z0-9_.-]+", entry.name):
                    raise ValueError("File names must be URL-safe")
                size, modified = self.file_status(entry.path)
                version = sha256(json.dumps([entry.path, size, modified]).encode()).hexdigest()
                if version not in seen:
                    files.append({"path": entry.path, "size": size,
                                  "modified": modified, "version": version})
                    if len(files) > int(config.get("max_files", 500)):
                        raise ValueError("File inventory exceeds per-batch limit. Narrow the source prefix")
        return sorted(files, key=lambda item: item["path"])

    def prepare(self, connector, mode, connection, config, dataset, previous):
        if mode == "watermark":
            low = utc(previous or config["initial_watermark"])
            delay = int(config.get("lookback_seconds", 0))
            lag = int(config.get("safety_lag_seconds", 0))
            if min(delay, lag) < 0:
                raise ValueError("Watermark lookback and lag must be nonnegative")
            high = datetime.now(timezone.utc) - timedelta(seconds=lag)
            if high <= low:
                raise ValueError("Watermark upper bound must exceed the committed position")
            return {"low": iso(low - timedelta(seconds=delay)), "high": iso(high),
                    "next_checkpoint": iso(high)}
        if mode == "sqlserver_cdc":
            return self.cdc_bounds(connection, config, previous)
        if connector == "adls":
            return {"files": self.list_files(connection, config, dataset, mode)}
        return {}

    def write_text(self, path, text):
        if not self.utils.fs.put(path, text, False):
            raise OSError("Lakehouse file write failed")

    def http(self, connector, connection, config, plan, path):
        endpoint = connection["base_url"].rstrip("/") + "/" + config["path"].lstrip("/")
        expected = urlsplit(connection["base_url"])

        def check_url(url, origin):
            actual = urlsplit(url)
            if (actual.scheme, actual.netloc) != ("https", origin.netloc) or actual.username:
                raise ValueError("HTTP URL must use the configured HTTPS origin")

        with requests.Session() as session:
            retry = Retry(total=4, status_forcelist=(429, 500, 502, 503, 504), backoff_factor=1,
                          allowed_methods=frozenset({"GET"}), respect_retry_after_header=True)
            session.mount("https://", HTTPAdapter(max_retries=retry))
            check_url(endpoint, expected)
            auth = connection.get("auth", "none")
            if auth == "bearer":
                session.headers["Authorization"] = "Bearer " + self.secret(connection["token_secret"])
            elif auth == "basic":
                session.auth = (connection["user"], self.secret(connection["password_secret"]))
            elif auth == "oauth_client_credentials":
                token_url = urlsplit(connection["token_url"])
                check_url(connection["token_url"], token_url)
                form = {"grant_type": "client_credentials"}
                if connection.get("scope"):
                    form["scope"] = connection["scope"]
                response = session.post(connection["token_url"], data=form, auth=(
                    connection["client_id"], self.secret(connection["client_secret"])),
                    timeout=60, allow_redirects=False)
                if response.status_code != 200:
                    raise RuntimeError(f"OAuth endpoint HTTP {response.status_code}")
                session.headers["Authorization"] = "Bearer " + response.json()["access_token"]
            elif auth != "none":
                raise ValueError("Unsupported HTTP authentication")

            original_params = dict(config.get("params", {}))
            if "low" in plan:
                original_params[config["from_param"]] = plan["low"]
                original_params[config["to_param"]] = plan["high"]
            params = dict(original_params)
            pagination = config.get("pagination", "none")
            if pagination not in {"none", "next_link", "cursor"}:
                raise ValueError("Unsupported pagination")
            extension = config.get("extension", "xml" if connector == "raas" else "json")
            token(extension, r"json|xml|csv|txt")
            for page in range(1, int(config.get("max_pages", 1000)) + 1):
                check_url(endpoint, expected)
                with session.get(endpoint, params=params, timeout=(15, 120),
                                 allow_redirects=False, stream=True) as response:
                    if response.status_code != 200:
                        raise RuntimeError(f"Source endpoint HTTP {response.status_code}")
                    body = bytearray()
                    max_bytes = int(config.get("max_page_bytes", 16 * 1024 * 1024))
                    for chunk in response.iter_content(chunk_size=65536):
                        body.extend(chunk)
                        if len(body) > max_bytes:
                            raise ValueError("HTTP response exceeds configured page size")
                    text = body.decode(config.get("encoding", "utf-8"))
                parsed = json.loads(text) if extension == "json" or pagination != "none" else None
                self.write_text(f"{path}/page_{page:06}.{extension}", text)
                if pagination == "none":
                    return page
                next_value = json_path(parsed, config["next_field"])
                if not next_value:
                    return page
                if pagination == "next_link":
                    endpoint, params = urljoin(endpoint, next_value), {}
                else:
                    params = {**original_params, config["cursor_param"]: next_value}
            raise ValueError("Pagination exceeded configured max_pages")

    def land(self, connector, mode, connection, config, plan, path):
        if connector in {"api", "raas"}:
            return self.http(connector, connection, config, plan, path)
        if connector == "adls":
            for item in plan["files"]:
                source = item["path"]
                name = PurePosixPath(urlsplit(source).path).name
                if self.file_status(source) != (item["size"], item["modified"]):
                    raise ValueError("Source file version changed after batch planning")
                target = f"{path}/{item['version']}_{name}"
                if not self.utils.fs.cp(source, target):
                    raise OSError("ADLS file copy failed")
            return len(plan["files"])
        if mode == "sqlserver_cdc":
            low, high = lsn(plan["low"]), lsn(plan["high"])
            if int(low, 16) > int(high, 16):
                return 0
            capture = sql_id(config["capture_instance"])
            # CDC function name embeds the capture instance.
            function = f"cdc.[fn_cdc_get_all_changes_{capture[1:-1]}]"
            query = f"SELECT * FROM {function}({low}, {high}, 'all update old')"
        else:
            table = ".".join(sql_id(part) for part in config["table"].split("."))
            query = f"SELECT * FROM {table}"
            if mode == "watermark":
                column = sql_id(config["watermark_column"])
                low, high = (utc(plan[key]).strftime("%Y-%m-%dT%H:%M:%S.%f") for key in ("low", "high"))
                query += (f" WHERE {column} >= CONVERT(datetime2, '{low}', 126)"
                          f" AND {column} < CONVERT(datetime2, '{high}', 126)")
        self.jdbc(connection, query).write.mode("errorifexists").parquet(f"{path}/data")
        return None

    def run(self, dataset, batch):
        dataset = token(dataset)
        batch = token(batch, r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}")
        successful = self.history(dataset, "succeeded")
        if successful.where(successful.batch_id == batch).limit(1).count():
            return "already_succeeded"
        job = self.one("dataset", dataset_id=dataset, enabled=True)
        source = self.one("connection", connection_id=job.connection_id,
                          environment=self.environment)
        connector, mode = source.connector, job.load_mode
        supported = {"api": {"full", "watermark"}, "raas": {"full", "watermark"},
                     "sqlserver": {"full", "watermark", "sqlserver_cdc"},
                     "adls": {"full", "file_incremental"}}
        if mode not in supported.get(connector, set()):
            raise ValueError("Unsupported connector and load mode combination")
        config, connection = json.loads(job.options_json), json.loads(source.options_json)
        signature = sha256(json.dumps([connector, mode, config, connection], sort_keys=True).encode()).hexdigest()
        # Operational contract: serialize all invocations for a dataset.
        latest = successful.orderBy("recorded_utc", ascending=False).first()
        previous = json.loads(latest.plan_json).get("next_checkpoint") if latest else None
        if latest and json.loads(latest.plan_json)["mode"] != mode:
            raise ValueError("Use a new dataset ID or migrate checkpoints before changing load mode")
        started = self.history(dataset, "started")
        pending = (started.select("batch_id").distinct()
                   .join(successful.select("batch_id").distinct(), "batch_id", "left_anti"))
        if pending.where(pending.batch_id != batch).limit(1).count():
            raise ValueError("Retry the unresolved batch before starting a new batch for this dataset")
        prior = started.where(started.batch_id == batch).orderBy("recorded_utc").first()
        if prior:
            plan = json.loads(prior.plan_json)
            if plan["signature"] != signature:
                raise ValueError("Metadata changed since the pending batch was planned")
        else:
            try:
                plan = self.prepare(connector, mode, connection, config, dataset, previous)
            except Exception as error:
                self.event(dataset, batch, uuid4().hex, "planning_failed",
                           {"mode": mode}, error=type(error).__name__)
                raise
            plan.update(signature=signature, mode=mode)
        attempt = uuid4().hex
        self.event(dataset, batch, attempt, "started", plan)
        root = f"{self.root}/{self.client}/{self.environment}/{dataset}/{batch}/{attempt}"
        try:
            if not self.utils.fs.mkdirs(root):
                raise OSError("Unable to create landing folder")
            count = self.land(connector, mode, connection, config, plan, root)
            manifest = f"{root}/_manifest.json"
            self.write_text(manifest, json.dumps({"dataset_id": dataset, "batch_id": batch,
                                                 "attempt_id": attempt, "connector": connector,
                                                 "plan": plan, "object_count": count,
                                                 "data_path": root}))
            self.event(dataset, batch, attempt, "succeeded", plan, manifest, count)
        except Exception as error:
            try:
                self.event(dataset, batch, attempt, "failed", plan, error=type(error).__name__)
            except Exception:
                pass
            raise RuntimeError(f"Landing failed for {dataset}/{batch}: {type(error).__name__}") from None
        return "succeeded"
