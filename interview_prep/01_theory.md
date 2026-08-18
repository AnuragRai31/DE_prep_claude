# Theory Reference — Databricks DE Interviews

---

## 1. Delta Lake

### One-liner
Delta Lake adds ACID transactions, time travel, and schema enforcement on top of Parquet files.

### Detail
Plain Parquet is immutable and has no transaction support — two concurrent writes can corrupt data, and you can't update or delete rows without rewriting the entire file. Delta Lake solves this by adding a transaction log (`_delta_log` folder) that records every write as a JSON entry. This enables:
- **ACID transactions** — writes fully succeed or fully roll back
- **Time travel** — query any previous version
- **Merge / update / delete** — row-level operations
- **Schema enforcement** — rejects writes that break the schema

### Key Commands
```sql
-- Time travel
SELECT * FROM catalog.schema.table VERSION AS OF 5
SELECT * FROM catalog.schema.table TIMESTAMP AS OF '2026-01-01'

-- Audit history
DESCRIBE HISTORY catalog.schema.table

-- Compact small files + co-locate by column
OPTIMIZE catalog.schema.table ZORDER BY (customer_id, date)

-- Delete old unreferenced files
VACUUM catalog.schema.table RETAIN 168 HOURS
```

```python
# Time travel in PySpark
df = spark.read.format("delta").option("versionAsOf", 5).load("path")

# Schema evolution — add new columns automatically
df.write.option("mergeSchema", "true").format("delta").mode("append").saveAsTable("...")

# Full schema replacement
df.write.option("overwriteSchema", "true").format("delta").mode("overwrite").saveAsTable("...")
```

### Gotchas
- Always OPTIMIZE before VACUUM — OPTIMIZE creates new compacted files, VACUUM cleans up the old ones
- VACUUM destroys time travel history beyond the retention window
- `mergeSchema` = additive only. `overwriteSchema` = full replacement, use carefully

---

## 2. Unity Catalog

### One-liner
Unity Catalog is Databricks' central governance layer — three-level namespace, access control, lineage, and audit logs across all workspaces.

### Detail
Before Unity Catalog, access control was per-workspace and used a two-level namespace (`database.table`). Unity Catalog adds a catalog on top: `catalog.schema.table`. This lets you manage permissions, lineage, and auditing centrally across every workspace in an organization.

### Three-Level Namespace
```
catalog . schema . table
   |         |       |
top-level  database  table
```

### Key Operations
```sql
-- Grant access
GRANT SELECT ON TABLE prod_catalog.finance.invoices TO `analyst@company.com`
GRANT ALL PRIVILEGES ON SCHEMA prod_catalog.finance TO `data_engineers`
```

### Features
- **Access control** — grant/revoke at catalog, schema, or table level
- **Data lineage** — column-level lineage tracked automatically
- **Audit logs** — who accessed what and when
- **Delta Sharing** — share tables externally without copying data

### Interview Line
"In my current role I write gold layer tables to Unity Catalog. The three-level namespace lets you manage access centrally rather than per-workspace."

---

## 3. Delta Lake vs Apache Iceberg

### One-liner
Both add ACID and time travel to a data lake. Delta Lake is optimized for Databricks. Iceberg is engine-agnostic.

### Comparison Table
| | Delta Lake | Apache Iceberg |
|---|---|---|
| Created by | Databricks | Netflix |
| Best with | Databricks / Spark | Spark, Trino, Flink, Hive |
| ACID | Yes | Yes |
| Time travel | Yes | Yes |
| Schema evolution | Yes | Yes |
| Partition evolution | Limited | Yes |
| Catalog | Unity Catalog, Hive | Hive, Glue, REST, Nessie |

### Interview Line
"Both solve the same core problem — ACID transactions and time travel on a data lake. Delta Lake is optimized for Databricks. Iceberg is better when multiple compute engines need to read the same tables. In my current role we use Delta Lake exclusively."

---

## 4. Lakeflow Declarative Pipelines (Delta Live Tables)

### One-liner
Declarative pipeline framework — you define what tables should look like, Databricks handles execution order, retries, and scaling.

### Detail
Instead of writing imperative code (read this → transform → write that → run next), you define each table as a function with a `@dlt.table` decorator. Databricks resolves the dependency graph automatically and runs tables in the right order.

Key features:
- **`@dlt.table`** — defines a table from a function's return value
- **`APPLY CHANGES INTO`** — implements SCD2 from CDC events automatically. Manages `__IS_CURRENT`, `__START_AT`, `__END_AT`, `__SK_ID` columns
- **Expectations (DQ)** — `CONSTRAINT valid_id EXPECT (id IS NOT NULL)` — warn, drop, or fail on violations
- **Auto Loader integration** — natively ingests files from cloud storage incrementally

### Interview Answer
"Lakeflow is Databricks' declarative pipeline framework. Instead of writing code that says run this then that, you define what each table should look like using decorators, and Databricks resolves dependencies and manages execution automatically. For CDC specifically, `APPLY CHANGES INTO` handles SCD2 — you give it the source, primary key, and sequence column and it maintains a full history table. Data quality is built in via expectations where you define constraints and choose whether violations warn, drop the row, or fail the pipeline. In my current role the silver layer uses Lakeflow to apply SCD2 across 147 tables from CDC events."

---

## 5. Databricks Workflows vs DLT vs Asset Bundles

### One-liner
DLT = declarative table definitions. Workflows = imperative job orchestrator. Asset Bundles = CI/CD packaging layer for both.

### Detail
| | DLT / Lakeflow | Workflows | Asset Bundles |
|---|---|---|---|
| What it is | Declarative pipeline framework | Job orchestrator | CI/CD packaging tool |
| You define | Table logic | Task execution order | Resources as code |
| Best for | Streaming + batch with complex dependencies | Orchestrating multiple job types | Deploying via Git/CLI |
| Analogy | dbt for Spark | Airflow (but Databricks-native) | Terraform for Databricks resources |

A Workflow can trigger a DLT pipeline as one of its tasks. Asset Bundles package and deploy both via `databricks bundle deploy --target prod`.

---

## 6. Medallion Architecture

### One-liner
Bronze = raw, Silver = clean + historical, Gold = business-ready.

### Detail
- **Bronze** — Raw ingestion, append-only, no transforms. Preserves source data exactly as received. Reprocessing always possible from here.
- **Silver** — Cleaned, deduplicated, standardized. SCD2 applied. Joins across sources may happen here.
- **Gold** — Business-ready aggregates and dimensional models. Filtered to current records. Feeds BI tools and applications.

### Your Stack
- Bronze: CDC events from SQL Server landing in S3 as Parquet
- Silver: Lakeflow DLT with `APPLY CHANGES INTO` SCD2 — 147 TRUX tables. Managed by platform team.
- Gold: Your layer — PySpark DataFrame transforms, dimension joins, column renames, `__IS_CURRENT = true` filter. Deployed via Asset Bundles to Unity Catalog. Feeds Tableau.

---

## 7. Spark Internals

### Lazy Evaluation
Transformations (`.filter`, `.select`, `.join`, `.groupBy`) build a DAG but execute nothing. An action (`.count()`, `.write`, `.collect()`) triggers execution. This lets Catalyst optimize the full plan before running.

### Narrow vs Wide Transformations
- **Narrow** — one input partition → one output partition. No shuffle. `.filter`, `.select`, `.withColumn`
- **Wide** — data moves between partitions. Shuffle happens. `.groupBy`, `.join`, `.distinct`, `repartition`

### Shuffle
Data movement between executor nodes to complete a wide operation. Causes disk I/O + network transfer. Most performance problems trace here. Minimize shuffles.

### Repartition vs Coalesce
- `repartition(n)` — full shuffle, can increase or decrease partition count
- `coalesce(n)` — no shuffle, can only decrease. Faster but may create uneven partitions.

### When to Cache
When the same DataFrame is used 2+ times downstream. Without cache, Spark recomputes from scratch every time.
```python
df.cache()                                    # memory + disk
df.persist(StorageLevel.DISK_ONLY)            # disk only
```

---

## 8. Performance Optimization

### One-liner
Filter early, broadcast small tables, cache reused DataFrames, coalesce before writes, avoid shuffles.

| Technique | When to use |
|---|---|
| Broadcast join | One table < ~100MB — eliminates shuffle on large table |
| Cache / persist | Same DataFrame used 2+ times downstream |
| Filter early | Push filters before joins to reduce data shuffled |
| Coalesce before write | Avoid thousands of tiny output files |
| Salting | Fix skewed joins — append random int to key |
| Tune shuffle partitions | Default 200 — reduce for small data, increase for large |

```python
from pyspark.sql.functions import broadcast
df_result = df_large.join(broadcast(df_small), on="id", how="inner")
```

---

## 9. ETL / Pipeline Design

### Idempotency
A pipeline that produces the same result whether run once or ten times. Achieved with Delta merge — running the same merge twice produces the same table state.

### SCD1 vs SCD2
- SCD1: Overwrite — no history, current value only
- SCD2: New row on change, history tracked via `start_date`, `end_date`, `is_current`

### Handling Late-Arriving Data
Use a watermark/metadata pattern — query `WHERE updated_at > last_run_time`. Late records are picked up in the next run. In Delta, a merge handles them correctly whenever they arrive.

### Handling Schema Changes
- New column added → `mergeSchema = true`
- Breaking change (rename, type change) → fail loudly, add schema validation upstream

### Batch vs Streaming
- Batch: data completeness matters, hourly/daily SLA, simpler
- Streaming: low latency required (seconds/minutes), event-driven

---

## 10. ACID

### One-liner
ACID guarantees writes are safe, consistent, and durable — critical in distributed systems where partial writes and concurrent access are the default.

### Each Letter
- **Atomicity** — write fully completes or fully rolls back. No partial writes.
- **Consistency** — data is always in a valid state after a transaction.
- **Isolation** — concurrent writes don't interfere with each other.
- **Durability** — once committed, data survives node failures.

---

## 11. File Formats

| Format | Columnar | Compressed | Schema | ACID | Best for |
|---|---|---|---|---|---|
| CSV | No | No | No | No | Raw ingestion, interchange |
| JSON | No | No | No | No | APIs, semi-structured |
| Parquet | Yes | Yes | Yes | No | Analytics, large data |
| Delta | Yes | Yes | Yes | Yes | Lakehouse, updates, history |

**Why Parquet over CSV:** Columnar — querying 3 of 50 columns reads only those 3. CSV reads everything. Also compressed by default.

**Why Delta over Parquet:** Adds ACID, time travel, merge/update/delete. Parquet is immutable — updating a row requires rewriting the whole file.

---

## 12. Auto Loader

### One-liner
Databricks tool for incremental file ingestion from cloud storage — detects new files automatically using cloud notifications.

### Detail
Two discovery modes:
- **File notification mode** — S3/ADLS event notifications push new file arrivals. More efficient at scale — push-based not poll-based.
- **Directory listing mode** — polls the folder on a schedule.

Uses a checkpoint to track processed files — never reprocesses the same file even after a restart.

### Interview Line
"Auto Loader uses cloud file notifications so the moment a file lands in S3, it's triggered — it doesn't have to list the directory every run. It tracks processed files via a checkpoint so restarts are safe."

---

## Quick-Fire Answers

| Question | Answer |
|---|---|
| What is the Delta transaction log? | `_delta_log` folder — one JSON file per transaction. Spark reads this to reconstruct any version. |
| OPTIMIZE before or after VACUUM? | OPTIMIZE first — creates compacted files. VACUUM cleans up the old unreferenced ones. |
| RANK vs DENSE_RANK? | RANK skips numbers after ties (1,1,3). DENSE_RANK doesn't (1,1,2). |
| left_anti join? | Returns rows from LEFT table with NO match in the right table. |
| Z-ORDER vs partitioning? | Partitioning = separate folders per value. Z-ORDER = co-locates similar rows within files for data skipping. |
| inferSchema in production? | Never — slow (full scan), can guess wrong. Always define explicit schema. |
| What triggers Spark execution? | An action: `.count()`, `.write`, `.collect()`, `.display()` |
| What is a shuffle? | Data movement between executor nodes — caused by wide transformations (groupBy, join). Most expensive operation. |

---

## Questions to Ask the Interviewer
1. "What does the current data stack look like — what's managed in Databricks vs outside it?"
2. "What's the biggest pain point in the current pipelines that this role would help solve?"
3. "How do you handle pipeline failures and SLA breaches — is there on-call rotation?"
