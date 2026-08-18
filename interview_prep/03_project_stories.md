# Project Stories — Databricks DE Interviews

---

## THE FRAMEWORK — Use SPIT for every story
- **S**ource — where does data come from
- **P**rocess — how is it moved and transformed
- **I**ssues — what problem existed, what you solved
- **T**arget — where it lands, who uses it

**Always lead with a one-sentence concept before details.**

---

## Story 1 — Current Role: Waste Connections Databricks Gold Layer

### One-liner
"I'm building the gold layer on a Databricks Lakeflow pipeline — PySpark DataFrame transforms on top of a CDC-driven SCD2 silver layer, deployed via Asset Bundles to Unity Catalog."

### Full Story
> "The pipeline starts at SQL Server on-premise. The source uses SQL Server Change Tracking — not CDC. Change Tracking is lighter than CDC: it records that a row changed and its primary key, but not the full before/after image. It's sufficient here because the snapshot provides the full row data on the initial load, and CT only needs to track what changed after that.
>
> Two types of files land in S3 as Parquet: snapshot files — a full table extract run once — and change tracking files that arrive incrementally as rows change in SQL Server.
>
> The silver layer has a two-flow design per table. Auto Loader reads both file types as streaming sources using `cloudFiles` format with `addNewColumns` schema evolution — so if a new column appears in the source, the pipeline adapts automatically without breaking. A `rescuedDataColumn` catches any fields that don't fit the current schema rather than losing them.
>
> The first flow — the initial load — reads the snapshot files and runs once using `once=True`. It seeds the SCD2 staging table with the full historical state. The second flow — the CT flow — runs continuously, picking up change tracking files as they arrive. It maps SQL Server change tracking operation codes to DLT actions: operation 1 means delete, operation 3 is the before-image of an update which gets filtered out to avoid double-processing.
>
> Both flows feed into `create_auto_cdc_flow` which implements SCD2 — each flow passes a `sequence_by` struct containing commit time and LSN values so Databricks can order changes correctly and maintain the `__IS_CURRENT`, `__START_AT`, `__END_AT`, `__SK_ID` columns. LSN — Log Sequence Number — is SQL Server's transaction ordering mechanism, ensuring changes are applied in the exact order they happened at the source.
>
> This layer covers 147 TRUX tables and is managed by the platform team. My ownership is the gold layer — I take the silver SCD2 tables, filter `__IS_CURRENT = true`, join dimension tables, rename columns to business terminology, and deploy as PySpark DataFrame transforms via Asset Bundles to Unity Catalog. The gold layer feeds Tableau directly."

### When They Probe Deeper

**"What's the difference between CDC and Change Tracking?"**
> "CDC — Change Data Capture — reads the SQL Server transaction log and captures the full before and after image of every changed row. Change Tracking is lighter — it only records that a row changed and which primary key was affected, not the full row content. We use Change Tracking because the snapshot gives us the full initial state; incremental updates only need the key to know what to re-fetch or apply."

**"Why two separate flows — snapshot and CT?"**
> "The snapshot is a one-time full load that seeds the SCD2 table with all historical data. Running it continuously would re-process every row on every run — imagine extracting 10 million rows every hour just to find 500 that changed. The CT flow then takes over for ongoing changes — it's incremental and continuous. The `once=True` flag on the snapshot flow ensures it runs exactly once and never again."

**"Can you walk me through a concrete example?"**
> "Say the source is a `drivers` table with 3 rows on day 1. The snapshot runs once and loads all 3 rows into the SCD2 silver table — all with `__IS_CURRENT = true`.
>
> On day 5, Bob's status changes to Inactive in SQL Server. Change Tracking records that driver_id 2 changed, along with the operation code 4 — after-image of an update. The CT Parquet file that lands in S3 contains the full updated row. The CT flow picks it up, closes Bob's old row by setting `__IS_CURRENT = false` and stamping `__END_AT`, and inserts a new current row with the updated status.
>
> On day 8, Charlie is deleted. CT records operation code 1 — delete. The CT flow sees `apply_as_deletes` is set for operation 1, so Charlie's row gets closed with no new row inserted — he exists in history but has no current record.
>
> At any point the silver table has the full picture — because it was seeded from the snapshot and every change since has been applied on top of it in order using the LSN sequence."

**"How does Auto Loader work here?"**
> "Auto Loader uses the `cloudFiles` format in Databricks. It reads Parquet files from S3 as a streaming source — it detects new files automatically and only processes files it hasn't seen before using a checkpoint. We use `addNewColumns` schema evolution so new columns from the source are automatically added to the schema without pipeline failures. `rescuedDataColumn` captures any data that doesn't match the current schema into a side column rather than losing it."

**"What is an LSN and why does it matter?"**
> "LSN stands for Log Sequence Number — it's SQL Server's way of ordering every transaction in the transaction log. In the sequence_by struct we pass both commit_time and LSN to `create_auto_cdc_flow`. This ensures that if two changes happen in the same millisecond, they're still applied in the correct order. Without LSN ordering you could get race conditions where an update gets applied before the insert it depends on."

**"What is operation 3 and why do you filter it?"**
> "SQL Server Change Tracking operation codes: 1 = delete, 2 = insert, 3 = before-image of an update, 4 = after-image of an update. We filter out operation 3 — the before-image — because we only want to apply the final state of a change, not the intermediate state. Applying operation 3 would incorrectly revert rows to their pre-update values."

**"How does the SCD2 silver layer work?"**
> "Both flows feed `create_auto_cdc_flow` which manages the SCD2 logic. When a row changes, the old version gets `__IS_CURRENT` set to false and `__END_AT` stamped with the change time. A new current row is inserted with `__IS_CURRENT = true` and a new `__SK_ID` surrogate key. The sequence_by struct — commit_time + LSN + seqval — ensures changes are applied in exactly the right order even under high-volume concurrent writes."

**"What does your gold layer actually do?"**
> "I rewrite SQL string transforms as PySpark DataFrames. The transforms include multi-table joins — joining driver, vehicle, and route dimension tables. Window functions for latest-record deduplication. Date filtering — for example the production statistics table only includes the last 24 months of data. Column renames to align with business terminology. All tables filter `__IS_CURRENT = true` from silver before any transformation."

### Project Details
- Company: Waste Connections (current role)
- Stack: SQL Server Change Tracking → AWS Glue → S3 (snapshot + CT Parquet files) → Auto Loader (cloudFiles streaming) → Lakeflow DLT (SCD2 silver via `create_auto_cdc_flow`) → PySpark gold → Unity Catalog → Tableau
- Silver layer: 147 TRUX tables, two flows per table (snapshot `once=True` + continuous CT), SCD2 columns: `__IS_CURRENT`, `__START_AT`, `__END_AT`, `__SK_ID`
- CT operation codes: 1=delete, 2=insert, 3=before-image (filtered out), 4=after-image
- Sequence ordering: `sequence_by` struct with `__commit_time` + `__$start_lsn` + `__$seqval`
- Schema evolution: `addNewColumns` mode + `rescuedDataColumn` for schema drift safety
- Gold layer: 26 Python files (12 dims, 14 facts) under `src/TRUX_TABLEAU/transformations/gold/`
- Deploy: `databricks bundle deploy --target local`
- Gold layer rewrites completed: `trux_dim_driver`, `trux_dim_vehicle`, `trux_fact_route_production_statistics`, `trux_fact_invoice_detail`, `trux_fact_getcost`
- Pipeline status: TRUX_TABLEAU pipeline currently paused, work ongoing

---

## Story 2 — Pearl Technologies: Metadata-Driven Airflow ETL

### One-liner
"I built 80 Airflow DAGs using a metadata-driven incremental pattern to move data from multiple sources into Snowflake."

### Full Story
> "At Pearl Technologies I was building incremental ETL pipelines from multiple SQL Server and Azure SQL sources into Snowflake using Airflow. The initial design ran full table extracts every run. As tables grew, this became slow and was putting unnecessary load on source databases during business hours.
>
> The problem was we had no consistent way to track what had changed since the last run — some tables had updated_at timestamps, some had auto-increment IDs, some had composite keys and no reliable watermark.
>
> I designed a metadata table in Snowflake that tracked, per source table, the last successful run timestamp and the watermark column to use. Each DAG run reads the metadata first, generates a dynamic incremental query — `WHERE updated_at > last_run_time` — executes only against changed rows, writes to a Snowflake staging table, calls a stored procedure to merge staging into the final table, then updates the metadata on success.
>
> This reduced average pipeline runtime significantly and eliminated the source load problem. It also made the system self-documenting — you could query the metadata table to see exactly when each table last ran."

### When They Probe Deeper

**"What were the sources?"**
> "Azure SQL, SQL Server, MongoDB with nested JSON flattening, and a Workday XML API. The Workday pipeline called the API, received XML, parsed it with Python's `xml.etree` module, structured it into a DataFrame, wrote to staging, then a stored procedure handled the final insert."

**"How did you handle tables with no timestamp?"**
> "For tables with no watermark column we fell back to a full extract wrapped in a transaction — truncate staging then insert, inside one transaction. Not ideal but idempotent — running it twice produced the same result."

**"How did you manage 80 DAGs without 80 different codebases?"**
> "The metadata-driven design meant adding a new source was just adding a row to the config table — the DAG code was the same. We had frequency-based DAG separation: 5-minute, hourly, and daily DAGs. The config table had a frequency column, so each DAG only picked up sources matching its schedule."

### Project Details
- Company: Pearl Technologies, Brampton (Nov 2023 – present on resume)
- Stack: Azure SQL / SQL Server / MongoDB / Workday API → Airflow → Snowflake staging → SP merge
- ~80 DAGs, metadata-driven incremental loads
- Watermark tracked per source table in Snowflake metadata table
- Frequency-based DAG separation: 5min, hourly, daily
- Python patterns used: requests, xml.etree, pandas, PyMongo, json_normalize

---

## Story 3 — dbt Production Project (TRUX SQL Server)

### One-liner
"I built a full dbt project on SQL Server — 56 models, 34 macros, multi-tenant across 40+ source databases — running in production via GitHub Actions and powering a live Tableau report."

### Full Story
> "dbt is a transformation framework that brings software engineering practices to SQL — version control, testing, documentation, dependency management. You write each transformation as a plain SQL SELECT, dbt handles the CREATE TABLE or INSERT logic. Models reference other models and dbt resolves the execution order.
>
> What dbt does NOT do: it doesn't move data. It only transforms data already in your warehouse. You still need an ingestion tool for that.
>
> I built a production dbt project called `dbt_truxsql_prod` on SQL Server. The complexity came from multi-tenancy — 40+ customer databases with the same schema but separate data. I built a macro framework where a single macro loops over all target databases and generates the correct SQL for each one. 34 macros covered patterns like incremental merge, delete+insert, full refresh, and dynamic date filtering.
>
> The project had 4 load strategies — full refresh, incremental merge, append, and delete+insert — each implemented as a macro so any model could adopt a strategy in one line. CI/CD ran via GitHub Actions on a self-hosted runner: pull requests triggered model compilation and tests, merges to main triggered deployment.
>
> It was rejected org-wide not because of quality, but because the company had already committed to Databricks Lakeflow — dbt models can't be introduced into a closed Lakeflow pipeline. It still runs in production."

### When They Probe Deeper

**"Why not use dbt-databricks instead?"**
> "The silver layer is managed inside Lakeflow's closed system — assets are bound to the DLT pipeline and can't be replaced by external tools. The gold layer was my entry point, and I chose PySpark there to align with the platform team's approach and to build Spark skills. For a greenfield Databricks project I would absolutely use dbt-databricks — the adapter is nearly identical to dbt-sqlserver."

**"What tests did you have?"**
> "Not_null and unique tests on primary keys across all models. I was aware of schema tests and freshness tests but hadn't extended beyond the basics in this project — that's something I'd add in a more mature setup."

### Project Details
- Project name: `dbt_truxsql_prod`
- Location: `C:\Users\351786\Documents\dbt_TRUX_SQL_Server_Prod\dbt_truxsql_prod`
- Status: Running in production via GitHub Actions, powering a live Tableau report
- 56 models, 34 macros
- 4 targets (multi-tenant), 40+ source databases
- 4 load strategies: full refresh, incremental merge, append, delete+insert
- CI/CD: GitHub Actions on self-hosted runner — PR triggers compile + test, merge triggers deploy
- Rejected org-wide due to pre-existing Databricks investment, not quality

---

## Story 4 — Problem You Faced and Solved

### One-liner
"I had a performance problem with full table extracts in Airflow, and solved it with a metadata-driven incremental pattern."

### Full Story
> "At Pearl Technologies the initial pipeline design ran full table extracts on every Airflow run. As source tables grew to millions of rows, pipeline runtimes ballooned and we were hitting source database CPU limits during business hours.
>
> The root cause was no incremental state — each run had no memory of what it had already processed. I needed a way to track the high-water mark per table without modifying the source systems.
>
> I built a metadata table in Snowflake with one row per source table — tracking the watermark column name, last successful run timestamp, and row counts. Every DAG run starts by querying this table, generates `WHERE updated_at > last_run_time`, processes only changed rows, then updates the metadata on success. If a run fails, the metadata isn't updated so the next run re-processes from the last clean state — making it self-healing.
>
> Runtime dropped significantly and source load issues disappeared. As a side benefit the metadata table became an operational dashboard — you could see at a glance which tables were current and which were behind."

---

## Behavioral Questions

### "Why are you leaving your current role?"
> "My current role is focused on the analyst side — SQL, Tableau, reporting. The Databricks work I'm doing is adjacent to my core responsibilities, not the primary focus. I want a role where data engineering is the main job, not a side project."

### "Where do you want to be in 2 years?"
> "Senior Data Engineer. I want to own end-to-end pipelines at scale — ingestion through to serving layer — and start contributing to platform decisions like architecture and tooling selection."

### "Tell me about a time a pipeline broke in production."
> "A source schema changed — a column was renamed — and the pipeline failed on the next run. The immediate fix was updating the column reference. The longer-term fix was adding a schema validation step at the start of the pipeline that compares the incoming schema to the expected schema and fails loudly before bad data propagates downstream. Silent failures are worse than loud ones in data pipelines."

### "What's your biggest technical gap right now?"
> "Low-level Hadoop administration — HDFS, YARN. I've worked with systems built on that stack but haven't administered a cluster. For most modern Databricks roles that's managed infrastructure anyway. On the Spark side I'm actively building PySpark depth through production work at my current role."

---

## Quick Reference — One-Line Concept Openers

| Topic | Open with |
|---|---|
| Current pipeline | "The pipeline moves operational data from SQL Server to Tableau through bronze, silver, and gold layers on Databricks." |
| Airflow project | "I built metadata-driven incremental pipelines from multiple sources into Snowflake using Airflow." |
| dbt project | "I built a production dbt project with 56 models, multi-tenant across 40+ databases, running in CI/CD via GitHub Actions." |
| Lakeflow | "It's a declarative pipeline framework — you define what tables should look like, Databricks handles execution order." |
| dbt explanation | "dbt brings software engineering practices — testing, versioning, dependency management — to SQL transformations." |
| Problem solved | "I had a performance problem with full table extracts and solved it with a metadata-driven incremental pattern." |

---

## What I Own vs What I Don't (be honest about this)

| Layer | Owner | What I know about it |
|---|---|---|
| SQL Server Change Tracking | Source/DBA team | CT records key + operation code per changed row. Operation codes: 1=delete, 2=insert, 3=before-image (filtered), 4=after-image. Lighter than CDC — no full row capture needed because snapshot seeds the initial state. |
| AWS Glue / S3 landing | Platform team | Two file types land in S3: snapshot Parquet (full extract, once) and CT Parquet (incremental, ongoing) |
| Auto Loader + Lakeflow silver | Platform team | cloudFiles streaming, addNewColumns schema evolution, rescuedDataColumn, two flows per table (snapshot once=True + continuous CT), SCD2 via create_auto_cdc_flow with LSN-based sequencing |
| Gold layer PySpark transforms | Me | Full ownership — wrote, deploy, maintain |
| Asset Bundles deployment | Me | `databricks bundle deploy --target local` |
| Unity Catalog tables | Me | Write gold layer tables here |
| Tableau reports | Me (analyst hat) | Gold layer feeds these directly |

**The right framing:** "I own the gold layer end to end. The silver layer is managed by the platform team — I consume it. But I understand how the full pipeline works because I need to know what's in the silver tables to build on top of them correctly."
