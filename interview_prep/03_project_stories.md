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
> "The pipeline starts at SQL Server on-premise. SQL Server CDC tracks every insert, update, and delete at the row level by reading the transaction log — no triggers, no polling, just log-based change capture. Those change events land in S3 as Parquet files.
>
> Auto Loader picks them up from S3 using file notifications — S3 event notifications trigger it the moment a new file lands, so it doesn't have to list the directory every run. It's push-based, not poll-based. Auto Loader feeds into Lakeflow Declarative Pipelines.
>
> The silver layer applies SCD2 using `APPLY CHANGES INTO`. For each table it tracks inserts and updates with `__IS_CURRENT`, `__START_AT`, `__END_AT`, and `__SK_ID` columns, giving full row history. This layer covers 147 TRUX tables and is managed by the platform team.
>
> My ownership is the gold layer. I take silver tables and build business-ready dimensional models — joining dimension tables, renaming columns to business terminology, filtering to current records using `__IS_CURRENT = true`. I write these as PySpark DataFrame transforms deployed via Asset Bundles to Unity Catalog. The gold layer feeds Tableau directly."

### When They Probe Deeper

**"How exactly does CDC work?"**
> "SQL Server CDC reads the transaction log — every committed transaction writes to the log before hitting the actual table. CDC captures those log entries as change events: the operation type (insert/update/delete), the before and after image of the row, and a sequence number. It's asynchronous — it doesn't slow down the source table."

**"How does Auto Loader work?"**
> "Auto Loader has two modes. File notification mode — which we use — sets up S3 event notifications so the moment a file lands, Auto Loader is triggered. Directory listing mode polls the folder on a schedule. File notification is more efficient at scale. Auto Loader tracks processed files via a checkpoint so it never reprocesses the same file, even after a restart."

**"How does the SCD2 silver layer work?"**
> "Lakeflow uses `APPLY CHANGES INTO`. It takes the CDC events and maintains a history table — when a record changes, the old row gets `__IS_CURRENT` set to false and `__END_AT` stamped, and a new current row is inserted. The surrogate key `__SK_ID` uniquely identifies each version."

**"What does your gold layer actually do?"**
> "I rewrite SQL string transforms as PySpark DataFrames. The transforms include multi-table joins — joining driver, vehicle, and route dimension tables. Window functions for latest-record deduplication. Date filtering — for example the production statistics table only includes the last 24 months of data. Column renames to align with business terminology. All tables filter `__IS_CURRENT = true` from silver before any transformation."

### Project Details
- Company: Waste Connections (current role)
- Stack: SQL Server CDC → AWS Glue → S3 → Auto Loader → Lakeflow DLT (SCD2 silver) → PySpark gold → Unity Catalog → Tableau
- Silver layer: 147 TRUX tables, Lakeflow Declarative Pipelines, SCD2 via `APPLY CHANGES INTO`
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
| SQL Server CDC | Source/DBA team | How it works conceptually — reads transaction log, captures change events |
| AWS Glue / S3 landing | Platform team | Files land as Parquet snapshots in S3 |
| Auto Loader + Lakeflow silver | Platform team | How it works — file notifications, checkpoint, `APPLY CHANGES INTO` SCD2 |
| Gold layer PySpark transforms | Me | Full ownership — wrote, deploy, maintain |
| Asset Bundles deployment | Me | `databricks bundle deploy --target local` |
| Unity Catalog tables | Me | Write gold layer tables here |
| Tableau reports | Me (analyst hat) | Gold layer feeds these directly |

**The right framing:** "I own the gold layer end to end. The silver layer is managed by the platform team — I consume it. But I understand how the full pipeline works because I need to know what's in the silver tables to build on top of them correctly."
