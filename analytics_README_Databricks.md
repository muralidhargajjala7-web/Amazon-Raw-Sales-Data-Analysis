```markdown
# Databricks integration - Amazon Raw Sales Data Analysis

This README explains how to run the Databricks-ready notebooks and jobs included in analytics/.

Files:
- databricks_batch_delta_ingest.py — Notebook-style Python for batch ingest -> Delta + aggregations
- databricks_autoloader_streaming.py — Autoloader (cloudFiles) streaming ingest -> Delta
- databricks_sql_queries.sql — Useful SQL queries for Databricks SQL / notebooks
- databricks_job.json — Example Databricks Job definition (import or Jobs API)

Quick start (interactive):
1. Upload or copy databricks_batch_delta_ingest.py into a Databricks notebook (Python) in your workspace.
2. Create mounts or ensure access to the cloud paths referenced (e.g. /mnt/raw/ or s3://bucket/).
   - Use Databricks Secrets to store credentials and avoid embedding keys.
   - Example: dbutils.secrets.get(scope = "prod", key = "s3-access-key")
3. Set the notebook widgets (or pass as job parameters) for:
   - input_path, output_delta_path, delta_table_name, partition_by, sample_rows_for_schema
4. Run the notebook to ingest raw CSV(s) into Delta and produce aggregates.

Autoloader streaming:
- Copy databricks_autoloader_streaming.py into a notebook and start the stream.
- Start it as a long-running job in Jobs to keep streaming ingestion alive.
- Provide checkpointPath for exactly-once semantics.

Scheduling as a Job:
- Import databricks_job.json via the Jobs UI or Jobs API and update notebook_path, cluster spec, and email addresses.
- Schedule to run daily/hourly as needed.

Best practices & tips:
- Use curated schemas for production ingestion to avoid unexpected schema drift.
- Use Delta OPTIMIZE and ZORDER for accelerating point lookups and time-range queries.
- Use Unity Catalog and Table ACLs for secure governance if available.
- Use dbt or Databricks SQL dashboards for downstream analytics / BI.
- Monitor streaming jobs (Autoloader) with the Structured Streaming UI or Jobs UI.

If you want, I can:
- Open a PR to add these files to muralidhargajjala7-web/Amazon-Raw-Sales-Data-Analysis.
- Convert the notebook content to a .dbc export or a workspace import JSON.
- Create a full Jobs/Workflows spec with task dependencies (ingest -> optimize -> aggregate -> publish).
- Tailor cluster specs, sample curated schema, and scheduling cron to your environment.
```