```markdown
# Databricks + AWS S3 — Analytics notebooks

This README explains how to run the Databricks notebook files for ingesting Amazon raw sales CSVs stored in AWS S3.

Files included:
- databricks_batch_delta_ingest_s3.py — Batch ingestion with Delta write to S3
- databricks_autoloader_streaming_s3.py — Autoloader streaming (cloudFiles) on S3
- databricks_job_s3.json — Example Jobs JSON to schedule the batch notebook

Quick decisions: IAM role vs access keys
- Recommended: Attach an IAM role (instance profile) to the Databricks cluster that grants access to the S3 buckets. This is the safest and simplest method.
- Alternate: Use Databricks Secret Scopes to store AWS access keys and session tokens. Not recommended for long-term production if instance profiles are available.

Cluster setup (recommended)
1. Create or use an existing Databricks cluster.
2. Attach an instance profile (IAM role) that allows s3:GetObject, s3:PutObject, s3:ListBucket and any PutObject/DeleteObject needed for Delta (prefix-level).
3. Ensure the cluster runtime supports Delta Lake (Databricks runtime). OPTIMIZE and ZORDER require Databricks runtime features.

Using the notebooks
1. Copy the notebook contents into two notebooks in your workspace (or import the .py as a notebook).
2. Set widget values (top of each notebook) or pass them via Job parameters:
   - input_path: s3://your-bucket/raw/...
   - output_delta_path: s3://your-bucket/delta/amazon_sales/
   - delta_table_name: default.amazon_sales_delta
   - partition_by: sale_date (or empty)
   - If using secrets: aws_secret_scope and secret names for keys.

3. Run the batch notebook to ingest historical data. Use overwrite_mode=true for full re-create, otherwise append.

Autoloader streaming
- Copy autoloader notebook and start as a long-running job (Jobs -> create job -> select notebook -> enable 'Run until manually stopped' or create a streaming job task).
- Provide checkpoint path in S3 and an accessible output_delta_path.
- Autoloader on S3 will rely on the cluster's access method (IAM role or secrets). Set "cloudFiles.schemaLocation" (checkpoint/schema) to an S3 path you control.

Jobs & scheduling
- Import databricks_job_s3.json via Jobs UI or Jobs API. Update notebook_path and cluster spec to match your account.
- Schedule daily/ hourly as desired.

Security & best practices
- Prefer instance profile / IAM role. Avoid embedding keys.
- Use Databricks Secret Scopes for any required secrets.
- Use curated schema for production ingestion to prevent schema drift issues.
- Use Delta OPTIMIZE + ZORDER for performance on large tables.
- Monitor streaming jobs in Jobs UI and Structured Streaming UI.

If you want, I can:
- Open a PR adding these files to muralidhargajjala7-web/Amazon-Raw-Sales-Data-Analysis.
- Create a curated schema inferred from a sample header you paste here and embed it into the notebook to harden production ingestion.
- Produce a workspace export (.dbc) or a Jobs API script to create the Job programmatically.
