# Real-Time Data Streaming: EC2 → NiFi → S3 → Snowflake

**What this is:** a small, end-to-end real-time pipeline.  
Python (Jupyter in Docker on EC2) generates customer records as CSVs → **Apache NiFi** (in Docker on EC2) picks them up and pushes the CSVs to **Amazon S3** → **Snowflake Snowpipe** auto-loads to a RAW table → **Streams** and **Tasks** keep **SCD1 (current)** and **SCD2 (history)** tables up to date.  

By the end you have three tables:
- `CUSTOMER_RAW` — landing/ingest
- `CUSTOMER` — SCD1 (latest values)
- `CUSTOMER_HISTORY` — SCD2 (full history with start/end times)

---
## Architecture Overview
![Architecture](Data%20Architecture.png)

---

## What I learnt through this project
- Run NiFi + Jupyter in Docker **on Amazon EC2**.
- Move files to S3 with **NiFi** (ListFile → FetchFile → PutS3Object).
- Auto-ingest files to Snowflake with **Snowpipe**.
- Maintain **SCD Type 1** (current) and **SCD Type 2** (history) with **Streams** and **Tasks**.
- Use practical SQL patterns (null-safe `MERGE`, task dependencies).

---

## Repo contents
- `Data Generation.ipynb` — Python notebook that creates CSVs of fake customers
- `docker-compose.yml` — JupyterLab + NiFi in Docker
- `table_creation.sql` — DB/Schema/Tables + Stream on `CUSTOMER`
- `s3-snowflake_integration.sql` — Storage Integration, Stage, and Snowpipe
- `SCD1.sql` — procedure + Task for SCD1 upsert (`CUSTOMER_RAW → CUSTOMER`)
- `SCD2.sql` — view + Task for SCD2 (`CUSTOMER → CUSTOMER_HISTORY`)
- `Data Architecture.png` — high-level diagram

---

## Quick start

### 1) Run services
```bash
docker compose up -d
# JupyterLab: http://localhost:4888
# NiFi UI:    http://localhost:2080/nifi/
```

### 2) Generate data
Open `Data_generation.ipynb` in JupyterLab and run all cells. It writes CSV files like `FakeDataset/customer_<timestamp>.csv`.
In NiFi, set **ListFile → Input Directory** to the folder where the notebook saves CSVs (e.g., `/opt/workspace/FakeDataset` if you mapped it).

### 3) Configure NiFi (three processors)
**ListFile → FetchFile → PutS3Object**

- **ListFile**
  - Input Directory: `/opt/workspace/FakeDataset`
  - success → FetchFile
 
- **FetchFile**
  - File to Fetch: `${absolute.path}${filename}`
  - success → PutS3Object
 
- **PutS3Object**
  - Bucket: `<YOUR_BUCKET>`
  - Object Key: `stream_data/${filename}`
  - Credentials: via AWS Credentials Provider Controller Service
 
You should see files appear in `s3://<YOUR_BUCKET>/stream_data/`.

### 4) Create Snowflake objects
1) `table_creation.sql` — creates `SCD_DEMO.SCD2` objects:
  - Tables: `CUSTOMER_RAW`, `CUSTOMER`, `CUSTOMER_HISTORY`
  - Stream: `CUSTOMER_TABLE_CHANGES` on `CUSTOMER`

2) `s3-snowflake_integration.sql` — sets up:
   - Storage Integration (IAM role trust)
   - Stage pointing to your S3 path
   - Snowpipe to load into `CUSTOMER_RAW`

3) `SCD1.sql` — creates:
  - Procedure `PDR_SCD_DEMO()` (null-safe MERGE, sets `ERROR_ON_NONDETERMINISTIC_MERGE=FALSE`)
  - Task `TSK_SCD_RAW` (every minute) that calls the proc
  - Resume: `ALTER TASK SCD_DEMO.SCD2.TSK_SCD_RAW RESUME;`

4) `SCD2.sql` — creates:
  - View `V_CUSTOMER_CHANGE_DATA` (maps stream events to changes)
  - Task `TSK_SCD_HIST` (runs **AFTER** `TSK_SCD_RAW`)
  - Resume: `ALTER TASK SCD_DEMO.SCD2.TSK_SCD_HIST RESUME;`

**How SCD1 & SCD2 work (simple)**

- SCD1 (current table): keep only the latest value for each customer.
  The MERGE updates changed fields and inserts new IDs.

- SCD2 (history table): keep a timeline of changes.
  For each change, close the old version (set `end_time`, `is_current = FALSE`) and insert a new current row with a fresh `start_time`.

This project uses a **Stream** on `CUSTOMER` to detect inserts/updates/deletes, then a **Task** merges those into `CUSTOMER_HISTORY`.

---

## Validate the flow

- Files land in S3 under `stream_data/`
- `CUSTOMER_RAW` fills via Snowpipe
- After the SCD1 task runs, `CUSTOMER` shows the latest values
- After the SCD2 task runs, `CUSTOMER_HISTORY` records inserts/changes/deletes

---
## Contact

If you’re also exploring data engineering or cloud projects — I’d love to connect!
