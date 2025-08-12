# Exchange Rates Data Pipeline

## Overview
This project automates the retrieval, processing, and storage of historical exchange rate data.  
It dynamically generates tasks based on a control table in Snowflake and uses Airflow’s dynamic task mapping for fine-grained tracking.  
The system ensures per-request and per-date status monitoring, allowing efficient error handling, retries, and audits.

## Tech Stack
- **Python** – Core logic, API calls, and transformations
- **Apache Airflow** – Orchestration, scheduling, and dynamic task mapping
- **Snowflake** – Control, tracking, and final storage
- **DBT** – (Optional) Data transformation and modeling
- **Exchange Rate API** – External data source for historical rates
- **S3 / Local Storage** – Optional raw JSON storage before load
- **Email Notification System** – Alerts on pipeline failures


## Architecture

Snowflake Control Table --> Airflow DAG (Dynamic Mapping)
--> API Fetch
--> JSON Output (optional)
--> Snowflake Target Table
--> Status Update per Date & Request

## Workflow Steps
1. **Read Requests from Control Table**  
   - Each row specifies: currency pair, date range, request ID, and status.

2. **Generate Dynamic Tasks**  
   - Airflow maps each request to multiple tasks, one per date.

3. **Check for Existing Data**  
   - Queries Snowflake to skip already-processed dates.

4. **Fetch Exchange Rate**  
   - Calls API for historical exchange rate of the specified currency and date.

5. **Write Raw Data (Optional)**  
   - Saves fetched data as JSON locally or in S3 for traceability.

6. **Load into Snowflake**  
   - Inserts only missing rates into the target fact table.

7. **Update Statuses**  
   - Per-date status: success/failure
   - Per-request status: updated after all dates are processed

8. **Failure Notifications**  
   - Sends an email if any task in the DAG fails.

## Key Features
- **Dynamic Task Mapping** for per-date, per-request execution
- **Retry Logic** with exponential backoff on API calls
- **Granular Traceability** for easy debugging and audit
- **Incremental Processing** to avoid duplicate loads
- **Configurable API Source** for flexibility

## Control Tables
### `exchange_requests_control`
| Column         | Type      | Description |
|----------------|-----------|-------------|
| request_id     | STRING    | Unique ID of the request |
| base_currency  | STRING    | Base currency code |
| target_currency| STRING    | Target currency code |
| start_date     | DATE      | Start of requested range |
| end_date       | DATE      | End of requested range |
| status         | STRING    | Overall request status |

### `exchange_dates_tracking`
| Column         | Type      | Description |
|----------------|-----------|-------------|
| request_id     | STRING    | Linked to control table |
| date           | DATE      | Date for which data is fetched |
| status         | STRING    | success / failure |
| error_message  | STRING    | API or load error details |

## Installation & Setup
1. **Clone Repository**
   ```bash
   git clone <repo_url>
   cd exchange-rates-pipeline
    ```
2. **Install Python Dependencies**
    ```bash
    pip install -r requirements.txt
    ```
3. Configure Environment Variables
    - API key
    - Snowflake credentials
    - S3 bucket (Future feature)
4. Deploy Airflow DAG
    - Place DAG file in Airflow’s DAGs folder
    - Ensure Airflow connections for Snowflake and API are set
5. DBT Models (Optional)
    - Place DBT models in /dbt folder
    - Run dbt run after loading raw data

## Future Improvements
- Add parallel S3 load for raw data archiving
- Build DBT transformation layer for analytics-ready tables
- Introduce API rate-limit handling logic
- Implement full automated backfill for new currency pairs
