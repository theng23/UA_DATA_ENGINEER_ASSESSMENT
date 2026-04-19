# UA_DATA_ENGINEER_ASSESSMENT - VO NGOC THANH
## Part 1: Grain Definition

### TASK 1A: Define the Grain of Each Fact Table 

#### Workstream A :FACT_KPI_ACTUAL
</details>

<br>

| Grain Option | Correct / Incorrect | Why |
|-------------|------------------|-----|
| One row per KPI per Year (annual rollup) | Incorrect | This is an aggregated grain (year-level), not the lowest level of measurement. It removes monthly detail and prevents time-series analysis and correct re-aggregation. |
| One row per KPI Name per Month | Incorrect | KPI Name alone is not sufficient because a KPI can have multiple Sub KPI Types. This grain collapses multiple measurements into one row, causing data loss or incorrect aggregation. |
| One row per Pillar per Month | Incorrect | Pillar is a higher-level aggregation of multiple KPIs, not the measurement level. This grain removes KPI-level detail and prevents drill-down analysis. |
| One row per KPI + Sub KPI Type per Month | Correct | This matches the lowest level of measurement in the source data. It preserves full detail and supports flexible aggregation across KPI, Pillar, and time dimensions. |
| One row per KPI + Sub KPI Type + Department per Month | Incorrect | Department is a descriptive attribute (ownership), not part of the measurement grain. Including it increases granularity artificially and may create duplicate or misleading records. |

---

#### Workstream B — FACT_ORDER_COST

| Grain Option | Correct / Incorrect | Why |
|-------------|------------------|-----|
| One row per order | Incorrect | This grain is too high-level and loses important dimensions such as cost category and time. It prevents detailed cost breakdown and accurate financial analysis. |
| One row per order × cost category | Incorrect | This allows cost breakdown by category but still lacks the time dimension. Without month-level granularity, it cannot support period-based reporting or trend analysis. |
| One row per order × cost category × month | Correct | This represents the lowest level of financial measurement. It captures cost per order, per category, per month, preserving full detail and enabling accurate aggregation across all dimensions. |
| One row per customer × month | Incorrect | This is an aggregated grain that removes order-level and cost category detail. It prevents accurate cost allocation and traceability at the transactional level. |



**Follow-up: Adding Region Dimension**

The addition of a new Region dimension does not change the grain if it is only a descriptive attribute.

- **Workstream A (KPI):** Region can be derived from existing attributes such as department or KPI ownership and added as a foreign key.

- **Workstream B (Order Cost):** Region can be derived from customer attributes (e.g., location) and included as a dimension.

In both cases, the grain remains unchanged because Region does not define how data is measured. It is only used for filtering and grouping.

**When does Region change the grain?**

If the business starts measuring data separately by Region (e.g., KPI values or costs differ by region), then Region becomes part of the grain and must be included in the fact table.

**Conclusion**

Grain remains stable unless a new dimension introduces a new level of measurement.

Grain is determined by how data is measured, not how it is described.

---

### TASK 1B: Aggregation Logic by KPI Type

| Aggregation Method | KPI Type                                      | Logic                       |
| ------------------ | --------------------------------------------- | --------------------------- |
| `SUM`              | Flow metrics (e.g., Revenue)                  | Sum all values              |
| `AVERAGE`          | Ratio metrics (e.g., Efficiency, Defect Rate) | Average values              |
| `LAST`             | Snapshot metrics (e.g., Headcount)            | Take latest value by period |

##### 1. Headcount KPIs → `LAST`

Headcount is a **snapshot metric**, representing the number of employees at a specific point in time.

* **Why not `SUM`?**
  Summing headcount across months produces a meaningless value (e.g., 100 + 110 + 120 = 330), which represents *employee-months*, not actual headcount.

* **Why not `AVERAGE`?**
  Averaging smooths the data but does not reflect the actual headcount at the reporting point.

* **Correct approach:**
  Use **`LAST`** — the most recent value within the aggregation period.

---

##### 2. Revenue KPIs → `SUM`

Revenue is a **flow metric**, accumulating over time.

* **Why `SUM`?**
  Revenue must be aggregated cumulatively to reflect total business performance.

* **Why not `AVERAGE`?**
  Averaging revenue understates performance and misrepresents the scale of the business.

* **Correct approach:**
  Use **`SUM`** to capture total revenue over the aggregation period.

---

##### 3. Rate KPIs → `AVERAGE`

Examples: OT Cost Ratio, Line Efficiency, Defect Rate
These are **ratio metrics**, typically expressed as percentages.

* **Why not `SUM`?**
  Summing percentages produces mathematically invalid results.

* **Why `AVERAGE`?**
  Averaging preserves the unit (%) and reflects typical performance across periods.

* **Correct approach:**
  Use **`AVERAGE`**.

---

##### 4. Impact of Incorrect Aggregation on Leadership Dashboards

If the wrong aggregation method is applied, the leadership dashboard will display **misleading but seemingly valid metrics**, leading to incorrect interpretations of business performance.

**Examples:**

* Headcount (using `SUM`) → inflated workforce size, suggesting growth that does not exist
* Revenue (using `AVERAGE`) → understated financial performance, masking true business scale
* Rate KPIs (using `SUM`) → mathematically invalid values (e.g., >100%), breaking KPI meaning

**Critical Risk:**
These issues result in **silent data corruption** — no system errors are triggered, but the reported metrics are fundamentally incorrect.

**Business Impact:**

* Executives make decisions based on inaccurate data
* Strategic priorities may be misaligned
* KPI tracking and performance evaluation become unreliable
* Trust in the data platform is significantly reduced

**Conclusion:**
Incorrect aggregation does not fail loudly; it fails silently, making it one of the most dangerous data quality risks in analytical systems.

---

##### **Dynamic Aggregation Function Design**

Although Part 1 focuses primarily on grain and aggregation semantics, the assessment also asks for a design of a Python or PySpark function that applies aggregation dynamically.

This is a **design-level function**, not a full implementation at raw source level.

It is intended to run **after** KPI actual files have been transformed into a unified dataset where:
- monthly columns have been unpivoted into `PERIOD`
- KPI values have been standardized into `actual_value`
- KPI records have been matched to `UA_ID` from `kpi_master_dim.csv`

At that stage, the function can dynamically read `Aggregation_Method` from the master dimension and apply the correct aggregation rule without hardcoding per KPI.


## Part 2 : Architecture Design

### TASK 2A: Bronze / Silver / Gold Layer Design

#### Overview

This pipeline uses a **medallion architecture** (Bronze → Silver → Gold) applied
consistently across both workstreams:

- **Workstream A** — KPI Tracking (CSV + KPI Master)
- **Workstream B** — ERP Order Cost (REST API + Financial Flat File)

Both workstreams share the same Bronze / Silver / Gold structure and are orchestrated in one pipeline.
Bronze keeps raw source fidelity, Silver standardizes data into the correct business grain, and Gold publishes reporting-ready outputs. This matches the assessment requirement that both workstreams use the same medallion architecture.

---

#### Medallion Architecture Table

| Layer      | Source                        | Contents                                                                                         | Transformations Applied                                                                                                                                      | File Format                  | Partitioning Strategy |
| ---------- | ----------------------------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------- | --------------------- |
| **Bronze** | **KPI Actuals (long + wide)** | Raw `kpi_actual_long.csv` and `kpi_actual_wide.csv` exactly as received, plus ingestion metadata | No business transformation. Only add `ingestion_timestamp` and `source_file`                                                                                 | CSV preserved + Parquet copy | `ingestion_date`      |
| **Bronze** | **KPI Master Dim**            | Raw `kpi_master_dim.csv` exactly as received                                                     | No cleaning. Only add `ingestion_timestamp`                                                                                                                  | CSV preserved + Parquet copy | `ingestion_date`      |
| **Bronze** | **ERP API (orders + lines)**  | Raw JSON payloads from orders headers and order lines endpoints                                  | Read endpoint/file, log response count, add `ingestion_timestamp`, keep raw JSON unchanged                                                                   | JSON (raw)                   | `ingestion_date`      |
| **Bronze** | **Financial Flat File**       | Raw `financial_long_format.csv`, `financial_wide_format.csv`, and FX reference file              | No cleaning. Only add `ingestion_timestamp` and `source_file`                                                                                                | CSV preserved + Parquet copy | `ingestion_date`      |
| **Silver** | **KPI Actuals unified**       | One clean monthly KPI dataset reconciled from long and wide inputs                               | Unpivot both source formats, standardize `PERIOD` to `YYYY-MM`, fix OCR typos, normalize text, deduplicate, join to active KPI master, flag orphaned records | Delta / Parquet              | `year`, `month`       |
| **Silver** | **Orders + Lines**            | One conformed ERP dataset joining order headers and order lines                                  | Rename columns based on assessment mapping, join headers and lines by `order_id`, cast fields, validate structure                                            | Delta / Parquet              | `ingestion_date`      |
| **Silver** | **Financial costs**           | One clean canonical financial dataset at grain `order × cost category × month`                   | Fix OCR errors, normalize category and customer names, standardize date formats, unpivot wide format, convert to VND, deduplicate, flag `ORD-099`            | Delta / Parquet              | `year`, `month`       |
| **Gold**   | **KPI achievement summary**   | Monthly KPI reporting output                                                                     | Join KPI fact with dimensions, calculate `achievement_pct`, expose KPI monthly achievement and related summary fields for reporting                          | Delta / Parquet              | `year`, `month`       |
| **Gold**   | **Order cost reporting**      | Reporting-ready cost mart                                                                        | Aggregate clean financial cost data into reporting output such as cost per customer per month in VND                                                         | Delta / Parquet              | `year`, `month`       |

---

#### Why Parquet over CSV, XLSX, TXT, or JSON for intermediate layers?

For intermediate layers (mainly Silver and Gold), I choose Parquet / Delta instead of CSV, XLSX, TXT, or JSON. This is because Silver and Gold are analytical layers, so they must be optimized for performance, schema reliability, and downstream querying.

1. Parquet / Delta advantages
- Columnar storage → faster reads for analytics
- Compression → lower storage cost
- Schema enforcement → safer than raw text files
- Spark-native → ideal for PySpark and Fabric Lakehouse
- Partition support → efficient filtering by year/month
- Delta adds ACID + MERGE + versioning → useful for UPSERT and controlled incremental loads
2. Why not use other formats for intermediate layers?
- CSV: simple and readable, but no schema enforcement and slower to query
- XLSX: good for business users, but not suitable for scalable pipeline processing
- TXT: unstructured and weak for analytics
- JSON: useful for preserving raw API payloads in Bronze, but too verbose and inefficient for Silver/Gold analytics
3. Final decision
- Bronze: preserve native raw format where needed
- Silver / Gold: use Delta / Parquet

---

##### Incremental Load Strategy


| Source                  | Bronze Strategy   | Silver Strategy                          | Gold Strategy                       | Justification                                      |
| ----------------------- | ----------------- | ---------------------------------------- | ----------------------------------- | -------------------------------------------------- |
| **KPI Actuals**         | Append            | UPSERT by `UA_ID + PERIOD`               | Rebuild partition                   | KPI files may be re-submitted or corrected for the same month, so Silver must update   |
| **KPI Master Dim**      | Snapshot (weekly) | MERGE / UPSERT by `UA_ID`                | N/A                                 | Master data changes less frequently, but may update targets, ownership, or metadata    |
| **ERP API**             | Append raw JSON   | MERGE by `order_id` (+ line level)       | N/A                                 | API responses may later include updated order information; Bronze keeps history while Silver maintains latest conformed state  |
| **Financial Flat File** | Append            | UPSERT by `order_no + category + period` | N/A                                 | Manual financial exports can contain corrections, duplicates, or restatements          |
| **Gold Outputs**        | N/A               | N/A                                      | Rebuild partition (`year`, `month`) | Gold is derived from Silver, so partition rebuild is simpler and keeps results |



**Why not full reload every run?**  
A full reload is simpler, but it does not scale and causes unnecessary recomputation. In this pipeline, Bronze should keep every arrival for traceability, while Silver should keep the latest trusted version of each business record.

---

##### Python vs PySpark — Decision Threshold


| Criteria            | Use Python                  | Use PySpark                      |
| ------------------- | --------------------------- | -------------------------------- |
| Workload type       | API ingestion, control flow | Data transformation, aggregation |
| Data size           | Small to medium             | Large / scalable                 |
| Operation           | I/O bound                   | Compute-heavy                    |
| Use cases           | API call, retry, logging    | Unpivot, join, dedup, group by   |
| Example in pipeline | ERP ingestion, config       | KPI + Financial Silver, Gold     |


**Threshold guidance:**  
| Condition                        | Decision           |
| -------------------------------- | ------------------ |
| < ~5M rows                       | Python / pandas OK |
| > ~5M rows                       | Switch to PySpark  |
| Heavy join / shuffle             | Use PySpark        |
| Window / aggregation large scale | Use PySpark        |


---

##### Microsoft Fabric — Layer-by-Layer Choice

| Layer | Technology | Why |
|---|---|---|
| Bronze | Lakehouse (OneLake storage) | Raw file storage, no compute needed, cheap, Parquet queryable via SQL endpoint for inspection |
| Silver | Lakehouse + Spark notebooks | Spark-based transformation, Delta write, partitioning managed by Fabric runtime |
| Gold | Lakehouse + SQL Endpoint or Direct Lake | Gold tables are small, query patterns are known; SQL Endpoint gives semantic layer access; Direct Lake skips import for large tables in Power BI |
| Serving (BI) | Direct Lake mode in Power BI | Reads Gold Delta tables directly from OneLake without import, near-real-time refresh |

**When SQL Endpoint becomes wrong:** If Gold tables grow beyond ~10GB or require
complex cross-table joins that Power BI cannot handle, migrate the serving layer
to a Fabric Data Warehouse. That decision point is not reached in this pipeline's
current scope.

---

### TASK 2B: Dimensional Model : KPI Workstream

#### Star Schema vs Snowflake

**Decision: Use Star Schema**

I chose a **Star Schema** because this pipeline is designed for analytics and BI workloads (Power BI dashboards).

- The fact table joins directly with dimension tables → simpler queries and easier optimization  
- Well-suited for aggregation-heavy dashboard use cases  
- Reduces query complexity and improves performance  

---

##### Trade-off Consideration

| Aspect               | Star Schema                         | Snowflake Schema                     |
|---------------------|-------------------------------------|--------------------------------------|
| Query Performance   | Faster (fewer joins)                | Slower (more joins)                  |
| Storage             | Redundant dimension data            | More storage efficient               |
| Complexity          | Simpler                             | More complex                         |
| BI Compatibility    | Optimized for BI tools              | More complex to use                  |

---

##### Why Star Schema is appropriate in this case

- Dimension tables are small (hundreds of KPIs) → redundancy is not a practical concern  
- No complex hierarchies requiring further normalization  
- Trading a small amount of storage for significantly better query performance is a reasonable design decision  \

#### Handling KPIs Without Sub KPI Type

**Decision: Use sentinel value `'N/A'`**

For KPIs that do not have a Sub KPI Type, I use a **sentinel value (`'N/A'`)** instead of `NULL` or empty string `''`.

---

##### Why not empty string?

- No clear business meaning  
- Can be confused with missing or bad data  
- Leads to inconsistent interpretation  

##### Why sentinel value `'N/A'`?

- Clearly indicates no sub-type by design  
- Works reliably with `JOIN`, `GROUP BY`, and `WHERE`  
- Provides consistent meaning for downstream users  

##### Justification
Using a sentinel value ensures consistency, stability, and clear business interpretation, avoiding ambiguity between missing and intentionally absent data.


#### Yearly Target — Fact vs Dimension

**Decision: Store Yearly Target in `DIM_KPI`**

The target is an attribute of the KPI, not a time-based event.

---

##### Why not store in FACT?

```text
KPI: Revenue | Month: 2024-01 | actual: 100 | yearly_target: 1200  
KPI: Revenue | Month: 2024-02 | actual: 110 | yearly_target: 1200  
KPI: Revenue | Month: 2024-03 | actual: 95  | yearly_target: 1200

```
--> The same target is dupliccated across all month without adding value

---

#### Achievement % — Direction-Based Formula
There is no universal formula for calculating `achievement_pct`.  
The calculation depends on the KPI direction, which is stored as metadata in `DIM_KPI.direction`.

The pipeline reads this attribute and applies the correct formula dynamically, rather than hardcoding logic by KPI name. This ensures flexibility, scalability, and consistency across different KPI types.

For KPIs where higher values indicate better performance, the formula is based on the ratio of actual to target. For KPIs where lower values are better, the formula is inverted, comparing target against actual. This allows the achievement percentage to consistently reflect performance relative to target, regardless of KPI direction.

---

### TASK 2C: Dimensional Model :Order Cost Workstream

#### Dimension Tables
**Dim_date**
Grain: One row per caclender month
| Column         | Data Type  | Description                                         |
| -------------- | ---------- | --------------------------------------------------- |
| `date_sk`      | INT        | Surrogate key (PK)                                  |
| `period_label` | VARCHAR(7) | Reporting month in `YYYY-MM` format, e.g. `2024-01` |
| `month`        | INT        | Month number (1–12)                                 |
| `quarter`      | INT        | Quarter number (1–4)                                |
| `year`         | INT        | Calendar year                                       |

**Primary Key:** ``date_sk``

**Dim_customer**
Grain: One row per normalized customer
| Column                 | Data Type    | Description                                          |
| ---------------------- | ------------ | ---------------------------------------------------- |
| `customer_sk`          | INT          | Surrogate key (PK)                                   |
| `customer_name`        | VARCHAR(255) | Canonical business-approved customer name            |
| `customer_name_raw`    | VARCHAR(255) | Raw customer value from source                       |
| `normalization_status` | VARCHAR(20)  | `MATCHED`, `UNMATCHED`, `REVIEW`                     |
| `mapping_rule_id`      | VARCHAR(50)  | Optional mapping reference used during normalization |

**Primary Key:** ``customer_sk``

**Dim_season**
Grain: One row per season
| Column        | Data Type   | Description                 |
| ------------- | ----------- | --------------------------- |
| `season_sk`   | INT         | Surrogate key (PK)          |
| `season_code` | VARCHAR(10) | e.g. `SS24`, `AW24`, `SS25` |

**Primary Key**: ``season_sk``

**Dim_drop**
Grain: One row per drop
| Column      | Data Type   | Description           |
| ----------- | ----------- | --------------------- |
| `drop_sk`   | INT         | Surrogate key (PK)    |
| `drop_code` | VARCHAR(10) | e.g. `D1`, `D2`, `D3` |

**Primary Key**: ``drop_sk``

**Dim_fabri_type**
Grain: One row per fabric type
| Column           | Data Type   | Description                                         |
| ---------------- | ----------- | --------------------------------------------------- |
| `fabric_type_sk` | INT         | Surrogate key (PK)                                  |
| `fabric_type`    | VARCHAR(50) | e.g. `Cotton`, `Polyester`, `Linen`, `Silk`, `Wool` |

**Primary Key**: ``fabri_type_sk``

**Dim_curency**
Grain: One row per currency
| Column                 | Data Type     | Description                      |
| ---------------------- | ------------- | -------------------------------- |
| `currency_sk`          | INT           | Surrogate key (PK)               |
| `currency_code`        | VARCHAR(3)    | `USD`, `EUR`, `VND`              |
| `currency_name`        | VARCHAR(50)   | Currency description             |
| `exchange_rate_to_vnd` | DECIMAL(18,6) | Reference conversion rate to VND |

**Primary Key**: ``currency_sk``

#### Fact Table
**Fact_order_cost**
| Column                 | Data Type     | Description                                                            |
| ---------------------- | ------------- | ---------------------------------------------------------------------- |
| `fact_sk`              | BIGINT        | Surrogate key (PK)                                                     |
| `order_no`             | VARCHAR(50)   | Natural order number from source                                       |
| `category`             | VARCHAR(100)  | Cost category from financial file, e.g. `FABRIC`, `OUTSOURCE`, `TRIM`  |
| `date_sk`              | INT           | FK → `dim_date.date_sk`                                                |
| `customer_sk`          | INT           | FK → `dim_customer.customer_sk`                                        |
| `currency_sk`          | INT           | FK → `dim_currency.currency_sk`                                        |
| `season_sk`            | INT           | FK → `dim_season.season_sk`, nullable or mapped to `Unknown`           |
| `drop_sk`              | INT           | FK → `dim_drop.drop_sk`, nullable or mapped to `Unknown`               |
| `fabric_type_sk`       | INT           | FK → `dim_fabric_type.fabric_type_sk`, nullable or mapped to `Unknown` |
| `amount_original`      | DECIMAL(18,4) | Amount in original source currency                                     |
| `converted_amount_vnd` | DECIMAL(18,4) | Amount converted to VND                                                |
| `is_orphaned`          | BOOLEAN       | `True` if order exists in financial source but not in ERP              |
| `ingestion_ts`         | TIMESTAMP     | Load timestamp                                                         |

**Primary Key**: ``fact_sk``
**Foreign Keys**
- ``date_sk → dim_date(date_sk)``
- ``customer_sk → dim_customer(customer_sk)``
- ``currency_sk → dim_currency(currency_sk)``
- ``season_sk → dim_season(season_sk)``
- ``drop_sk → dim_drop(drop_sk)``
- ``fabric_type_sk → dim_fabric_type(fabric_type_sk)``

#### Handle ORD-099 
ORD-099 exists in the financial file but has no matching order in the ERP API.
-> Decision: **Keep, flag, do not drop**.
- is_orphaned = True is set in Silver layer
- ORD-099 is excluded from all Gold aggregations until confirmed by business
- Business and relevant departments must be consulted to determine:
  - Was this order created outside the ERP system?
  - Is this a data entry error?
  - Should it be included or excluded from reporting?

Dropping ORD-099 silently would hide a potential data integrity issue.
Flagging it makes the problem visible and actionable.

#### Converted_amount_vnd lives in Fact or Gold?
FX rate at the time of transaction is a **historical fact** — it happens once and does not change. If calculated at Gold layer using current rates, amounts from 6 months ago would be converted at today's rate — wrong from a business perspective.

#### Handle four variants of 'Sunrise Appare'
Normalization approach:
- UPPER + TRIM to fix formatting differences
- Business-confirmed mapping table to resolve semantic differences
- If not in mapping table → flag as UNMATCHED, await business confirmation

### PART 3: Surrogate Keys and SCD Design
#### Task 3B: SCD Type 1 vs Type 2 : Decision Table

| Attribute                      | Type       | Justification                                                                                                                                                                                |
| ------------------------------ | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **kpi_name**                   | **Type 1** | Simple rename that does not change the KPI meaning or calculation logic. Overwriting is sufficient.                                                                                          |
| **unit (% → Number)**          | **Type 2** | 0.94 (ratio) and 12400 (count) are fundamentally different units. Without versioning, historical data becomes non-comparable and leads to incorrect analysis.                                |
| **yearly_target (revised M6)** | **Type 2** | Historical targets (M1–M5) must be preserved to calculate **achievement_pct** correctly. Overwriting would recalculate past performance using the new target, causing business inaccuracies. |
| **aggregation_method**         | **Type 2** | Aggregation method defines how the KPI is computed (SUM / AVERAGE / LAST). Changing it without versioning would silently recalculate historical data incorrectly.                            |
| **department**                 | **Type 2** | KPI ownership impacts organizational analysis. Historical data must reflect the correct department at each point in time.                                                                    |
| **is_active**                  | **Type 1** | Represents current status only (active/inactive). It does not affect historical calculations.                                                                                                |
| **pillar_id**                  | **Type 2** | Pillar is used for grouping KPIs in reporting. If restructured and overwritten, historical data would be incorrectly reassigned to the new pillar.                                           |



### PART 4: REST API Ingestion
#### TASK 4A: ERP API Ingestion Script

<details> 
<summary>Scrip Python API</summary>



```
import json
import logging
import time
import random
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# =========================================================
# CONFIG
# =========================================================
try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path.cwd()

RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "bronze"
LOG_DIR = BASE_DIR / "logs"

HEADERS_SOURCE = RAW_DIR / "mock_orders_headers.json"
DETAILS_SOURCE = RAW_DIR / "mock_orders_details.json"

CONFIG = {
    "api_mode": "file",   # "file" or "http"
    "timeout_seconds": 10,
    "auth": {
        "enabled": False,
        "bearer_token": None
        # production: set enabled=True and bearer_token="<token>"
        # this is the only block that needs to change when auth is added
    },
    "retry": {
        # 5xx will retry, 4xx will not — retrying a bad request is pointless
        "max_attempts": 3,
        "base_delay_seconds": 1,   # exponential backoff: 1s, 2s, 4s
        "jitter": True             # small random offset to avoid thundering herd
    }
}


# =========================================================
# LOGGING SETUP
# =========================================================
def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "ingestion.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


# =========================================================
# STRUCTURED ERROR LOG
# Plain logging.error is not enough — we need structured fields
# so that months later someone can query: "how many records were
# lost to malformed JSON in January?" and find the answer immediately
# =========================================================
def log_ingestion_error(
    endpoint: str,
    error_type: str,
    error_message: str,
    raw_response: Optional[str] = None,
    request_page: Optional[int] = None,
    ingestion_ts: Optional[str] = None
) -> None:
    """
    Write a structured error record to ingestion_errors.log.

    Fields captured:
      endpoint      : which endpoint failed
      request_page  : which page number (if pagination is active)
      raw_response  : the raw text that caused the error (truncated to 500 chars)
      error_type    : category of error (MalformedJSON, ConnectionTimeout, HTTP4xx, ...)
      error_message : full error detail
      ingestion_ts  : UTC timestamp when the error occurred
    """
    error_record = {
        "endpoint": endpoint,
        "request_page": request_page,
        "raw_response": raw_response[:500] if raw_response else None,
        "error_type": error_type,
        "error_message": error_message,
        "ingestion_ts": ingestion_ts or datetime.now(timezone.utc).isoformat()
    }

    error_log_path = LOG_DIR / "ingestion_errors.log"
    with open(error_log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(error_record, ensure_ascii=False) + "\n")

    logging.error(f"INGESTION ERROR | {json.dumps(error_record, ensure_ascii=False)}")


# =========================================================
# CUSTOM EXCEPTIONS
# =========================================================
class APIIngestionError(Exception):
    """Base exception for all ingestion errors."""

class APIConnectionError(APIIngestionError):
    """Raised when connection or file access fails."""

class APIEmptyResponseError(APIIngestionError):
    """Raised when the API or file returns empty data."""

class APIResponseFormatError(APIIngestionError):
    """Raised when the response is malformed or not valid JSON."""

class APIHTTPError(APIIngestionError):
    """Raised on HTTP errors. Carries status_code so retry logic can decide."""
    def __init__(self, message: str, status_code: int):
        super().__init__(message)
        self.status_code = status_code


# =========================================================
# RETRY HELPER
# Key distinction:
#   4xx = the request itself is wrong (bad URL, unauthorized, not found)
#         Retrying the same request will always produce the same result — do NOT retry
#   5xx = the server had a temporary problem, the request is valid
#         Retrying after a delay has a real chance of succeeding — DO retry
# =========================================================
def should_retry(exception: Exception) -> bool:
    """
    Return True if the exception is transient and worth retrying.
    - 5xx or ConnectionError  -> retry
    - 4xx or MalformedJSON    -> do not retry
    """
    if isinstance(exception, APIHTTPError):
        return exception.status_code >= 500
    if isinstance(exception, APIConnectionError):
        return True    # timeout, network drop — transient
    return False       # malformed JSON, empty response — not transient


def with_retry(func, *args, config: Dict, endpoint_name: str, **kwargs):
    """
    Execute func with exponential backoff retry.
    Only retries errors where should_retry() returns True.
    """
    retry_cfg = config.get("retry", {})
    max_attempts = retry_cfg.get("max_attempts", 3)
    base_delay = retry_cfg.get("base_delay_seconds", 1)
    jitter = retry_cfg.get("jitter", True)

    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)

        except Exception as e:
            last_exception = e

            if not should_retry(e):
                # Non-retryable (4xx, malformed) — fail immediately
                logging.warning(f"{endpoint_name}: non-retryable error on attempt {attempt}: {e}")
                raise

            if attempt == max_attempts:
                logging.error(f"{endpoint_name}: all {max_attempts} attempts failed")
                raise

            # Exponential backoff: 1s, 2s, 4s ...
            # Jitter adds a small random offset to avoid thundering herd
            # (multiple failed runs retrying at exactly the same interval)
            delay = base_delay * (2 ** (attempt - 1))
            if jitter:
                delay += random.uniform(0, 0.5)

            logging.warning(
                f"{endpoint_name}: attempt {attempt}/{max_attempts} failed ({e}). "
                f"Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)

    raise last_exception


# =========================================================
# API CLIENT
# Abstraction over the transport layer.
# Today: reads local mock JSON files
# Future: calls real HTTP endpoints
# Switching between the two requires changing only config["api_mode"]
# — no changes to ingestion or mapping logic
# =========================================================
class APIClient:

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def get_auth_headers(self) -> Dict[str, str]:
        """
        Single place that controls authentication.
        To add Bearer token in production: set auth.enabled=True and auth.bearer_token.
        Nothing else in the codebase needs to change.
        """
        headers = {"Content-Type": "application/json"}
        auth_cfg = self.config.get("auth", {})
        if auth_cfg.get("enabled") and auth_cfg.get("bearer_token"):
            headers["Authorization"] = f"Bearer {auth_cfg['bearer_token']}"
        return headers

    def get(self, source: Any, endpoint_name: str) -> Any:
        """Unified GET — delegates to file or HTTP based on config."""
        mode = self.config.get("api_mode", "file")
        logging.info(f"Calling endpoint: {endpoint_name} | mode={mode}")

        if mode == "file":
            return self._get_from_file(source, endpoint_name)
        elif mode == "http":
            return with_retry(
                self._get_from_http_once,
                source, endpoint_name,
                config=self.config,
                endpoint_name=endpoint_name
            )
        else:
            raise ValueError(f"Unsupported api_mode: {mode}")

    def _get_from_file(self, file_path: Path, endpoint_name: str) -> List[Dict[str, Any]]:
        """Read a local JSON file as if it were a live API response."""
        try:
            if not file_path.exists():
                raise APIConnectionError(
                    f"{endpoint_name}: source file not found: {file_path.resolve()}"
                )

            with open(file_path, "r", encoding="utf-8") as f:
                raw_text = f.read()

            try:
                data = json.loads(raw_text)
            except json.JSONDecodeError as e:
                # Malformed JSON — log structured record and skip
                # Pipeline must not crash because of partial data loss
                log_ingestion_error(
                    endpoint=endpoint_name,
                    error_type="MalformedJSON",
                    error_message=str(e),
                    raw_response=raw_text
                )
                logging.warning(f"{endpoint_name}: malformed JSON — skipping, returning empty list")
                return []

            if not data:
                raise APIEmptyResponseError(f"{endpoint_name}: empty response")

            if not isinstance(data, list):
                raise APIResponseFormatError(f"{endpoint_name}: expected a JSON list")

            logging.info(f"{endpoint_name}: retrieved {len(data)} records")
            return data

        except OSError as e:
            raise APIConnectionError(f"{endpoint_name}: failed to read file: {e}") from e

    def _get_from_http_once(self, url: str, endpoint_name: str) -> List[Dict[str, Any]]:
        """
        Single HTTP GET attempt. Called by with_retry().
        Raises APIHTTPError with status_code so retry logic knows whether to retry.
        """
        import requests

        try:
            response = requests.get(
                url,
                headers=self.get_auth_headers(),
                timeout=self.config.get("timeout_seconds", 10)
            )

            if 400 <= response.status_code < 500:
                # Client error — the request itself is wrong, do not retry
                raise APIHTTPError(
                    f"{endpoint_name}: HTTP {response.status_code} client error — will NOT retry",
                    status_code=response.status_code
                )

            if 500 <= response.status_code < 600:
                # Server error — transient, retry is appropriate
                raise APIHTTPError(
                    f"{endpoint_name}: HTTP {response.status_code} server error — will retry",
                    status_code=response.status_code
                )

            try:
                data = response.json()
            except ValueError as e:
                log_ingestion_error(
                    endpoint=endpoint_name,
                    error_type="MalformedJSON",
                    error_message=str(e),
                    raw_response=response.text
                )
                logging.warning(f"{endpoint_name}: malformed JSON from HTTP — skipping")
                return []

            if not data:
                raise APIEmptyResponseError(f"{endpoint_name}: empty response")

            if not isinstance(data, list):
                raise APIResponseFormatError(f"{endpoint_name}: expected a JSON list")

            logging.info(f"{endpoint_name}: retrieved {len(data)} records")
            return data

        except requests.Timeout as e:
            raise APIConnectionError(f"{endpoint_name}: connection timeout") from e
        except requests.RequestException as e:
            raise APIConnectionError(f"{endpoint_name}: request failed: {e}") from e


# =========================================================
# PAGINATION HANDLER
# The API currently returns a plain list with no pagination.
# The assessment says we must not assume this will always be true.
#
# Design:
#   - Fetch once
#   - If response is a plain list  -> no pagination, collect and stop
#   - If response is a dict with "next_page" -> collect "data", fetch next cursor
#   - Stop when next_page is None or missing
#
# Both formats are handled without breaking either.
# =========================================================
def fetch_with_pagination(client: APIClient, source: Any, endpoint_name: str) -> List[Dict[str, Any]]:
    """
    Handles two response formats transparently:

    Format A — current (no pagination):
        [{"id": 1, ...}, {"id": 2, ...}]

    Format B — future (cursor-based pagination):
        {
          "data": [{"id": 1, ...}],
          "next_page": "cursor_abc123"   # null when no more pages
        }
    """
    all_records: List[Dict[str, Any]] = []
    page_num = 0
    current_source = source

    while True:
        page_num += 1
        logging.info(f"{endpoint_name}: fetching page {page_num}")

        raw_response = client.get(current_source, endpoint_name)

        if isinstance(raw_response, list):
            # Format A: plain list — no pagination, collect and stop
            all_records.extend(raw_response)
            logging.info(f"{endpoint_name}: plain list format, no pagination. Total={len(all_records)}")
            break

        elif isinstance(raw_response, dict):
            # Format B: paginated — collect this page, check for next
            page_data = raw_response.get("data", [])
            all_records.extend(page_data)

            next_page = raw_response.get("next_page")

            if not next_page:
                logging.info(f"{endpoint_name}: pagination complete. Total={len(all_records)}")
                break
            else:
                logging.info(f"{endpoint_name}: page {page_num} done, next_page={next_page}")
                # In file mode this branch will never be reached
                # In http mode: append cursor to base URL and fetch next page
                current_source = f"{source}?cursor={next_page}"
        else:
            logging.warning(f"{endpoint_name}: unexpected response format on page {page_num}, stopping")
            break

    return all_records


# =========================================================
# FIELD MAPPING
# =========================================================
def map_order_headers(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Map raw API fields to business field names for order headers."""
    return [{
        "order_id": r.get("id"),
        "customer_id": r.get("userId"),
        "order_code": r.get("title"),
        "order_notes": r.get("body")
    } for r in records]


def map_order_details(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Map raw API fields to business field names for order line items."""
    return [{
        "line_id": r.get("id"),
        "order_id": r.get("postId"),
        "line_description": r.get("name"),
        "buyer_contact": r.get("email"),
        "line_notes": r.get("body")
    } for r in records]


# =========================================================
# BRONZE SAVE
# Raw responses are saved to Bronze before any mapping.
# This preserves the original source data so we can reprocess
# if mapping logic changes later, without re-calling the API.
# =========================================================
def add_ingestion_timestamp(records: List[Dict[str, Any]], ingestion_ts: str) -> List[Dict[str, Any]]:
    """Add ingestion_timestamp to every record before saving to Bronze."""
    return [{**r, "ingestion_timestamp": ingestion_ts} for r in records]


def save_bronze(records: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    logging.info(f"Saved Bronze: {output_path.resolve()} | records={len(records)}")


# =========================================================
# JOIN LOGIC
# =========================================================
def join_orders_and_lines(
    order_headers: List[Dict[str, Any]],
    order_lines: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Join order headers with their line items by order_id.
    Orders with no matching lines get line_items=[] — they are not dropped.
    """
    lines_by_order: Dict[Any, List] = {}
    for line in order_lines:
        oid = line.get("order_id")
        lines_by_order.setdefault(oid, []).append(line)

    joined = []
    for header in order_headers:
        oid = header.get("order_id")
        joined.append({**header, "line_items": lines_by_order.get(oid, [])})

    logging.info(f"Joined: {len(joined)} orders")
    return joined


# =========================================================
# DEBUG HELPER
# =========================================================
def log_runtime_paths() -> None:
    logging.info(f"BASE_DIR: {BASE_DIR}")
    logging.info(f"HEADERS_SOURCE exists: {HEADERS_SOURCE.exists()}")
    logging.info(f"DETAILS_SOURCE exists: {DETAILS_SOURCE.exists()}")


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    setup_logging()
    logging.info("=== ERP API Ingestion START ===")

    ingestion_ts = datetime.now(timezone.utc).isoformat()
    client = APIClient(CONFIG)

    try:
        log_runtime_paths()

        # Step 1: Fetch from both endpoints
        # Using pagination wrapper to handle both plain list and cursor-paginated responses
        raw_headers = fetch_with_pagination(client, HEADERS_SOURCE, "GET /orders/headers")
        raw_details = fetch_with_pagination(client, DETAILS_SOURCE, "GET /orders/lines")

        # Step 2: Save raw responses to Bronze with ingestion timestamp
        # Raw is saved before any mapping — preserves source data for reprocessing
        save_bronze(
            add_ingestion_timestamp(raw_headers, ingestion_ts),
            BRONZE_DIR / "orders_headers_bronze.json"
        )
        save_bronze(
            add_ingestion_timestamp(raw_details, ingestion_ts),
            BRONZE_DIR / "orders_details_bronze.json"
        )

        # Step 3: Apply business field mapping
        mapped_headers = map_order_headers(raw_headers)
        mapped_details = map_order_details(raw_details)
        logging.info(f"Mapped: {len(mapped_headers)} headers, {len(mapped_details)} details")

        # Step 4: Join headers with their line items
        joined_orders = join_orders_and_lines(mapped_headers, mapped_details)

        if joined_orders:
            logging.info("Sample joined order:")
            logging.info(json.dumps(joined_orders[0], ensure_ascii=False, indent=2))

        logging.info("=== ERP API Ingestion COMPLETE ===")

    except APIIngestionError as e:
        logging.error(f"Ingestion failed: {e}")
        raise
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()
```
</details>

**Script Structure**

| Component                   | Description                                                                                                       |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------- | 
| **CONFIG block**            | Centralises all settings such as `api_mode`, authentication, and retry configuration                              | 
| **Structured error log**    | Writes errors to a structured log file with fields like `endpoint`, `error_type`, `raw_response`, and `timestamp` |
| **Custom exceptions**       | Defines distinct error types (`APIConnectionError`, `APIHTTPError`, `APIEmptyResponseError`)                      | 
| **should_retry()**          | Encapsulates retry decision logic: retries only transient failures such as 5xx and connection errors              |
| **APIClient**               | Abstracts the transport layer and centralises authentication handling                                             |
| **fetch_with_pagination()** | Handles both plain list responses and future cursor-based pagination (`data`, `next_page`)                        | 
| **Field mapping**           | Translates raw API field names into business-friendly schema                                                      | 
| **Bronze save**             | Persists raw API responses before any transformation                                                              | 
| **main()**                  | Orchestrates the full execution flow: fetch → save raw → map fields → join → log result                           |

#### TASK 4B: he ERP Has No API: Scenario Question 

**Scenario**
The ERP system has **no API, no export functionality, and no database access**.  
The vendor has confirmed that no API will be built.  
However, the data is required in the pipeline on a **weekly basis**.

---

#### 1. Options to Get Data

**Option 1 — Manual Export**
Business users manually extract data from the ERP and upload it weekly.

- **Technical Risk:** Human error, inconsistent format, missing data  
- **Business Risk:** Depends on individuals (absence → pipeline failure)  
- **Maintenance Cost:** Low (data team), high (business team)

---

**Option 2 — Web Scraping / Crawling**
Automate extraction by capturing ERP **HTTP requests (headers, AJAX/XHR)** and replaying them.

- **Technical Risk:**
  - Dependency on ERP request structure (headers, tokens)
  - Breaks if UI or request structure changes
- **Business Risk:**
  - Pipeline fails if runtime environment is unavailable
  - Legal/contract risk if scraping violates vendor ToS
- **Maintenance Cost:** High (requires monitoring and frequent fixes)

**Mitigation:**
- Run on a **VM/server** instead of a local machine  
- Schedule jobs using **cron/Airflow**

---

#### 2. Recommended Approach

#### Web Crawling (Running on VM)

**Why:**
- Only method to extract data directly from ERP without manual dependency  
- Fully automatable and schedulable  
- VM removes dependency on local machine  

This approach is based on real-world experience implementing ERP crawling.

---

#### When Recommendation Changes

| Condition | Alternative |
|----------|------------|
| Vendor prohibits scraping (legal/ToS) | Manual export |
| Budget available for automation tools | RPA |
| Small / low-priority dataset | Manual export |
| Vendor negotiation possible | Request data feed/API |

---

#### 3. Business Alignment Before Implementation

##### Stakeholders
- Tech Lead / Data Lead  
- Business Owner  
- IT / Security  
- Legal / Compliance  
- Senior Management  

##### Key Questions
- Does the vendor contract allow automated data extraction?  
- What is the acceptable risk level?  
- What happens if the pipeline fails?  
- Is manual fallback acceptable?  

> Do not proceed without **Legal and Security approval**

---

#### 4. Migration Plan (When API Becomes Available)

### Principle
Separate **ingestion** from **transformation** so the data source can be replaced easily.

**Current: Crawling -> Bronze -> Lakehouse** \
**Future: API -> Bronze -> Lakehouse**


---

##### Migration Steps

**Step 1 — Parallel Run**
- Run both Crawling and API ingestion  
- Store outputs separately in Bronze layer  

**Step 2 — Validation**
- Compare:
  - Row counts  
  - Field values  
  - Aggregations  

**Step 3 — Switch to API**
- Replace crawling with API ingestion  
- Monitor for stability (1–2 weeks)

**Step 4 — Cleanup**
- Remove crawling logic  
- Update documentation  

---

#### Final Conclusion

- Crawling is a **pragmatic workaround**, not a long-term solution  
- Must be:
  - Automated (VM + scheduler)  
  - Monitored  
  - Legally aligned  

### PART 5: Silver Transformation
#### TASK 5A: KPI ACTUALS: Bronze to silver
1. Ingest both kpi_actual_long.csv and kpi_actual_wide.csv

<details> 
<summary>PySpark Script</summary>

```
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable
import re

# =========================================================
# CONFIG
# =========================================================
BRONZE_PATH = "Files/UA_TEST"

LONG_FILE = f"{BRONZE_PATH}/kpi_actual_long.csv"
WIDE_FILE = f"{BRONZE_PATH}/kpi_actual_wide.csv"
KPI_FILE  = f"{BRONZE_PATH}/kpi_master_dim.csv"

spark = (
    SparkSession.builder
    .appName("kpi_actuals_bronze_to_silver")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

# =========================================================
# READ DATAFRAMES
# =========================================================
df_kpi_long_raw = (
    spark.read
         .option("header", True)
         .option("inferSchema", False)
         .csv(LONG_FILE)
)

df_kpi_wide_raw = (
    spark.read
         .option("header", True)
         .option("inferSchema", False)
         .csv(WIDE_FILE)
)

df_kpi_dim_raw = (
    spark.read
         .option("header", True)
         .option("inferSchema", False)
         .csv(KPI_FILE)
)

# =========================================================
# DISPLAY
# =========================================================
display(df_kpi_long_raw)
display(df_kpi_wide_raw)
display(df_kpi_dim_raw)
```

</details>

**Idea:** Read both CSV files from Bronze layer into PySpark DataFrames.
Each file gets a SOURCE_FILE column and a ROW_ID (SHA256 hash of all
columns) added immediately at ingestion time for traceability.

2. Fix OCR typos

**Idea**: Numeric columns (Target, M1–M12 for long format; Target, Jan-24–Dec-24
for wide format) may contain letter O instead of digit 0 due to OCR errors.

<details> 
<summary>PySpark Script</summary>

```
# =========================================================
# ADD ROW_ID + SOURCE_FILE
# =========================================================
df_kpi_long_step1 = (
    df_kpi_long_raw
    .withColumn(
        "ROW_ID",
        F.sha2(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df_kpi_long_raw.columns]),
            256
        )
    )
    .withColumn("SOURCE_FILE", F.lit("kpi_actual_long.csv"))
)

df_kpi_wide_step1 = (
    df_kpi_wide_raw
    .withColumn(
        "ROW_ID",
        F.sha2(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df_kpi_wide_raw.columns]),
            256
        )
    )
    .withColumn("SOURCE_FILE", F.lit("kpi_actual_wide.csv"))
)

# =========================================================
# IDENTIFY VALUE COLUMNS
# - long: M1..M12 + Target if exists
# - wide: Jan-24..Dec-24 + Target if exists
# =========================================================
long_value_cols = [c for c in df_kpi_long_step1.columns if re.fullmatch(r"M([1-9]|1[0-2])", c)]
wide_value_cols = [c for c in df_kpi_wide_step1.columns if re.fullmatch(r"[A-Za-z]{3}-\d{2}", c)]

if "Target" in df_kpi_long_step1.columns:
    long_value_cols.append("Target")

if "Target" in df_kpi_wide_step1.columns:
    wide_value_cols.append("Target")

print("long value cols:", long_value_cols)
print("wide value cols:", wide_value_cols)

# =========================================================
# OCR FIX FUNCTION
# =========================================================
def fix_ocr_o_to_zero(df, value_cols):
    log_dfs = []
    out_df = df

    for col_name in value_cols:
        before_col = f"__before_{col_name}"
        after_col = f"__after_{col_name}"

        out_df = out_df.withColumn(before_col, F.col(col_name))

        out_df = out_df.withColumn(
            after_col,
            F.when(
                F.col(col_name).rlike(r"^[0-9Oo\.\,\-\s]+$"),
                F.regexp_replace(F.col(col_name), r"[Oo]", "0")
            ).otherwise(F.col(col_name))
        )

        log_df = (
            out_df
            .filter(
                F.coalesce(F.col(before_col).cast("string"), F.lit("")) !=
                F.coalesce(F.col(after_col).cast("string"), F.lit(""))
            )
            .select(
                "ROW_ID",
                "SOURCE_FILE",
                F.lit(col_name).alias("COLUMN_NAME"),
                F.col(before_col).cast("string").alias("ORIGINAL_VALUE"),
                F.col(after_col).cast("string").alias("CORRECTED_VALUE"),
                F.current_timestamp().alias("LOGGED_AT")
            )
        )

        log_dfs.append(log_df)

        out_df = out_df.withColumn(col_name, F.col(after_col))
        out_df = out_df.drop(before_col, after_col)

    if log_dfs:
        final_log = log_dfs[0]
        for x in log_dfs[1:]:
            final_log = final_log.unionByName(x)
    else:
        final_log = spark.createDataFrame(
            [],
            "ROW_ID string, SOURCE_FILE string, COLUMN_NAME string, ORIGINAL_VALUE string, CORRECTED_VALUE string, LOGGED_AT timestamp"
        )

    return out_df, final_log

# =========================================================
# APPLY OCR FIX
# =========================================================
df_kpi_long_ocr_fixed, df_kpi_long_ocr_log = fix_ocr_o_to_zero(df_kpi_long_step1, long_value_cols)
df_kpi_wide_ocr_fixed, df_kpi_wide_ocr_log = fix_ocr_o_to_zero(df_kpi_wide_step1, wide_value_cols)

df_kpi_ocr_log = df_kpi_long_ocr_log.unionByName(df_kpi_wide_ocr_log)

# =========================================================
# PREVIEW
# =========================================================
print("=== OCR LOG ===")
display(df_kpi_ocr_log)

print("=== LONG AFTER OCR FIX ===")
display(df_kpi_long_ocr_fixed)

print("=== WIDE AFTER OCR FIX ===")
display(df_kpi_wide_ocr_fixed)
```
</details>
- The idea for this step is read all value number eg Target,M1,M2.. to fix letter O as digit 0 with log để ghi lại những giá trị thay đổi so với bản gốc
  - long: M1..M12 + Target
  - wide: Jan-24..Dec-24 + Target
  
3. Normalise whitespace and casing

**Idea:** Standardize all categorical columns to Initcap case with Trimmed
whitespace to ensure consistent joining and grouping downstream.

<details> 
<summary>PySpark Script</summary>
  
```
from pyspark.sql import functions as F

# =========================================================
# DEFINE CATEGORICAL COLUMNS
# =========================================================
categorical_cols = [
    "Pillar",
    "Sub_Pillar",
    "Department",
    "KPI_Name",
    "Sub_KPI_Type",
    "Unit"
]

# =========================================================
# ENSURE ROW_ID + SOURCE_FILE EXIST
# =========================================================
if "ROW_ID" not in df_kpi_long_ocr_fixed.columns:
    df_kpi_long_ocr_fixed = df_kpi_long_ocr_fixed.withColumn(
        "ROW_ID",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("Pillar").cast("string"), F.lit("")),
                F.coalesce(F.col("Sub_Pillar").cast("string"), F.lit("")),
                F.coalesce(F.col("Department").cast("string"), F.lit("")),
                F.coalesce(F.col("KPI_Name").cast("string"), F.lit("")),
                F.coalesce(F.col("Sub_KPI_Type").cast("string"), F.lit("")),
                F.coalesce(F.col("Unit").cast("string"), F.lit(""))
            ),
            256
        )
    )

if "SOURCE_FILE" not in df_kpi_long_ocr_fixed.columns:
    df_kpi_long_ocr_fixed = df_kpi_long_ocr_fixed.withColumn("SOURCE_FILE", F.lit("kpi_actual_long.csv"))

if "ROW_ID" not in df_kpi_wide_ocr_fixed.columns:
    df_kpi_wide_ocr_fixed = df_kpi_wide_ocr_fixed.withColumn(
        "ROW_ID",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("Pillar").cast("string"), F.lit("")),
                F.coalesce(F.col("Sub_Pillar").cast("string"), F.lit("")),
                F.coalesce(F.col("Department").cast("string"), F.lit("")),
                F.coalesce(F.col("KPI_Name").cast("string"), F.lit("")),
                F.coalesce(F.col("Sub_KPI_Type").cast("string"), F.lit("")),
                F.coalesce(F.col("Unit").cast("string"), F.lit(""))
            ),
            256
        )
    )

if "SOURCE_FILE" not in df_kpi_wide_ocr_fixed.columns:
    df_kpi_wide_ocr_fixed = df_kpi_wide_ocr_fixed.withColumn("SOURCE_FILE", F.lit("kpi_actual_wide.csv"))

# =========================================================
# NORMALIZATION RULE
# =========================================================
def normalize_categorical_value(col_name):
    cleaned = F.trim(F.regexp_replace(F.col(col_name), r"\s+", " "))
    
    if col_name in ["Pillar"]:
        return F.upper(cleaned)
    else:
        return cleaned

# =========================================================
# FUNCTION: NORMALIZE + LOG EVERY CHANGE
# =========================================================
def normalize_and_log(df, categorical_columns):
    log_dfs = []
    cleaned_df = df

    for col_name in categorical_columns:
        if col_name not in cleaned_df.columns:
            continue

        before_col = f"__before_{col_name}"
        after_col = f"__after_{col_name}"

        cleaned_df = cleaned_df.withColumn(before_col, F.col(col_name))
        cleaned_df = cleaned_df.withColumn(after_col, normalize_categorical_value(col_name))

        log_df = (
            cleaned_df
            .filter(
                F.coalesce(F.col(before_col).cast("string"), F.lit("")) !=
                F.coalesce(F.col(after_col).cast("string"), F.lit(""))
            )
            .select(
                "ROW_ID",
                "SOURCE_FILE",
                F.lit(col_name).alias("COLUMN_NAME"),
                F.col(before_col).cast("string").alias("ORIGINAL_VALUE"),
                F.col(after_col).cast("string").alias("CORRECTED_VALUE"),
                F.current_timestamp().alias("LOGGED_AT")
            )
        )

        log_dfs.append(log_df)

        cleaned_df = cleaned_df.withColumn(col_name, F.col(after_col))
        cleaned_df = cleaned_df.drop(before_col, after_col)

    if log_dfs:
        final_log = log_dfs[0]
        for x in log_dfs[1:]:
            final_log = final_log.unionByName(x)
    else:
        final_log = spark.createDataFrame(
            [],
            "ROW_ID string, SOURCE_FILE string, COLUMN_NAME string, ORIGINAL_VALUE string, CORRECTED_VALUE string, LOGGED_AT timestamp"
        )

    return cleaned_df, final_log

# =========================================================
# APPLY TO LONG AND WIDE
# =========================================================
df_kpi_long_cleaned, df_kpi_long_norm_log = normalize_and_log(df_kpi_long_ocr_fixed, categorical_cols)
df_kpi_wide_cleaned, df_kpi_wide_norm_log = normalize_and_log(df_kpi_wide_ocr_fixed, categorical_cols)

# combine logs
df_kpi_norm_log = df_kpi_long_norm_log.unionByName(df_kpi_wide_norm_log)

# =========================================================
# PREVIEW
# =========================================================
print("=== NORMALIZATION LOG ===")
display(df_kpi_norm_log)

print("=== LONG AFTER NORMALIZATION ===")
display(df_kpi_long_cleaned)

print("=== WIDE AFTER NORMALIZATION ===")
display(df_kpi_wide_cleaned)
```
</details>


    
4. Detect and remove duplicate rows

**Idea:** Detect exact duplicate rows before unpivoting. Two passes of
deduplication are applied:

**Before unpivot** (exact row duplicates):
Hash all columns — if two rows are identical in every column, keep 1,
flag the rest as IS_DUPLICATE = True.
**After unpivot** (business key duplicates):
Deduplicate on composite key: ``Pillar`` + ``Sub_Pillar`` + ``Department`` + ``KPI_Name`` + ``Sub_KPI_Type`` + PERIOD

<details> 
<summary>PySpark Script</summary>
  
```
from pyspark.sql import Window
from pyspark.sql import functions as F
import re

# =========================================================
# BUSINESS KEY COLUMNS
# =========================================================
business_key_cols = [
    "Pillar",
    "Sub_Pillar",
    "Department",
    "KPI_Name",
    "Sub_KPI_Type",
    "Unit"
]

# =========================================================
# VALUE COLUMNS FOR EACH SOURCE
# =========================================================
long_month_cols = [c for c in df_kpi_long_cleaned.columns if re.fullmatch(r"M([1-9]|1[0-2])", c)]
wide_month_cols = [c for c in df_kpi_wide_cleaned.columns if re.fullmatch(r"[A-Za-z]{3}-\d{2}", c)]

long_value_cols = long_month_cols + ([c for c in ["Target"] if c in df_kpi_long_cleaned.columns])
wide_value_cols = wide_month_cols + ([c for c in ["Target"] if c in df_kpi_wide_cleaned.columns])

long_dedup_cols = business_key_cols + long_value_cols
wide_dedup_cols = business_key_cols + wide_value_cols

print("long dedup cols:", long_dedup_cols)
print("wide dedup cols:", wide_dedup_cols)


# =========================================================
# FUNCTION: DEDUP + LOG
# =========================================================
def deduplicate_and_log(df, dedup_cols, source_name):
    """
    Keep first row per duplicate group.
    Drop remaining rows and log them.

    Uses monotonically_increasing_id() as tiebreaker in the Window orderBy
    because all duplicate rows share the same ROW_ID (SHA256 of business keys).
    Without a tiebreaker, orderBy(ROW_ID) is non-deterministic — Spark may pick
    a different "first" row on each run.
    """

    # Add a stable tiebreaker so row ordering is deterministic across runs
    df_with_seq = df.withColumn("__row_seq", F.monotonically_increasing_id())

    w = Window.partitionBy(
        *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in dedup_cols]
    ).orderBy("__row_seq")

    df_ranked = (
        df_with_seq
        .withColumn("DUP_RANK", F.row_number().over(w))
        .withColumn(
            "IS_DUPLICATE",
            F.when(F.col("DUP_RANK") > 1, F.lit(True)).otherwise(F.lit(False))
        )
    )

    # Rows to keep — drop internal columns
    df_deduped = (
        df_ranked
        .filter(F.col("IS_DUPLICATE") == False)
        .drop("DUP_RANK", "__row_seq")
    )

    # Rows dropped
    df_dropped = df_ranked.filter(F.col("IS_DUPLICATE") == True)

    # Log dropped rows
    dedup_reason = (
        f"Dropped as duplicate within {source_name} "
        f"after OCR fix and categorical normalization"
    )

    dedup_key_expr = F.concat_ws(
        " || ",
        *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in dedup_cols]
    )

    df_dedup_log = (
        df_dropped
        .select(
            F.lit("DUPLICATE_DROP").alias("LOG_TYPE"),
            "ROW_ID",
            "SOURCE_FILE",
            F.lit("ROW_LEVEL").alias("COLUMN_NAME"),
            F.lit(None).cast("string").alias("ORIGINAL_VALUE"),
            dedup_key_expr.alias("CORRECTED_VALUE"),
            F.lit(dedup_reason).alias("DETAILS"),
            F.current_timestamp().alias("LOGGED_AT")
        )
    )

    # Summary log
    dropped_count = df_dropped.count()
    kept_count = df_deduped.count()

    summary_log = (
        spark.createDataFrame(
            [(
                "DUPLICATE_SUMMARY",
                source_name,
                dropped_count,
                kept_count,
                f"Detected duplicates using columns: {', '.join(dedup_cols)}"
            )],
            ["LOG_TYPE", "SOURCE_FILE", "DROPPED_COUNT", "KEPT_COUNT", "DETAILS"]
        )
        .withColumn("LOGGED_AT", F.current_timestamp())
    )

    return df_deduped, df_dedup_log, summary_log


# =========================================================
# APPLY TO LONG AND WIDE
# =========================================================
df_kpi_long_deduped, df_kpi_long_dedup_log, df_kpi_long_dedup_summary = deduplicate_and_log(
    df_kpi_long_cleaned,
    long_dedup_cols,
    "kpi_actual_long.csv"
)

df_kpi_wide_deduped, df_kpi_wide_dedup_log, df_kpi_wide_dedup_summary = deduplicate_and_log(
    df_kpi_wide_cleaned,
    wide_dedup_cols,
    "kpi_actual_wide.csv"
)

# Combine row-level logs
df_kpi_dedup_log = df_kpi_long_dedup_log.unionByName(df_kpi_wide_dedup_log)

# Combine summary logs
df_kpi_dedup_summary = df_kpi_long_dedup_summary.unionByName(df_kpi_wide_dedup_summary)

# =========================================================
# PREVIEW
# =========================================================
print("=== DEDUP ROW-LEVEL LOG ===")
display(df_kpi_dedup_log)

print("=== DEDUP SUMMARY LOG ===")
display(df_kpi_dedup_summary)

print("=== LONG AFTER DEDUP ===")
display(df_kpi_long_deduped)

print("=== WIDE AFTER DEDUP ===")
display(df_kpi_wide_deduped)
```
</details>

-> Duplicates found in kpi_actual_long.csv:
  - FS || Revenue || Commercial || FOB Revenue || Export || NUMBER ||
  - OF || Productivity || Production || Line Efficiency || Line A || % ||
  - OF || Training || HR || Training Completion || Internal || %

5. Unpivot Wide Format (jan-24...dec-24) into period with format là YYYY-MM

<details> 
<summary>PySpark Script</summary>
  
```
from pyspark.sql import functions as F
import re

# =========================================================
# UNPIVOT WIDE FILE (Jan-24...Dec-24) INTO PERIOD (YYYY-MM)
# =========================================================
def unpivot_wide(df):
    # Get all month columns (e.g., Jan-24, Feb-24, ..., Dec-24)
    month_cols = [col for col in df.columns if re.fullmatch(r"[A-Za-z]{3}-\d{2}", col)]
    
    # Apply the unpivot operation using a correctly formatted string for stack
    stack_expr = ", ".join([f"'{col}', `{col}`" for col in month_cols])
    unpivoted_df = (
        df.select(
            "*",  # Keep all existing columns
            F.expr(f"stack({len(month_cols)}, {stack_expr}) as (month_token, actual_value)")
        )
        .withColumn("PERIOD", F.concat(F.lit("20"), F.regexp_extract(F.col("month_token"), r"([A-Za-z]{3})-(\d{2})", 2), F.lit("-"), F.lit("01")))
        .drop("month_token")  # Drop the temporary 'month_token' column
    )

    return unpivoted_df

# =========================================================
# APPLY UNPIVOT TO WIDE DATA
# =========================================================
df_kpi_wide_unpivoted = unpivot_wide(df_kpi_wide_ocr_fixed)

# Preview the unpivoted dataframe
print("=== WIDE AFTER UNPIVOT ===")
display(df_kpi_wide_unpivoted) 
```

</details>



6. Unpivot Long Format (M1–M12 → PERIOD)


<details> 
<summary>PySpark Script</summary>
  
```
# =========================================================
# UNPIVOT LONG FORMAT M1 - M12 INTO PERIOD (YYYY-MM)
# =========================================================
def unpivot_long(df):
    # Get all month columns (e.g., M1, M2, ..., M12)
    month_cols = [col for col in df.columns if re.fullmatch(r"M([1-9]|1[0-2])", col)]
    
    # Apply unpivot operation using stack to convert columns to rows
    stack_expr = ", ".join([f"'{col}', `{col}`" for col in month_cols])
    unpivoted_df = (
        df.select(
            "*",  # Keep all existing columns
            F.expr(f"stack({len(month_cols)}, {stack_expr}) as (month_token, actual_value)")  # unpivot months
        )
        .withColumn("PERIOD", F.concat(F.lit("2024-"), F.regexp_extract(F.col("month_token"), r"^M(\d{1,2})$", 1)))
        .drop("month_token")  # Drop the temporary 'month_token' column
    )

    return unpivoted_df

# =========================================================
# APPLY UNPIVOT TO LONG DATA
# =========================================================
df_kpi_long_unpivoted = unpivot_long(df_kpi_long_ocr_fixed)

# Preview the unpivoted dataframe
print("=== LONG AFTER UNPIVOT ===")
display(df_kpi_long_unpivoted)
```

</details>

7. Reconcile both formats into a single unified schema

**Idea:** Union both unpivoted DataFrames into a single unified schema
and handle discrepancies explicitly.

<details> 
<summary>PySpark Script</summary>
  
```
from pyspark.sql import functions as F

# =========================================================
# RECONCILE BOTH SOURCES INTO A SINGLE UNIFIED SCHEMA
# =========================================================
def reconcile_sources(df_long, df_wide):
    # Normalize missing Sub_Pillar
    df_long = df_long.withColumn("Sub_Pillar", F.coalesce(F.col("Sub_Pillar"), F.lit("UNKNOWN")))
    df_wide = df_wide.withColumn("Sub_Pillar", F.coalesce(F.col("Sub_Pillar"), F.lit("UNKNOWN")))

    # Handle missing Sub_KPI_Type by replacing NULL or empty with 'N/A'
    df_long = df_long.withColumn("Sub_KPI_Type", F.coalesce(F.col("Sub_KPI_Type"), F.lit("N/A")))
    df_wide = df_wide.withColumn("Sub_KPI_Type", F.coalesce(F.col("Sub_KPI_Type"), F.lit("N/A")))

    # Ensure the columns match before union
    unified_df = df_long.unionByName(df_wide, allowMissingColumns=True)

    # Clean up discrepancies in Sub_Pillar
    unified_df = unified_df.withColumn("Sub_Pillar", F.trim(F.regexp_replace(F.col("Sub_Pillar"), r"\s+", " ")))

    # Optionally: Deduplicate based on key columns
    unified_df = unified_df.dropDuplicates(
        ["Pillar", "Sub_Pillar", "Department", "KPI_Name", "Sub_KPI_Type", "Unit", "PERIOD"]
    )

    return unified_df


# =========================================================
# APPLY RECONCILE TO LONG AND WIDE UNPIVOTED DATA
# =========================================================
df_kpi_unified = reconcile_sources(df_kpi_long_unpivoted, df_kpi_wide_unpivoted)

# Preview the unified dataframe
print("=== UNIFIED DATA AFTER RECONCILE ===")
display(df_kpi_unified)

```

</details>

**Discrepancy Handling**

| Case | Situation | How I Handle It |
|------|-----------|-----------------|
| **Case 1** | Same KPI + PERIOD + same value (exact duplicate) | Deduplicate — keep 1 row, flag `IS_DUPLICATE = True` on dropped rows |
| **Case 2** | Same KPI + PERIOD + different value (conflict) | Keep **both** rows, flag `IS_CONFLICT = True` on both |
| **Case 3** | KPI exists in long format but not in wide (or vice versa) | Keep as-is, do not drop |
| **Case 4** | Column names differ between formats | Rename to unified schema before union |



8. Match to UA_ID via Composite Natural Key

**Idea:** Join unified dataset with kpi_master_dim.csv using a composite
natural key to retrieve UA_ID for each record.

``Pillar + Sub_Pillar + Department + KPI_Name + Sub_KPI_Type``

<details> 
<summary>PySpark Script</summary>
  
```
from pyspark.sql import functions as F

# Perform the join with aliases for the columns to avoid ambiguity
df_kpi_with_ua_id = df_kpi_dim_raw.alias("df_raw").join(
    df_kpi_unified.alias("df_unified"),
    (F.col("df_raw.Pillar") == F.col("df_unified.Pillar")) &
    (F.col("df_raw.Sub_Pillar") == F.col("df_unified.Sub_Pillar")) &
    (F.col("df_raw.Department") == F.col("df_unified.Department")) &
    (F.col("df_raw.KPI_Name") == F.col("df_unified.KPI_Name")) &
    (F.col("df_raw.Sub_KPI_Type") == F.col("df_unified.Sub_KPI_Type")),
    how="left"  # Left join to ensure no data loss from df_kpi
)

# Debugging: Print schema to ensure the Target column is present
df_kpi_with_ua_id.printSchema()

# Display the result
print("=== Result after join with UA_ID ===")
display(df_kpi_with_ua_id)
```

</details>

9. Flag Orphaned Records

**Idea:** Records that do not match any active master record are flagged
as orphaned — they are NOT dropped.

```
df_kpi_with_ua_id = df_kpi_with_ua_id.withColumn(
    "IS_ORPHANED",
    F.when(F.col("UA_ID").isNull(), True).otherwise(False)
)
```

10. Add Metadata Columns

**Idea:** Add standard metadata columns to every Silver record.

```
hash_cols = [
    "PILLAR_ID", "SP_ID", "UA_ID",
    "Pillar", "Sub_Pillar", "Department",
    "KPI_Name", "Sub_KPI_Type", "Unit",
    "PERIOD", "actual_value"
]

df_silver_kpi = df_kpi_with_ua_id.withColumn(
    "ROW_HASH",
    F.sha2(
        F.concat_ws("||", *[
            F.coalesce(F.col(c).cast("string"), F.lit(""))
            for c in hash_cols
        ]),
        256
    )
)
display(df_silver_kpi)
```

#### TASK 5B: Financial File : Bronze to Silver
##### Overview

Transform and standardize financial data from mixed formats (long + wide) into a clean, unified Silver dataset with data quality controls and audit logging.

Processing Steps
1. Ingestion

- Load:
  - financial_long_format.csv
  - financial_wide_format.csv
  - exchange_rates_reference.csv

2. Unpivot Wide Format

Convert date columns → rows (period_raw, amount)
Parse dd/MM/yyyy → YYYY-MM
Drop null amounts

3. Union Data

Align schema between long & wide
Union into one DataFrame
Apply all transformations once (avoid duplicated logic)

4. OCR Fix (Data Cleaning)

Replace O → 0 only when value matches numeric pattern
Log all corrections (original vs corrected)

5. Normalize Data

customer_name: lowercase
category: trim + initcap + normalize spaces
amount: trim whitespace

6. Parse Period

Handle multiple formats:
YYYY-MM, YYYY/MM, MMM yyyy, MM-yyyy
Standardize → YYYY-MM
Flag failures: PERIOD_PARSE_FAIL

7. Customer Name Standardization

Use mapping table for canonical names
Fallback: title()
Avoid relying only on casing (entity-level normalization required)

8. Amount Casting

Cast to double
Flag failures: IS_AMOUNT_UNPARSEABLE

9. Orphan Order Handling (ORD-099)

Do NOT drop
Flag: IS_ORPHANED_ORDER = True
Exclude from joins & aggregations
Route for investigation (data inconsistency with ERP)

10. Deduplication

Key: order_no + category + period
Keep row with higher amount (conservative financial logic)
Log dropped duplicates

11. FX Conversion

Convert all currencies → VND
amount_vnd = amount × rate_to_vnd
Ensure:
VND rate = 1.0
Flag failures: IS_FX_CONVERSION_FAILED

12. Metadata columns
| Column                    | Description          |
| ------------------------- | -------------------- |
| `INGESTION_TS`            | Processing timestamp |
| `SOURCE_FILE`             | Source (long/wide)   |
| `IS_DUPLICATE`            | Duplicate flag       |
| `IS_ORPHANED_ORDER`       | Missing in ERP       |
| `IS_AMOUNT_UNPARSEABLE`   | Invalid numeric      |
| `IS_FX_CONVERSION_FAILED` | FX join failure      |

<details> 
<summary>PySpark Script</summary>

```

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql import Row
from datetime import datetime, timezone
import re

# Set legacy time parser to handle abbreviated month names like "Feb 2024"
# Spark 3.0+ strict parser cannot parse MMM yyyy format without this setting
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

ingestion_ts = datetime.now(timezone.utc).isoformat()

# =========================================================
# STEP 1: INGEST BOTH FILES
# =========================================================
financial_long_df = spark.read.option("header", True).csv(
    "Files/data/raw/financial_long_format.csv"
)
financial_wide_df = spark.read.option("header", True).csv(
    "Files/data/raw/financial_wide_format.csv"
)
exchange_rates_df = spark.read.option("header", True).csv(
    "Files/data/raw/exchange_rates_reference.csv"
)

print(f"Long rows: {financial_long_df.count()}")
print(f"Wide rows: {financial_wide_df.count()}")
print(f"Exchange rates rows: {exchange_rates_df.count()}")

display(financial_long_df)
display(financial_wide_df)
display(exchange_rates_df)


# =========================================================
# STEP 2: UNPIVOT WIDE FORMAT
# Wide columns like 31/01/2024, 28/02/2024 -> unpivot to (period, amount)
# Parse date format dd/MM/yyyy -> YYYY-MM
# Drop rows where amount is null after unpivot (sparse wide format)
# =========================================================
date_columns = [
    c for c in financial_wide_df.columns
    if re.match(r"\d{2}/\d{2}/\d{4}", c)
]
non_date_cols = [c for c in financial_wide_df.columns if c not in date_columns]

stack_expr = ", ".join([f"'{c}', `{c}`" for c in date_columns])

financial_wide_unpivoted = (
    financial_wide_df
    .selectExpr(
        *non_date_cols,
        f"stack({len(date_columns)}, {stack_expr}) as (period_raw, amount)"
    )
    .withColumn(
        "period",
        F.date_format(F.to_date(F.col("period_raw"), "dd/MM/yyyy"), "yyyy-MM")
    )
    .drop("period_raw")
    .filter(F.col("amount").isNotNull())
    .withColumn("SOURCE_FILE", F.lit("financial_wide_format.csv"))
)

financial_long_df = financial_long_df.withColumn(
    "SOURCE_FILE", F.lit("financial_long_format.csv")
)

print("=== WIDE AFTER UNPIVOT ===")
display(financial_wide_unpivoted)


# =========================================================
# STEP 3: UNION LONG + WIDE
# Both sources now share the same schema
# Cleaning applied once on combined DataFrame
# =========================================================
combined_df = financial_long_df.unionByName(
    financial_wide_unpivoted,
    allowMissingColumns=True
)

print(f"Combined row count before cleaning: {combined_df.count()}")


# =========================================================
# STEP 4: OCR FIX — replace letter O with digit 0 in amount
# Only apply when value looks like a number
# Log every fix: original value, corrected value, order identifier
#
# Additional issue found (not in known list):
# amount column may have leading/trailing spaces — handled in Step 5
# =========================================================
combined_df = combined_df.withColumn(
    "__amount_before", F.col("amount")
).withColumn(
    "amount",
    F.when(
        F.col("amount").isNotNull() &
        F.col("amount").rlike(r"^[0-9Oo\.\,\-\s]+$"),
        F.regexp_replace(F.col("amount"), r"[Oo]", "0")
    ).otherwise(F.col("amount"))
)

df_ocr_log = (
    combined_df
    .filter(
        F.coalesce(F.col("__amount_before").cast("string"), F.lit("")) !=
        F.coalesce(F.col("amount").cast("string"), F.lit(""))
    )
    .select(
        "order_no",
        F.lit("amount").alias("COLUMN_NAME"),
        F.col("__amount_before").cast("string").alias("ORIGINAL_VALUE"),
        F.col("amount").cast("string").alias("CORRECTED_VALUE"),
        F.lit("OCR_FIX").alias("LOG_TYPE"),
        "SOURCE_FILE",
        F.current_timestamp().alias("LOGGED_AT")
    )
)

combined_df = combined_df.drop("__amount_before")

print("=== OCR FIX LOG ===")
display(df_ocr_log)


# =========================================================
# STEP 5: NORMALIZE WHITESPACE + CASING
# - customer_name: trim + collapse spaces + lowercase
#   (lowercase first so canonical map in Step 6 can match reliably)
# - category: trim + collapse spaces + initcap
# - amount: trim leading/trailing spaces (additional issue found)
# =========================================================
combined_df = combined_df.withColumn(
    "customer_name",
    F.lower(F.trim(F.regexp_replace(F.col("customer_name"), r"\s+", " ")))
).withColumn(
    "category",
    F.initcap(F.trim(F.regexp_replace(F.col("category"), r"\s+", " ")))
).withColumn(
    "amount",
    F.trim(F.col("amount"))
)


# =========================================================
# STEP 6: STANDARDISE PERIOD COLUMN
# Handle all 5 known formats, output YYYY-MM
#
#   2024-01      -> yyyy-MM
#   2024/01      -> yyyy/MM
#   January 2024 -> MMMM yyyy
#   01-2024      -> MM-yyyy
#   Feb 2024     -> MMM yyyy  (requires LEGACY timeParserPolicy)
#
# Chain when().when() — first format that parses wins
# Fallback: keep original and flag as PERIOD_PARSE_FAIL
# =========================================================
date_formats = [
    "yyyy-MM",
    "yyyy/MM",
    "MMMM yyyy",
    "MM-yyyy",
    "MMM yyyy"
]

combined_df = combined_df.withColumn("__period_raw", F.col("period"))

period_expr = None
for fmt in date_formats:
    parsed = F.to_date(F.col("__period_raw"), fmt)
    formatted = F.date_format(parsed, "yyyy-MM")
    if period_expr is None:
        period_expr = F.when(parsed.isNotNull(), formatted)
    else:
        period_expr = period_expr.when(parsed.isNotNull(), formatted)

period_expr = period_expr.otherwise(F.col("__period_raw"))
combined_df = combined_df.withColumn("period", period_expr)

# Flag rows where period could not be parsed
df_period_fail_log = (
    combined_df
    .filter(F.col("period") == F.col("__period_raw"))
    .filter(~F.col("period").rlike(r"^\d{4}-\d{2}$"))
    .select(
        "order_no",
        F.lit("period").alias("COLUMN_NAME"),
        F.col("__period_raw").alias("ORIGINAL_VALUE"),
        F.lit(None).cast("string").alias("CORRECTED_VALUE"),
        F.lit("PERIOD_PARSE_FAIL").alias("LOG_TYPE"),
        "SOURCE_FILE",
        F.current_timestamp().alias("LOGGED_AT")
    )
)

combined_df = combined_df.drop("__period_raw")

print("=== PERIOD PARSE FAIL LOG ===")
display(df_period_fail_log)


# =========================================================
# STEP 7: NORMALISE CUSTOMER NAME
#
# Normalisation logic:
#   1. Lowercase in Step 5 ensures consistent matching
#   2. Explicit map resolves known variants to canonical form
#   3. Unknown names fall back to title() — at least consistently cased
#
# Why explicit map instead of just initcap:
#   "sunrise apparel" / "Sunrise Apparel" / "SUNRISE APPAREL" are the same
#   entity. initcap alone cannot resolve cross-record entity matching.
#   The map must be maintained as new variants are discovered.
#
# Known variants:
#   "sunrise apparel"      -> "Sunrise Apparel"
#   "sunrise apparel inc." -> "Sunrise Apparel"
#   "sunrise garments"     -> "Sunrise Apparel"  (same entity, different name)
#   "SUNRISE APPAREL"      -> lowercased to "sunrise apparel" in Step 5, then mapped
# =========================================================
customer_name_map = {
    "sunrise apparel": "Sunrise Apparel",
    "sunrise apparel inc.": "Sunrise Apparel",
    "sunrise garments": "Sunrise Apparel",
}

def normalize_customer_name(name):
    if name is None:
        return None
    return customer_name_map.get(name.strip().lower(), name.strip().title())

normalize_customer_name_udf = F.udf(normalize_customer_name, StringType())

combined_df = combined_df.withColumn(
    "__customer_before", F.col("customer_name")
).withColumn(
    "customer_name",
    normalize_customer_name_udf(F.col("customer_name"))
)

df_customer_norm_log = (
    combined_df
    .filter(
        F.coalesce(F.col("__customer_before"), F.lit("")) !=
        F.coalesce(F.col("customer_name"), F.lit(""))
    )
    .select(
        "order_no",
        F.lit("customer_name").alias("COLUMN_NAME"),
        F.col("__customer_before").alias("ORIGINAL_VALUE"),
        F.col("customer_name").alias("CORRECTED_VALUE"),
        F.lit("CUSTOMER_NORM").alias("LOG_TYPE"),
        "SOURCE_FILE",
        F.current_timestamp().alias("LOGGED_AT")
    )
)

combined_df = combined_df.drop("__customer_before")

print("=== CUSTOMER NORMALISATION LOG ===")
display(df_customer_norm_log)


# =========================================================
# STEP 8: CAST AMOUNT TO NUMERIC
# After OCR fix, amount should be parseable as double
# Flag rows where cast still fails — additional data quality issue
# =========================================================
combined_df = combined_df.withColumn(
    "amount_numeric",
    F.col("amount").cast("double")
).withColumn(
    "IS_AMOUNT_UNPARSEABLE",
    F.when(
        F.col("amount").isNotNull() & F.col("amount_numeric").isNull(),
        F.lit(True)
    ).otherwise(F.lit(False))
)

df_unparseable_log = (
    combined_df
    .filter(F.col("IS_AMOUNT_UNPARSEABLE") == True)
    .select(
        "order_no", "category", "period",
        F.col("amount").alias("RAW_AMOUNT"),
        F.lit("AMOUNT_NOT_NUMERIC").alias("LOG_TYPE"),
        "SOURCE_FILE",
        F.current_timestamp().alias("LOGGED_AT")
    )
)

print("=== UNPARSEABLE AMOUNT LOG ===")
display(df_unparseable_log)


# =========================================================
# STEP 9: FLAG ORD-099
#
# ORD-099 exists in financial file but has no matching order in ERP API.
# Do NOT drop — flag with IS_ORPHANED_ORDER = True.
#
# What downstream consumers must do with ORD-099:
#   1. Exclude from fact_order_cost joins — no matching ERP order exists
#   2. Route to a quarantine / exception table for manual review
#   3. Investigate: was ORD-099 deleted from ERP, never created, or a data
#      entry error? Resolution must come from the business, not the pipeline.
#   4. Do not aggregate ORD-099 amounts into any cost summary report until
#      the orphan is resolved.
# =========================================================
combined_df = combined_df.withColumn(
    "IS_ORPHANED_ORDER",
    F.when(F.col("order_no") == "ORD-099", F.lit(True)).otherwise(F.lit(False))
)

print("=== ORD-099 FLAGGED ROWS ===")
display(combined_df.filter(F.col("IS_ORPHANED_ORDER") == True))


# =========================================================
# STEP 10: DEDUPLICATE
#
# ORD-005 OUTSOURCE appears twice with different casing and amount typo.
# After OCR fix and normalisation, both rows may still differ in amount.
#
# Strategy: keep row with HIGHER amount — conservative for cost reporting.
# Log all dropped rows.
#
# Dedup key: order_no + category + period
# (customer_name intentionally excluded — already normalised)
# =========================================================
dedup_key = ["order_no", "category", "period"]

w_dedup = Window.partitionBy(*dedup_key).orderBy(F.col("amount_numeric").desc())

combined_df = (
    combined_df
    .withColumn("__dedup_rank", F.row_number().over(w_dedup))
    .withColumn(
        "IS_DUPLICATE",
        F.when(F.col("__dedup_rank") > 1, F.lit(True)).otherwise(F.lit(False))
    )
)

df_dedup_log = (
    combined_df
    .filter(F.col("IS_DUPLICATE") == True)
    .select(
        "order_no", "category", "period", "amount", "customer_name",
        F.lit("DUPLICATE_DROP").alias("LOG_TYPE"),
        F.lit("Kept row with higher amount_numeric; dedup key: order_no+category+period").alias("DETAILS"),
        "SOURCE_FILE",
        F.current_timestamp().alias("LOGGED_AT")
    )
)

combined_df = (
    combined_df
    .filter(F.col("IS_DUPLICATE") == False)
    .drop("__dedup_rank")
)

print("=== DEDUP LOG ===")
display(df_dedup_log)
print(f"Row count after dedup: {combined_df.count()}")


# =========================================================
# STEP 11: FX CONVERSION — convert all amounts to VND
#
# Strategy:
#   1. Ensure VND row exists in reference with rate_to_vnd = 1.0
#      (VND × 1 = VND — no change, but avoids null after left join)
#   2. Join on currency = currency_code
#   3. converted_amount_vnd = amount_numeric * rate_to_vnd
#   4. Flag rows where conversion fails
#
# Risk of hardcoded rates in production:
#   - Exchange rates change daily — hardcoded rates produce stale conversions
#   - A rate frozen at 2024-01 applied to 2024-12 data is silently wrong
#   - No audit trail of which rate was used for which period
#   - Production solution: fetch live rates from a provider (e.g. Open Exchange
#     Rates API) at pipeline run time, store in a rate history table,
#     join on currency + effective_date closest to the transaction period
# =========================================================
vnd_exists = exchange_rates_df.filter(F.col("currency_code") == "VND").count() > 0

if not vnd_exists:
    vnd_row = spark.createDataFrame([
        Row(
            currency="Vietnamese Dong",
            currency_code="VND",
            rate_to_vnd="1.0",
            effective_date=None,
            rate_type="FIXED"
        )
    ])
    exchange_rates_df = exchange_rates_df.unionByName(
        vnd_row, allowMissingColumns=True
    )
    print("Added VND row with rate_to_vnd = 1.0")

fx_ref = exchange_rates_df.select(
    F.col("currency_code"),
    F.col("rate_to_vnd").cast("double").alias("rate_to_vnd")
)

combined_df = (
    combined_df
    .join(fx_ref, combined_df["currency"] == fx_ref["currency_code"], how="left")
    .withColumn(
        "converted_amount_vnd",
        F.when(
            F.col("amount_numeric").isNotNull() & F.col("rate_to_vnd").isNotNull(),
            F.round(F.col("amount_numeric") * F.col("rate_to_vnd"), 2)
        ).otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "IS_FX_CONVERSION_FAILED",
        F.when(F.col("converted_amount_vnd").isNull(), F.lit(True)).otherwise(F.lit(False))
    )
    .drop("currency_code", "rate_to_vnd")
)

df_fx_fail_log = (
    combined_df
    .filter(F.col("IS_FX_CONVERSION_FAILED") == True)
    .select(
        "order_no", "category", "period", "currency", "amount",
        F.lit("FX_CONVERSION_FAILED").alias("LOG_TYPE"),
        F.lit("No matching rate in exchange_rates_reference or amount is null").alias("DETAILS"),
        "SOURCE_FILE",
        F.current_timestamp().alias("LOGGED_AT")
    )
)

print("=== FX CONVERSION FAIL LOG ===")
display(df_fx_fail_log)


# =========================================================
# STEP 12: ADD INGESTION METADATA + FINAL FLAGS
# =========================================================
combined_df = combined_df.withColumn("INGESTION_TS", F.lit(ingestion_ts))

# =========================================================
# PREVIEW FINAL SILVER OUTPUT
# =========================================================
print("=== FINANCIAL SILVER — FINAL ===")
display(combined_df)

print("\n=== SUMMARY ===")
print(f"Total rows: {combined_df.count()}")
print(f"Orphaned orders (ORD-099): {combined_df.filter(F.col('IS_ORPHANED_ORDER') == True).count()}")
print(f"FX conversion failed: {combined_df.filter(F.col('IS_FX_CONVERSION_FAILED') == True).count()}")
print(f"Unparseable amounts: {combined_df.filter(F.col('IS_AMOUNT_UNPARSEABLE') == True).count()}")


```
</details>

#### TASK 5C: Handle Mixed Units

1. Data Type for `actual_value`

**Decision: `DECIMAL(18,4)`**

| Type            | Problem                                                                 |
|-----------------|-------------------------------------------------------------------------|
| **STRING**      | Cannot perform `SUM`, `AVG`, or any calculation. Hides data errors like OCR typos. Not suitable for analytics. |
| **FLOAT**       | Floating point precision error: `0.1 + 0.2 = 0.30000000004`. Unreliable for financial data. |
| **DECIMAL(18,4)**| Exact precision, supports calculation, suitable for financial and ratio values. |

###### Conclusion:
- **`DECIMAL(18,4)`** ensures precision and is fully compatible with BI tools and SQL engines, supporting calculations, aggregations, and handling financial and ratio-based KPIs accurately.

---

2. Preventing Accidental SUM Across Mixed-Unit KPIs
**Problem**:  
Power BI defaults to `SUM` on all numeric columns.  
For example: `SUM(Line Efficiency %) + SUM(FOB Revenue USD)` = meaningless number.

| Solution                                         | Description                                                                                             |
|--------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| **Solution 1 — Unit column in fact table**       | Every row in `FACT_KPI_ACTUAL` has a `unit` column (`%`, `NUMBER`, `VND`). BI reports must always filter or group by unit before aggregating. |
| **Solution 2 — Set "Don't Summarize" in Power BI** | `actual_value` is configured as "Don't Summarize" by default in the Power BI data model. Users must explicitly choose the aggregation method. |
| **Solution 3 — Gold layer pre-aggregation**      | Gold layer applies the correct `aggregation_method` from `DIM_KPI` (`SUM`, `AVERAGE`, `LAST`) per KPI before serving to BI. BI tool does not need to aggregate raw fact rows — it reads pre-aggregated Gold values. |

###### Conclusion:
These solutions ensure that KPIs with mixed units (e.g., percentage and numeric values) are properly handled in BI tools, preventing accidental mis-aggregation and ensuring correct results.

---

3. Store % KPIs as 0.85 or 85?

**Decision**: Store as `0.85` (decimal ratio)

| Representation | Risk                                                                                                         |
|----------------|--------------------------------------------------------------------------------------------------------------|
| **85**         | User may not know if `85` means `85%` or `85` units. Achievement formula breaks if someone stores `85` instead of `0.85`: `0.85 / 85 = 0.01` — silently wrong. |
| **0.85**       | Range validation is easy: value must be between 0 and 1. Consistent with most analytics systems. Display layer multiplies by 100 to show `85%`. |

###### Standard:
- All `%` KPIs are stored as decimal ratios (0–1 range).
- Display formatting (×100 + `%` symbol) is handled at the BI/Gold layer, not in the fact table.

###### Validation Rule added to DQ framework:
- If `unit = '%'` and `actual_value > 1` → flag `WARNING`
  - Likely stored as `85` instead of `0.85`.

---

4. Achievement % Formulas
The **Direction** for each KPI is stored in the `DIM_KPI.direction` column and read dynamically by the pipeline — meaning no hardcoding per KPI name.

##### **(a) % KPI — Lower is Better**  
*Example*: OT Cost Ratio, target = `0.12`  
Formula: `achievement_pct = target / actual`

| Actual Value | Calculation           | Interpretation         |
|--------------|----------------------|------------------------|
| `0.09`       | `0.12 / 0.09 = 133.3%`   | Above 100% = good      |
| `0.15`       | `0.12 / 0.15 = 80.0%`     | Below 100% = bad      |
| `0`          | `NULL`                | Flag in DQ log — divide by zero, needs business review |

##### **(b) % KPI — Higher is Better**  
*Example*: Line Efficiency, target = `0.85`  
Formula: `achievement_pct = actual / target`

| Actual Value | Calculation            | Interpretation         |
|--------------|------------------------|------------------------|
| `0.90`       | `0.90 / 0.85 = 105.9%`       | Above 100% = good      |
| `0.75`       | `0.75 / 0.85 = 88.2%`      | Below 100% = bad      |

##### **(c) Number KPI — SUM Aggregation**  
*Example*: FOB Revenue, target = `1,200,000`  
Formula: `achievement_pct = actual / target`

| Actual Value | Calculation              | Interpretation         |
|--------------|--------------------------|------------------------|
| `1,500,000`  | `1,500,000 / 1,200,000 = 125.0%`   | Above target           |
| `900,000`    | `900,000 / 1,200,000 = 75.0%`       | Below target           |

##### **(d) Number KPI — Target is Zero**  
*Example*: Lost Time Injuries, target = `0`  
Formula: `achievement_pct = actual / target`  
→ `actual / 0 = undefined` → cannot calculate.

**Decision**:
- `achievement_pct = NULL` (not calculable)
- Flag: `zero_target_kpi = True`
- Report the `actual_value` directly instead of `achievement_pct`
- Business interpretation: `0` injuries = perfect, any number of injuries = bad.

**Why not use a workaround like `(1 - actual)`**:
- This would require hardcoding logic for each KPI, which contradicts the goal of dynamically reading the direction from `DIM_KPI`.

---

##### **Summary Table**

| KPI                     | Direction        | Formula                     | Target = 0   |  
|-------------------------|------------------|-----------------------------|--------------|
| **OT Cost Ratio**        | Lower is better  | `target / actual`           | N/A          |
| **Line Efficiency**      | Higher is better | `actual / target`           | N/A          |
| **FOB Revenue**          | Higher is better | `actual / target`           | N/A          |
| **Lost Time Injuries**   | Lower is better  | `target / actual`           | `NULL` — report absolute value |


### Part6: Schema Evolution and Flexibility 
#### Task 6A: usiness Changes :Model Survival Test
| #     | Business Change                                                              | How the Model Handles It                                                                                                                                                                                                                                                                                                          |
| ----- | ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1** | A new KPI **“Sustainability Score”** is added under new Sub Pillar **“ESG”** | **Fully handled (schema-flexible)**. Pipeline reads KPI definitions dynamically from `kpi_master_dim.csv`. Adding a new row automatically introduces the KPI in the next run. No code changes required.                                                                                                                         |
| **2** | **On Time Delivery Rate** changes unit from `%` → `Number` mid-year          | **Partially handled (SCD Type 2)**. `DIM_KPI` creates a new version row with the new unit. Fact joins correctly by time. However, values before and after change are not comparable. Gold layer must flag `unit_changed = True` and block cross-period aggregation. Business decision required (restate history or split KPI). |
| **3** | **Line Efficiency for Line D** starts from M7 (M1–M6 blank by design)        | **Handled correctly (missing-aware design)**. Rows for M1–M6 exist with `actual_value = NULL` and `is_missing_actual = True`. This is treated as *expected missing*, not data error. Gold excludes NULL from aggregation.                                                                                                       |
| **4** | Sub Pillar renamed from **“Cost Efficiency” → “Cost Control”**               | **Handled via SCD Type 2**. Old record expires, new record created. Historical reports remain unchanged, new reports reflect updated name. No impact on fact table.                                                                                                                                                             |
| **5** | **OT Cost Ratio target** changes from `0.12 → 0.10` starting M6              | **Handled via SCD Type 2**. Two valid records in `DIM_KPI`. Fact joins by period, ensuring correct target per month. No need to update historical fact data.                                                                                                                                                                    |
| **6** | **FOB Revenue now tracked by Department (new grain)**                        | **Breaking change (grain violation)**. Current grain = KPI × Month. New requirement = KPI × Month × Department. Requires schema redesign: add `dept_sk` to fact. Historical data lacks department breakdown → `NULL`. Downstream reports must be updated. Comparability issue must be confirmed with business.                  |
| **7** | Finance adds **3 new cost categories** in financial file                     | **Handled (schema-on-read)**. `category` stored as raw attribute in fact table (not strict dimension FK). New values flow automatically into Silver/Gold. Only action: validate with business to avoid typo propagation.                                                                                                        |
| **8** | Two KPIs are **retired (no actuals after M9)**                               | **Partially handled**. No new records appear after M9, historical data preserved. Requires manual update: set `is_active = N` in `DIM_KPI`. Gold excludes inactive KPIs. Late-arriving data should be flagged `is_orphaned = True`.                                                                                            |


### Part 7: Data Quality Framework
#### Task 7A: Required Quaity Checks

| Level | Meaning         | Pipeline Action            |
| ----- | ----------------| -------------------------- |
|PASSED | No issues found     |Continue to Gold       |
|WARNING | Degraded but usable data — needs business review       |Continue to Gold with alert       |
|FAILED | Data is not trustworthy — pipeline must stop       |Halt — do not build Gold       |



**Silver Output Not Empty**
- **Idea**: Row count of Siver table after transformation
- Logic
```
  row_count = df_silver.count()
  PASSED → row_count > 0
  FAILED → row_count = 0

```
<details> 
<summary>PySpark Script</summary>

```
# =========================================================
# CHECK 1 — SILVER NOT EMPTY
# =========================================================
def check_not_empty(spark, df: DataFrame, table_name: str) -> DataFrame:
    """Silver output must have at least 1 row."""
    row_count = df.count()
 
    if row_count > 0:
        result = "PASSED"
        details = f"Row count: {row_count}"
    else:
        result = "FAILED"
        details = "Silver output is empty — pipeline should not proceed to Gold"
 
    print(f"[{result}] {table_name} | not_empty | {details}")
    return build_log_row(spark, "silver_not_empty", table_name,
                         result, row_count, details)
 
```

</details>

**Mandatory Columns Present and Not Fully Null**
- Column exists in schema
- Column is not 100% null
- Logic
```
  Column missing from schema → FAILED
  Column exists but fully null → FAILED
  Column exists and has data  → PASSED
```

<details> 
<summary>PySpark Script</summary>

```
# =========================================================
# CHECK 2 — MANDATORY COLUMNS PRESENT AND NOT FULLY NULL
# =========================================================
def check_mandatory_columns(spark, df: DataFrame, table_name: str,
                             mandatory_cols: List[str]) -> DataFrame:
    """
    FAILED if:
    - Any mandatory column does not exist in schema
    - Any mandatory column is fully null (100% null values)
    """
    row_count = df.count()
    issues = []
 
    for col in mandatory_cols:
        # Check column exists
        if col not in df.columns:
            issues.append(f"Column '{col}' missing from schema")
            continue
 
        # Check column not fully null
        non_null_count = df.filter(F.col(col).isNotNull()).count()
        if non_null_count == 0:
            issues.append(f"Column '{col}' is fully null")
 
    if issues:
        result = "FAILED"
        details = " | ".join(issues)
    else:
        result = "PASSED"
        details = f"All mandatory columns present and not fully null: {mandatory_cols}"
 
    print(f"[{result}] {table_name} | mandatory_columns | {details}")
    return build_log_row(spark, "mandatory_columns", table_name,
                         result, row_count, details)

```
</details>

**Orphaned Records Flagged**
- ``IS_ORPHANED`` column exists in Silver
- Orphaned records are present and flagged — not silently dropped
- Logic
```
  IS_ORPHANED column missing       → FAILED
  IS_ORPHANED column exists
    + orphaned_count > 0           → WARNING (needs business review)
    + orphaned_count = 0           → PASSED
```

<details> 
<summary>PySpark Script</summary>
  
```
# =========================================================
# CHECK 3 — ORPHANED RECORDS FLAGGED
# =========================================================
def check_orphaned_flagged(spark, df: DataFrame, table_name: str) -> DataFrame:
    """
    FAILED  — IS_ORPHANED column does not exist
    WARNING — IS_ORPHANED column exists and has records = True
    PASSED  — IS_ORPHANED column exists, no orphaned records
    """
    row_count = df.count()
 
    if "IS_ORPHANED" not in df.columns:
        result = "FAILED"
        details = "IS_ORPHANED column missing — orphaned records not being tracked"
    else:
        orphaned_count = df.filter(F.col("IS_ORPHANED") == True).count()
 
        if orphaned_count > 0:
            result = "WARNING"
            details = (f"{orphaned_count} orphaned records found. "
                      "These are flagged and excluded from Gold aggregations. "
                      "Business review required.")
        else:
            result = "PASSED"
            details = "No orphaned records found"
 
    print(f"[{result}] {table_name} | orphaned_flagged | {details}")
    return build_log_row(spark, "orphaned_flagged", table_name,
                         result, row_count, details)


```
</details>

**No Duplicates After Deduplication**
- Group by composite key — if any group has count > 1, duplicates remain.
- Logic

```
duplicate_count = 0  → PASSED
duplicate_count > 0  → FAILED
```

<details> 
<summary>PySpark Script</summary>
  
```
# =========================================================
# CHECK 4 — NO DUPLICATES AFTER DEDUP
# =========================================================
def check_no_duplicates(spark, df: DataFrame, table_name: str,
                         dedup_key: List[str]) -> DataFrame:
    """
    PASSED — no duplicate rows found after deduplication
    FAILED — duplicate rows still exist
    """
    row_count = df.count()
 
    from pyspark.sql import Window
    window = Window.partitionBy(dedup_key)
 
    duplicate_count = (
        df.withColumn("_cnt", F.count("*").over(window))
          .filter(F.col("_cnt") > 1)
          .count()
    )
 
    if duplicate_count == 0:
        result = "PASSED"
        details = f"No duplicates found on key: {dedup_key}"
    else:
        result = "FAILED"
        details = (f"{duplicate_count} duplicate rows remain on key: {dedup_key}. "
                  "Deduplication did not complete correctly.")
 
    print(f"[{result}] {table_name} | no_duplicates | {details}")
    return build_log_row(spark, "no_duplicates", table_name,
                         result, row_count, details)

```
</details>

**No OCR Typos in Numeric Columns**
- Cast numeric columns to DOUBLE. If new nulls appear after cast that
did not exist before cast, it means some values failed to parse —
OCR typos likely remain.
- Logic
```
null_before = nulls in column before cast
null_after  = nulls in column after cast to DOUBLE

new_nulls = null_after - null_before

new_nulls = 0  → PASSED
new_nulls > 0  → FAILED
```

<details> 
<summary>PySpark Script</summary>

```
# =========================================================
# CHECK 5 — NO OCR TYPOS IN NUMERIC COLUMNS
# =========================================================
def check_no_ocr_typos(spark, df: DataFrame, table_name: str,
                        numeric_cols: List[str]) -> DataFrame:
    """
    PASSED — all numeric columns cast to DOUBLE without producing new nulls
    FAILED — cast produces new nulls = OCR typos remain
    """
    row_count = df.count()
    issues = []
 
    for col in numeric_cols:
        if col not in df.columns:
            continue
 
        # Count nulls before cast
        null_before = df.filter(F.col(col).isNull()).count()
 
        # Count nulls after cast to double
        null_after = (
            df.withColumn("_cast", F.col(col).cast("double"))
              .filter(F.col("_cast").isNull())
              .count()
        )
 
        # New nulls = values that failed to cast = OCR typos remain
        new_nulls = null_after - null_before
        if new_nulls > 0:
            issues.append(
                f"Column '{col}': {new_nulls} values failed to cast — OCR typos remain"
            )
 
    if issues:
        result = "FAILED"
        details = " | ".join(issues)
    else:
        result = "PASSED"
        details = f"All numeric columns parse correctly: {numeric_cols}"
 
    print(f"[{result}] {table_name} | no_ocr_typos | {details}")
    return build_log_row(spark, "no_ocr_typos", table_name,
                         result, row_count, details)

```

</details>


**Log All Results**

Every check writes one row to `data_quality_log` with result
PASSED / WARNING / FAILED.

To view results after pipeline runs:
```
dq_log = run_dq_checks(spark, df_kpi_silver, df_financial_silver)
display(dq_log)

can_proceed = check_dq_gate(dq_log)
```


### TASK 8A- Airflow DAG
My note on experience

I want to be transparent that I have not used Apache Airflow in production in my previous role.

However, I do have experience with a similar orchestration pattern using Windows Task Scheduler on my laptop/server environment. In that setup, I scheduled my data pipeline to run daily at 13:30, orchestrated multiple scripts in sequence, and wrote logs to monitor whether the pipeline completed successfully or failed at a specific step.

So while Airflow itself is new to me, the underlying concepts are not completely new:

- scheduling jobs on a fixed cadence
- running tasks in dependency order
- logging execution status
- retrying or rerunning failed processes
- preventing downstream steps from running when upstream data is invalid

For this assessment, I will design the DAG based on my understanding of orchestration concepts and the task dependencies provided in the specification. Where I am not fully certain about Airflow-specific implementation details, I will state my assumptions clearly rather than pretend production experience I do not have.


### TASK 9A - Gold Layer Scripts

**Business Purpose of Gold Outputs**

The following interpretations are based on my understanding of the problem statement and the role of the Gold layer in a typical data warehouse architecture.

I am not making arbitrary assumptions. Instead, I map each required Gold output to the business question it is intended to answer, based on the structure and fields defined in the task.

If there are differences from the actual business intent, they should be clarified with stakeholders. However, the design below reflects my current understanding and reasoning.

---

#### 1. KPI Monthly Achievement Summary
This table provides a detailed monthly view of KPI performance.

It answers:
- What is the actual value of each KPI for a given month?
- What is the target value?
- How well did the KPI perform relative to target (achievement_pct)?
- Are there any missing actual values that require attention?

This output is primarily used for KPI monitoring and detailed performance analysis.

---

#### 2. Pillar Monthly Rollup
This table aggregates KPI performance at the pillar level.

It answers:
- What is the overall performance of each pillar for a given month?
- How do multiple KPIs within the same pillar perform collectively?

Only KPIs with valid actual values are included to avoid skewing the average.

This output is used for high-level performance tracking and executive dashboards.

---

#### 3. Order Cost Summary
This table summarizes operational cost per customer per month in a standard currency (VND).

It answers:
- How much cost is incurred per customer per month?
- What is the monthly financial footprint of each customer?

This output supports financial reporting and cost analysis.

---

#### 4. YTD KPI Actuals
This table provides cumulative KPI performance within a year.

It answers:
- What is the year-to-date cumulative actual value of each KPI?
- How is KPI performance progressing over time?

Only months with valid actual values are included to ensure data accuracy.

This output is used for tracking KPI progress against yearly targets.


### PART 10:
#### TASK 10A: Git, CI/CD, and Naming Conventions 
The answers below reflect my current knowledge and
my understand. Where I am not certain, I document what I know,
what I would do, and what I would learn or confirm with the team.
I prefer to be transparent about my limits rather than assume
something I cannot defend.

1. Git Repository Struture
Based on my experience, I would structure the repository to separate
concerns clearly — each folder has one responsibility.

```
  project-root/
  │
  ├── dags/                      # Airflow DAGs / orchestration
  ├── pipelines/                 # Transformation logic per layer
  │   ├── bronze/
  │   ├── silver/
  │   └── gold/
  ├── ingestion/                 # API and file ingestion scripts
  ├── common/                    # Shared helpers, utilities, constants
  ├── tests/                     # Unit tests and integration tests
  ├── configs/                   # Config files per environment
  ├── docs/                      # README, design docs, architecture
  ├── sql/                       # SQL scripts if needed
  └── requiremen
```
2. CI/CD Pipeline on PR to Main
I do not have experience setting up CI/CD pipelines.
This is a gap I am aware of and would prioritize learning on the job.
I am not going to document something I cannot defend in an interview.
3. Naming onventions
   - ``snake_case`` for everything — no mixing with camelCase
   - ``UPPER_SNAKE_CASE`` for metadata/system columns
   - ``lowercase`` for business columns
   - Be descriptive but concise — avoid abbreviations that are not obvious

**Tables**

```
bronze_kpi_actual_raw
silver_kpi_actual_clean
gold_kpi_achievement_summary

dim_kpi
dim_date
dim_department
dim_customer
dim_currency

fact_kpi_actual
fact_order_cost
  ```

**Column**
```
-- Business columns (lowercase snake_case)
kpi_id, kpi_name, actual_value, target_value
order_no, category, period, converted_amount_vnd

-- Metadata columns (UPPER_SNAKE_CASE)
ROW_HASH, INGESTION_TS, SOURCE_FILE
IS_DUPLICATE, IS_ORPHANED, IS_CONFLICT
```

**DAG IDs**
```
kpi_monthly_pipeline
orders_weekly_pipeline
erp_api_ingestion_daily
silver_to_gold_kpi_monthly
```

**Task IDs**
```
extract_order_api
load_bronze_kpi_raw
transform_silver_orders
build_gold_kpi_summary
run_data_quality_checks
publish_gold_tables
```

**Pipeline names**
```
kpi_reporting_pipeline
orders_reconciliation_pipeline
erp_ingestion_pipeline
monthly_kpi_aggregation_pipeline
```

**File path in data lake**
```
/bronze/kpi/source=excel/year=2026/month=03/file.parquet
/silver/kpi/year=2026/month=03/data.parquet
/gold/kpi_monthly_summary/year=2026/month=03/data.parquet

/bronze/orders/source=api/year=2026/month=03/day=15/file.parquet
/silver/orders/year=2026/month=03/data.parquet
/gold/orders_monthly_rollup/year=2026/month=03/data.parquet
```

4. Why Consistent Naming Matters
Consistent naming helps every team member navigate the codebase,
understand data structures, and avoid confusion between files,
scripts, and tables.

#### Task 10C: Service Principal and Credentials 
- I do not have hands-on experience with Azure Service
Principals or credential management in Azure Data Factory.
- I am not going to document something I cannot defend in an interview.
What I understand at a high level: pipelines should not run using
individual developer accounts because if that person leaves or their
password expires, the pipeline breaks. The correct solution involves
a dedicated service identity that is not tied to any individual person.
This is a gap I am aware of and would prioritize learning on the job.
