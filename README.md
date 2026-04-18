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
- Idea for this step is ingest data Bronze to Silver to transformation

2. Fix OCR typos

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
3. 123

  1. Lưu giá trị cũ
  2. Apply TRIM + UPPER
  3. So sánh cũ vs mới
  4. Nếu khác → log
  5. Update column
-> sau khi dedup thì kpi_actual_long.csv bị dedup với các cột sau
  - FS || Revenue || Commercial || FOB Revenue || Export || NUMBER ||
  - OF || Productivity || Production || Line Efficiency || Line A || % ||
  - OF || Training || HR || Training Completion || Internal || %
    
4. 3
5. 3
6. 3
7. 3
8. 3
