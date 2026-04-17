# UA_DATA_ENGINEER_ASSESSMENT - VO NGOC THANH
## Part 1: Grain Definition

### TASK 1A: Define the Grain of Each Fact Table 

#### Workstream A :FACT_KPI_ACTUAL
</details>

<br>

| Grain Option | Correct / Incorrect? | Why |
|--------|------------|---------|
| One row per KPI per Year (annual rollup) | Incorrect | This grain is too aggregated and removes the monthly time dimension, preventing trend analysis, month-over-month comparison, and YTD calculations. It represents a summary table, not a fact table. |
| One row per KPI Name per Month | Incorrect | This grain does not include Sub KPI Type, which is required to uniquely identify KPI records. Multiple sub-types would be merged, leading to incorrect aggregation and mismatched targets. |
| One row per Pillar per Month | Incorrect | Pillar is a higher-level aggregation above KPI. This grain is suitable for reporting (Gold layer), not for the base fact table, as it removes KPI-level detail. |
| One row per KPI + Sub KPI Type per Month | Correct | This is the correct grain because it represents the lowest level of KPI measurement. It aligns with the source structure (KPI × Sub KPI Type × Month), supports accurate joins to DIM_KPI, and enables flexible aggregation across dimensions. The grain must reflect how the KPI is measured in the source system, not how it is consumed in reports. |
| One row per KPI + Sub KPI Type + Department per Month | Incorrect | Although department may exist as a dimension, including it in the fact grain is unnecessary unless the KPI is explicitly measured at department level. This would increase data sparsity and risk duplication. |

---

#### Workstream B — FACT_ORDER_COST

| Grain Option | Correct / Incorrect? | Why |
|--------------|---------------------|-----|
| One row per order | Incorrect | This grain is too aggregated and removes cost category and time dimensions. It prevents detailed cost breakdown and accurate financial analysis across categories and periods. |
| One row per order x cost category | Incorrect | This grain captures cost breakdown but loses the time dimension. Since financial data is tracked monthly, removing month prevents trend analysis and period-based reporting. |
| One row per order x cost category x month | Correct | This is the correct grain because it represents the lowest level of financial measurement. It aligns with the source structure (order × cost category × month), supports accurate cost breakdown, time-based analysis, and flexible aggregation across dimensions such as customer and season. |
| One row per customer x month | Incorrect | This is an aggregated grain and does not represent atomic transactional data. It removes order-level detail and prevents accurate allocation of cost categories. |

**Follow-up: Adding Region Dimension**

The addition of a new Region dimension does not change the grain if it is only a descriptive attribute.

Workstream A: Region can be derived from existing dimensions (e.g., department or KPI ownership) and added as a foreign key.\
Workstream B: Region can be derived from customer attributes (e.g., location) and included as a dimension.

In both cases, the grain remains unchanged because Region does not affect the level of measurement.

However, if the business starts measuring data separately by Region (e.g., KPI values or costs differ by region), then Region must be included in the grain.

**Conclusion:**
Grain remains stable unless a new dimension introduces a new level of measurement.

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

### TASK 2A BOTH: Bronze / Silver / Gold Layer Design

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

