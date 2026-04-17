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
| One row per order × cost category | Incorrect | This grain captures cost breakdown but loses the time dimension. Since financial data is tracked monthly, removing month prevents trend analysis and period-based reporting. |
| One row per order × cost category × month | Correct | This is the correct grain because it represents the lowest level of financial measurement. It aligns with the source structure (order × cost category × month), supports accurate cost breakdown, time-based analysis, and flexible aggregation across dimensions such as customer and season. |
| One row per customer × month | Incorrect | This is an aggregated grain and does not represent atomic transactional data. It removes order-level detail and prevents accurate allocation of cost categories. |

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







