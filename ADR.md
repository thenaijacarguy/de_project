# Architecture Decision Record

An ADR documents significant design decisions made during the project,
the context that led to them, and the trade-offs involved. Future
engineers (or future you) can read this to understand *why* the
pipeline works the way it does, not just *how*.

---

## ADR-001: Star Schema for the Marts Layer

**Date:** 2024-01-15
**Status:** Accepted

### Context
We needed to choose a data model for the analytics layer that analysts
would query directly. The options were a flat wide table (denormalised),
a normalised relational model, or a star schema.

### Decision
We chose a **star schema** with one central fact table (fact_sales) and
separate dimension tables (dim_customers, dim_products, dim_date).

### Reasons
- Star schemas are the industry standard for analytical workloads and
  are what most BI tools (Tableau, Power BI, Looker) are optimised for
- Queries are simple to write — analysts join fact to one dimension at
  a time rather than navigating a complex relational model
- Dimension tables give a single source of truth for descriptive
  attributes — customer region is defined once in dim_customers,
  not repeated in every fact row
- Performance is predictable — the fact table is wide and flat,
  making aggregations fast

### Trade-offs
- Changes to dimension attributes (e.g. a customer moving region)
  require a decision about history — we chose SCD Type 1 (overwrite)
  for simplicity, which means we lose historical attribute values
- A star schema requires more upfront design thinking than a flat table

---

## ADR-002: Full Refresh vs Incremental Loading

**Date:** 2024-01-15
**Status:** Accepted

### Context
For each extraction script we had to choose between two loading strategies:
- **Full refresh**: truncate the raw table and reload everything each run
- **Incremental**: only extract new or changed rows since the last run

### Decision
We use **full refresh** for small dimension tables (customers, products)
and **idempotent append** for event tables (shipments, campaigns).

### Reasons
- Full refresh is simpler to implement and debug — there is no watermark
  logic to maintain and no risk of missing records due to an incorrect
  high-water mark
- For small tables (< 10,000 rows) the performance difference between
  full refresh and incremental is negligible
- Idempotent append for shipments and campaigns avoids duplicates while
  still accumulating historical records across multiple file drops

### Trade-offs
- Full refresh does not scale to large tables. If orders grew to
  millions of rows, full refresh would be too slow and expensive.
  The correct solution at that scale would be CDC (Change Data Capture)
  using a tool like Debezium to stream only changed rows
- Incremental loading requires careful watermark management — getting
  the high-water mark wrong means either missing data or loading
  duplicates, both of which are hard to detect

---

## ADR-003: Storing Raw API Responses as JSON

**Date:** 2024-01-15
**Status:** Accepted

### Context
When extracting from the marketing API we had two options for what to
store in the raw layer — parse the JSON immediately into columns, or
store the entire response as a raw JSON string.

### Decision
We store the **complete JSON response as a single TEXT column** in
raw.campaigns, and unpack it into columns only in the dbt staging model.

### Reasons
- If the API changes its response structure in the future, we still have
  the original data and can reprocess it with an updated staging model.
  If we had parsed at extraction time, a schema change would require
  re-fetching all historical data from the API
- The raw layer's job is to be a faithful copy of the source — parsing
  is a transformation and belongs in the staging layer
- JSON storage makes the extraction script simpler and less brittle —
  it doesn't need to know anything about the response structure

### Trade-offs
- Raw JSON is not queryable without parsing — analysts cannot query
  raw.campaigns directly and get meaningful results
- JSON blobs use more storage than typed columns, though this is
  negligible at our data volumes