# Data Lineage

## Pipeline Flow
```mermaid
flowchart TD
    subgraph Sources
        A[(PostgreSQL\nSource DB)]
        B[CSV Files\nSFTP Drop]
        C[Marketing\nREST API]
    end

    subgraph raw schema
        D[raw.orders]
        E[raw.order_items]
        F[raw.customers]
        G[raw.products]
        H[raw.shipments]
        I[raw.campaigns]
    end

    subgraph staging schema
        J[stg_orders]
        K[stg_order_items]
        L[stg_customers]
        M[stg_products]
        N[stg_shipments]
        O[stg_campaigns]
    end

    subgraph marts schema
        P[dim_customers]
        Q[dim_products]
        R[dim_date]
        S[fact_sales]
    end

    subgraph Quality
        T[pipeline_audit]
    end

    A --> D & E & F & G
    B --> H
    C --> I

    D --> J
    E --> K
    F --> L
    G --> M
    H --> N
    I --> O

    L --> P
    M --> Q
    J & K & M & N --> S
    P & Q & R --> S

    S --> T
```

## Transformation Rules Summary

| From | To | Key Transformations |
|------|----|-------------------|
| raw.customers | stg_customers | Lowercase IDs, title-case names, cast dates |
| raw.orders | stg_orders | Lowercase status, filter test orders, cast types |
| raw.order_items | stg_order_items | Cast types, filter qty <= 0, add line_revenue |
| raw.shipments | stg_shipments | Normalise dates, strip carrier casing, fix weight |
| raw.campaigns | stg_campaigns | Unpack JSON array into rows |
| stg_* | fact_sales | Join all sources, calculate profit and margin |