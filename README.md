# test-mermaid

graph TD
    subgraph "Main Process"
        A[Cloud Scheduler Trigger] --> B{Determine Function Type\n(Daily vs. Backfill)};
        B -->|Daily| C[Calculate Date Range\nbased on LOOKBACK_WINDOW];
        B -->|Backfill| D[Use start_date & end_date from request];
        C --> E;
        D --> E[Iterate Date Range &\nCheck for GA4 Export Tables];
        E --> F{Tables Found?};
        F -->|No| G[Log "No table found"\nand Terminate];
        F -->|Yes| H[Run BigQuery SQL Queries\nto Prepare Data];
        H --> I[Query BigQuery for New Records\n(Deduplication)];
    end

    subgraph "Parallel Upload & Logging"
        I --> J{Records to Process?};
        J -->|Yes| K[Initialize ThreadPoolExecutor];
        K --> L[Loop through each Record];
        L --> M{Required Keys Present?};
        M -->|No| N[Log Error & Add to Failed Records];
        M -->|Yes| O[Create GA4 Payload];
        O --> P[_send_to_ga4\n(HTTP POST with Retries)];
        P --> Q{Upload Successful?};
        Q -->|Yes| R[Add to all_uploaded_records];
        Q -->|No| S[Add to all_failed_records];
        L --> K;
        J -->|No| T[Log "No new records" & Terminate];
    end

    subgraph "Finalization"
        K --> U[Wait for all tasks to complete];
        U --> V{Uploaded Records?};
        V -->|Yes| W[Write UUIDs to BigQuery Operation Table];
        W --> X[Log Success Count];
        V -->|No| Y[Log Success Count];
        U --> Z{Failed Records?};
        Z -->|Yes| AA[Write to BigQuery Dead-Letter Table];
        AA --> AB[Log Dead-Letter Count];
        Z -->|No| AC[Log Dead-Letter Count];
    end

    G --> END((End));
    T --> END;
    AB --> END;
    AC --> END;
    X --> END;
    Y --> END;
