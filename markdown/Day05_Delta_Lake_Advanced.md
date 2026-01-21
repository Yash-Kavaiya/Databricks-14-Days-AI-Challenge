# Delta Lake Advanced - Comprehensive Notes

## Table of Contents
1. [Introduction to Delta Lake Architecture](#1-introduction-to-delta-lake-architecture)
2. [Time Travel (Version History)](#2-time-travel-version-history)
3. [MERGE Operations (Upserts)](#3-merge-operations-upserts)
4. [OPTIMIZE & ZORDER](#4-optimize--zorder)
5. [VACUUM for Cleanup](#5-vacuum-for-cleanup)
6. [Practical Tasks & Implementations](#6-practical-tasks--implementations)
7. [Best Practices & Performance Tips](#7-best-practices--performance-tips)

---

## 1. Introduction to Delta Lake Architecture

Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads. It sits on top of your existing data lake (typically cloud object storage like S3, ADLS, or GCS) and adds reliability, performance, and governance features.

### Core Components

Delta Lake consists of two primary components working together:

**Transaction Log (_delta_log)**: This is the heart of Delta Lake. It's a folder containing JSON files that record every change made to the table. Each JSON file represents a "commit" and contains information about what files were added, removed, or modified. The transaction log enables ACID properties and time travel capabilities.

**Parquet Data Files**: The actual data is stored in Parquet format, which provides efficient columnar storage and compression. When you write to a Delta table, new Parquet files are created, and the transaction log is updated to track these changes.

```
delta_table/
├── _delta_log/
│   ├── 00000000000000000000.json   # Version 0 commit
│   ├── 00000000000000000001.json   # Version 1 commit
│   ├── 00000000000000000002.json   # Version 2 commit
│   └── 00000000000000000010.checkpoint.parquet  # Checkpoint file
├── part-00000-xxx.parquet          # Data file 1
├── part-00001-xxx.parquet          # Data file 2
└── part-00002-xxx.parquet          # Data file 3
```

### How Transactions Work

When you perform any operation (INSERT, UPDATE, DELETE, MERGE), Delta Lake follows this process:

1. **Read Current State**: Delta Lake reads the transaction log to understand the current table state
2. **Perform Operation**: The operation is executed, creating new Parquet files if needed
3. **Commit Changes**: A new JSON file is written to the transaction log atomically
4. **Conflict Resolution**: If another operation modified the table concurrently, Delta Lake uses optimistic concurrency control to resolve conflicts

### ACID Properties Explained

**Atomicity**: Each transaction either completely succeeds or completely fails. If a write operation fails midway, the partial changes are not visible to readers.

**Consistency**: The table always moves from one valid state to another. Constraints and schema enforcement ensure data integrity.

**Isolation**: Concurrent transactions don't interfere with each other. Readers see a consistent snapshot of data while writers make changes.

**Durability**: Once a transaction commits, the changes are permanent and survive system failures.

---

## 2. Time Travel (Version History)

Time travel is one of Delta Lake's most powerful features. It allows you to query, restore, and audit previous versions of your data. Every change to a Delta table creates a new version, and Delta Lake maintains a complete history of these versions.

### How Time Travel Works Internally

When you query a specific version, Delta Lake reads the transaction log up to that version number. It reconstructs the table state by understanding which files existed at that point in time. The actual data files might still exist on disk (until VACUUM removes them), allowing Delta Lake to serve historical queries.

Each version in the transaction log contains:
- **Add actions**: Files that were added in this version
- **Remove actions**: Files that were logically deleted (marked as removed but physically still present)
- **Metadata**: Schema changes, partition information, and configuration
- **Transaction information**: Application ID, timestamp, and operation details

### Querying Historical Data

There are three ways to access historical versions:

**Method 1: Version Number**

```sql
-- Query a specific version number
SELECT * FROM my_table VERSION AS OF 5;

-- Using PySpark
df = spark.read.format("delta").option("versionAsOf", 5).load("/path/to/table")
```

**Method 2: Timestamp**

```sql
-- Query data as it existed at a specific timestamp
SELECT * FROM my_table TIMESTAMP AS OF '2024-01-15 10:30:00';

-- Using PySpark
df = spark.read.format("delta").option("timestampAsOf", "2024-01-15 10:30:00").load("/path/to/table")
```

**Method 3: Using @ Syntax**

```sql
-- Version syntax
SELECT * FROM my_table@v5;

-- Timestamp syntax (in Databricks)
SELECT * FROM my_table@20240115103000;
```

### Viewing Table History

The DESCRIBE HISTORY command provides a complete audit trail of all operations performed on the table:

```sql
DESCRIBE HISTORY my_table;
```

This returns information including:
- Version number
- Timestamp of the operation
- User who performed the operation
- Operation type (WRITE, MERGE, DELETE, etc.)
- Operation parameters
- Metrics (rows affected, files added/removed)

```sql
-- View only the last 10 operations
DESCRIBE HISTORY my_table LIMIT 10;

-- Using PySpark
from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/path/to/table")
history_df = deltaTable.history()
history_df.show()
```

### Restoring Previous Versions

If you accidentally delete or corrupt data, you can restore the table to a previous state:

```sql
-- Restore to a specific version
RESTORE TABLE my_table TO VERSION AS OF 10;

-- Restore to a specific timestamp
RESTORE TABLE my_table TO TIMESTAMP AS OF '2024-01-15 10:30:00';
```

The RESTORE operation creates a new version that matches the state of the specified historical version. It doesn't actually "go back in time" - it creates new commit entries that effectively undo the changes made after the target version.

```python
# PySpark restore
deltaTable = DeltaTable.forPath(spark, "/path/to/table")
deltaTable.restoreToVersion(10)

# Or restore to timestamp
deltaTable.restoreToTimestamp("2024-01-15 10:30:00")
```

### Time Travel Retention Configuration

By default, Delta Lake keeps version history for 30 days. You can configure this:

```sql
-- Set retention period to 7 days
ALTER TABLE my_table SET TBLPROPERTIES ('delta.logRetentionDuration' = '7 days');

-- Set minimum file retention (affects VACUUM)
ALTER TABLE my_table SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '7 days');
```

| Property | Default | Description |
|----------|---------|-------------|
| delta.logRetentionDuration | 30 days | How long to keep transaction log entries |
| delta.deletedFileRetentionDuration | 7 days | How long to keep data files after deletion |

### Use Cases for Time Travel

**Auditing and Compliance**: Track who changed what and when. Essential for regulated industries where you need to prove data lineage.

**Data Recovery**: Restore accidentally deleted or corrupted data without needing separate backups.

**Reproducibility**: Data scientists can query the exact data used for a machine learning model training run months ago.

**Debugging**: Compare current data with historical versions to understand when and why data issues were introduced.

**Rollback Failed Pipelines**: If a data pipeline produces incorrect results, quickly restore the table to its pre-pipeline state.

---

## 3. MERGE Operations (Upserts)

MERGE is a powerful DML operation that combines INSERT, UPDATE, and DELETE in a single atomic transaction. It's commonly called "upsert" because it can update existing records or insert new ones based on a matching condition.

### Understanding MERGE Semantics

MERGE works by comparing a source dataset against a target Delta table using a join condition. For each row in the source:
- If it matches a row in the target, you can UPDATE or DELETE the target row
- If it doesn't match, you can INSERT the new row

The operation is atomic - either all changes succeed or none do.

### Basic MERGE Syntax

```sql
MERGE INTO target_table AS target
USING source_table AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET 
        target.name = source.name,
        target.value = source.value,
        target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (id, name, value, created_at)
    VALUES (source.id, source.name, source.value, current_timestamp());
```

### MERGE Clause Types

**WHEN MATCHED**: Executes when the join condition finds matching rows between source and target. You can specify UPDATE or DELETE actions.

```sql
-- Update matching rows
WHEN MATCHED THEN
    UPDATE SET target.col1 = source.col1

-- Delete matching rows
WHEN MATCHED THEN
    DELETE

-- Conditional update with additional filter
WHEN MATCHED AND source.is_deleted = true THEN
    DELETE
WHEN MATCHED AND source.is_deleted = false THEN
    UPDATE SET target.col1 = source.col1
```

**WHEN NOT MATCHED (BY TARGET)**: Executes when source rows don't have matching target rows. Use INSERT to add new rows.

```sql
-- Insert new rows
WHEN NOT MATCHED THEN
    INSERT (id, name, value)
    VALUES (source.id, source.name, source.value)

-- Conditional insert
WHEN NOT MATCHED AND source.is_valid = true THEN
    INSERT *
```

**WHEN NOT MATCHED BY SOURCE**: Executes when target rows don't have matching source rows. Useful for deleting or updating orphaned records.

```sql
-- Delete target rows not in source (full sync pattern)
WHEN NOT MATCHED BY SOURCE THEN
    DELETE

-- Soft delete orphaned records
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET target.is_active = false
```

### Complete MERGE Example

```sql
MERGE INTO customers AS target
USING daily_updates AS source
ON target.customer_id = source.customer_id

-- Update existing customers
WHEN MATCHED AND source.operation = 'UPDATE' THEN
    UPDATE SET 
        target.name = source.name,
        target.email = source.email,
        target.phone = source.phone,
        target.modified_date = current_timestamp()

-- Delete customers marked for deletion
WHEN MATCHED AND source.operation = 'DELETE' THEN
    DELETE

-- Insert new customers
WHEN NOT MATCHED AND source.operation IN ('INSERT', 'UPDATE') THEN
    INSERT (customer_id, name, email, phone, created_date, modified_date)
    VALUES (source.customer_id, source.name, source.email, source.phone, 
            current_timestamp(), current_timestamp())

-- Deactivate customers not in the source (full sync)
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET target.is_active = false;
```

### PySpark MERGE Implementation

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit

# Load the target Delta table
deltaTable = DeltaTable.forPath(spark, "/path/to/customers")

# Prepare source DataFrame
updates_df = spark.read.parquet("/path/to/updates")

# Perform MERGE
deltaTable.alias("target").merge(
    updates_df.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdate(
    condition="source.operation = 'UPDATE'",
    set={
        "name": "source.name",
        "email": "source.email",
        "modified_date": current_timestamp()
    }
).whenMatchedDelete(
    condition="source.operation = 'DELETE'"
).whenNotMatchedInsert(
    condition="source.operation IN ('INSERT', 'UPDATE')",
    values={
        "customer_id": "source.customer_id",
        "name": "source.name",
        "email": "source.email",
        "created_date": current_timestamp(),
        "modified_date": current_timestamp()
    }
).execute()
```

### Slowly Changing Dimension (SCD) Type 2 Implementation

SCD Type 2 maintains historical records by creating new rows instead of updating existing ones. This is common in data warehousing.

```sql
-- SCD Type 2 MERGE pattern
MERGE INTO dim_customer AS target
USING (
    -- Prepare source with row hash for change detection
    SELECT 
        *,
        md5(concat(name, email, phone)) AS row_hash
    FROM staging_customer
) AS source
ON target.customer_id = source.customer_id 
   AND target.is_current = true

-- Close the existing record if changed
WHEN MATCHED AND target.row_hash != source.row_hash THEN
    UPDATE SET 
        target.is_current = false,
        target.end_date = current_date()

-- Insert new record (both new customers and changed existing customers)
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, phone, row_hash, 
            is_current, start_date, end_date)
    VALUES (source.customer_id, source.name, source.email, source.phone,
            source.row_hash, true, current_date(), null);

-- After MERGE, insert new versions of changed records
INSERT INTO dim_customer
SELECT 
    s.customer_id, s.name, s.email, s.phone, s.row_hash,
    true AS is_current, current_date() AS start_date, null AS end_date
FROM staging_customer s
JOIN dim_customer t ON s.customer_id = t.customer_id
WHERE t.is_current = false 
  AND t.end_date = current_date()
  AND md5(concat(s.name, s.email, s.phone)) != t.row_hash;
```

### MERGE Performance Considerations

**Join Optimization**: The join condition should use indexed or partitioned columns. Poor join conditions can cause full table scans.

**File Pruning**: Delta Lake uses data skipping to read only relevant files. Ensure your merge condition aligns with partition columns.

**Small File Problem**: Frequent MERGEs can create many small files. Run OPTIMIZE periodically to compact them.

**Broadcast Joins**: If your source DataFrame is small (< 10MB), Spark will broadcast it for better performance.

| Scenario | Recommendation |
|----------|----------------|
| Source << Target | Enable broadcast join for source |
| Source ~= Target | Consider partitioning both by merge key |
| High cardinality merge key | Add partition/zorder columns to merge condition |
| Frequent small merges | Batch updates or run OPTIMIZE regularly |

---

## 4. OPTIMIZE & ZORDER

As data is written to Delta tables through various operations, file sizes and data distribution can become suboptimal. OPTIMIZE and ZORDER are maintenance operations that improve query performance by reorganizing data files.

### Understanding the Small File Problem

Every write operation in Delta Lake creates new Parquet files. With streaming workloads or frequent updates, you might end up with thousands of tiny files. This causes problems:

**Read Overhead**: Each file requires metadata operations. Reading 1000 files of 1MB is slower than reading 10 files of 100MB.

**Memory Pressure**: Spark needs to track all files in memory during planning.

**Cloud API Costs**: Object stores charge per API call. More files mean more LIST and GET operations.

### OPTIMIZE Operation

OPTIMIZE compacts small files into larger ones, targeting a default file size of around 1GB. It reads all the small files and rewrites them as fewer, larger files.

```sql
-- Basic OPTIMIZE
OPTIMIZE my_table;

-- OPTIMIZE specific partitions
OPTIMIZE my_table WHERE date >= '2024-01-01';

-- Using PySpark
from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/path/to/table")
deltaTable.optimize().executeCompaction()
```

**How OPTIMIZE Works**:
1. Identifies files smaller than the target size
2. Groups files that can be combined (respecting partitions)
3. Reads and rewrites data into optimally-sized files
4. Atomically updates the transaction log
5. Original small files are marked for deletion (removed by VACUUM)

### ZORDER Optimization

ZORDER (Z-Ordering) is a technique that co-locates related data in the same files. It's particularly effective for columns frequently used in filter conditions.

```sql
-- OPTIMIZE with ZORDER on specific columns
OPTIMIZE my_table ZORDER BY (customer_id);

-- ZORDER by multiple columns
OPTIMIZE my_table ZORDER BY (region, customer_id);

-- Combined with partition filter
OPTIMIZE my_table 
WHERE date >= '2024-01-01' 
ZORDER BY (customer_id, product_id);
```

**How ZORDER Works**:

ZORDER uses a space-filling curve (specifically, a Z-order curve) to map multi-dimensional data to one dimension while preserving locality. Data points that are close in the multi-dimensional space remain close in the one-dimensional ordering.

The mathematical concept involves interleaving the binary representations of column values:

For a 2D example with values $(x, y)$ where:
- $x = 5$ (binary: $101$)
- $y = 3$ (binary: $011$)

The Z-order value is computed by interleaving bits:
$$Z = x_2 y_2 x_1 y_1 x_0 y_0 = 1\ 0\ 0\ 1\ 1\ 1 = 100111_2 = 39$$

This interleaving ensures that records with similar values across all ZORDER columns end up in the same files.

```python
# PySpark ZORDER
deltaTable = DeltaTable.forPath(spark, "/path/to/table")
deltaTable.optimize().executeZOrderBy("customer_id", "product_id")

# With partition filter
deltaTable.optimize().where("date >= '2024-01-01'").executeZOrderBy("customer_id")
```

### Data Skipping and File Statistics

Delta Lake maintains statistics for each data file:
- Minimum and maximum values for each column
- Null count
- Total row count

When you query with filters, Delta Lake uses these statistics to skip files that definitely don't contain matching data.

```sql
-- This query benefits from ZORDER BY (customer_id)
SELECT * FROM orders WHERE customer_id = 12345;
```

Without ZORDER, customer 12345's orders might be scattered across hundreds of files. With ZORDER, they're concentrated in a few files, allowing Delta Lake to skip most files during query execution.

### Choosing ZORDER Columns

Follow these guidelines for selecting ZORDER columns:

**Good Candidates**:
- High-cardinality columns used in filters (customer_id, order_id)
- Columns frequently used in JOIN conditions
- Columns used in WHERE clauses with equality or range predicates

**Poor Candidates**:
- Low-cardinality columns (gender, status) - use partitioning instead
- Columns rarely used in queries
- Columns already used for partitioning

**Column Order Matters**:
The first column in ZORDER has the strongest locality. Order columns by query importance:

```sql
-- If most queries filter by customer_id, then product_id:
OPTIMIZE sales ZORDER BY (customer_id, product_id);

-- NOT: ZORDER BY (product_id, customer_id)
```

| Column Characteristic | Use Partitioning | Use ZORDER |
|----------------------|------------------|------------|
| Low cardinality (< 1000) | Yes | No |
| High cardinality (> 1000) | No | Yes |
| Used in almost every query | Yes | Less important |
| Used in some queries | No | Yes |
| Equality predicates only | Yes (if low card) | Yes (if high card) |
| Range predicates | No | Yes |

### Auto Optimize Features

Delta Lake offers automatic optimization features:

**Auto Compaction**: Automatically runs OPTIMIZE after writes

```sql
-- Enable at table level
ALTER TABLE my_table SET TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Enable for all tables in session
SET spark.databricks.delta.autoCompact.enabled = true;
```

**Optimized Writes**: Automatically coalesces small files during writes

```sql
ALTER TABLE my_table SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);
```

| Feature | Purpose | When to Use |
|---------|---------|-------------|
| autoCompact | Post-write compaction | Streaming workloads |
| optimizeWrite | Pre-write coalescing | Frequent small writes |
| Manual OPTIMIZE | Full table optimization | Batch maintenance windows |

---

## 5. VACUUM for Cleanup

VACUUM removes data files that are no longer referenced by the Delta table's transaction log. These "stale" files accumulate from UPDATE, DELETE, MERGE, and OPTIMIZE operations.

### Why Files Become Stale

Delta Lake is a copy-on-write system. When you update a record:
1. A new file is written containing the updated data
2. The old file is marked as "removed" in the transaction log
3. The old file still exists on disk (for time travel)

Over time, these orphaned files consume storage and increase costs.

### VACUUM Operation

```sql
-- Remove files older than 7 days (default retention)
VACUUM my_table;

-- Remove files older than a specific retention period
VACUUM my_table RETAIN 168 HOURS;  -- 7 days

-- Dry run to see what would be deleted
VACUUM my_table DRY RUN;
```

**How VACUUM Works**:
1. Reads the transaction log to identify referenced files
2. Lists all files in the table directory
3. Identifies files not in the current table state
4. Filters to only files older than the retention threshold
5. Deletes the unreferenced, old files

```python
# PySpark VACUUM
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/path/to/table")

# Default retention (7 days)
deltaTable.vacuum()

# Custom retention (in hours)
deltaTable.vacuum(168)

# Dry run
deltaTable.vacuum(168, dry_run=True)
```

### Retention Period Configuration

The retention period protects against:
- Deleting files needed by running queries
- Losing time travel capability
- Race conditions with concurrent operations

```sql
-- Table property for deleted file retention
ALTER TABLE my_table SET TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = '7 days'
);

-- Warning: Setting retention below 7 days requires override
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM my_table RETAIN 0 HOURS;  -- Dangerous! Immediate deletion
```

**Recommended Retention Periods**:

| Scenario | Retention | Reasoning |
|----------|-----------|-----------|
| Production tables | 7-30 days | Balance storage cost and recovery capability |
| Development tables | 1-7 days | Lower storage cost, less recovery needed |
| Audit-required tables | 30-365 days | Compliance requirements |
| High-frequency updates | 7 days minimum | Protect long-running queries |

### VACUUM Safety Mechanisms

**Default 7-Day Minimum**: Delta Lake prevents VACUUM with retention less than 7 days by default. This protects long-running queries and streaming jobs.

**Dry Run**: Always use DRY RUN first to understand what will be deleted:

```sql
VACUUM my_table RETAIN 168 HOURS DRY RUN;
-- Output: Found 1543 files (15.2 GB) to delete
```

**Concurrent Operations**: VACUUM acquires a lock to prevent conflicts with concurrent writes. Running VACUUM doesn't block readers.

### Impact on Time Travel

VACUUM directly affects time travel capabilities. Once files are deleted:
- Queries for versions using those files will fail
- RESTORE to those versions becomes impossible
- Historical data is permanently lost

```sql
-- Before VACUUM: Can query version 5
SELECT * FROM my_table VERSION AS OF 5;  -- Works

-- After VACUUM (if version 5 files were removed)
SELECT * FROM my_table VERSION AS OF 5;  -- Error: Files not found
```

**Best Practice**: Align VACUUM retention with your time travel requirements:

```sql
-- If you need 30 days of time travel
ALTER TABLE my_table SET TBLPROPERTIES (
    'delta.logRetentionDuration' = '30 days',
    'delta.deletedFileRetentionDuration' = '30 days'
);

-- Then VACUUM with matching retention
VACUUM my_table RETAIN 720 HOURS;  -- 30 days
```

### Automating VACUUM

For production systems, automate VACUUM as part of maintenance:

```python
from delta.tables import DeltaTable
from datetime import datetime

def vacuum_table(table_path, retention_hours=168):
    """
    Safely vacuum a Delta table with logging
    """
    delta_table = DeltaTable.forPath(spark, table_path)
    
    # Dry run first
    print(f"[{datetime.now()}] Starting dry run for {table_path}")
    delta_table.vacuum(retention_hours, dry_run=True)
    
    # Actual vacuum
    print(f"[{datetime.now()}] Executing VACUUM with {retention_hours}h retention")
    delta_table.vacuum(retention_hours)
    
    print(f"[{datetime.now()}] VACUUM complete for {table_path}")

# Schedule this to run daily during low-traffic periods
vacuum_table("/path/to/my_table", retention_hours=168)
```

### VACUUM vs OPTIMIZE Comparison

| Aspect | OPTIMIZE | VACUUM |
|--------|----------|--------|
| Purpose | Improve query performance | Reduce storage usage |
| Creates new files | Yes | No |
| Deletes files | No (marks as stale) | Yes |
| Affects time travel | No | Yes |
| Storage impact | Temporary increase | Decrease |
| When to run | After many writes | After OPTIMIZE |
| Frequency | Daily or weekly | Weekly or monthly |

---

## 6. Practical Tasks & Implementations

### Task 1: Implement Incremental MERGE

This implementation handles Change Data Capture (CDC) data with insert, update, and delete operations.

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Define schema for our data
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("operation", StringType(), True),  # INSERT, UPDATE, DELETE
    StructField("timestamp", TimestampType(), True)
])

# Create initial Delta table
initial_data = [
    (1, "Alice", "alice@email.com", 100, "INSERT", "2024-01-01 10:00:00"),
    (2, "Bob", "bob@email.com", 200, "INSERT", "2024-01-01 10:00:00"),
    (3, "Charlie", "charlie@email.com", 300, "INSERT", "2024-01-01 10:00:00")
]

initial_df = spark.createDataFrame(initial_data, schema)
initial_df.write.format("delta").mode("overwrite").save("/delta/customers")

# Incremental CDC data (changes to apply)
cdc_data = [
    (1, "Alice Smith", "alice.smith@email.com", 150, "UPDATE", "2024-01-02 09:00:00"),
    (2, None, None, None, "DELETE", "2024-01-02 09:30:00"),
    (4, "Diana", "diana@email.com", 400, "INSERT", "2024-01-02 10:00:00"),
    (5, "Eve", "eve@email.com", 500, "INSERT", "2024-01-02 10:30:00")
]

cdc_df = spark.createDataFrame(cdc_data, schema)

# Load target table
target_table = DeltaTable.forPath(spark, "/delta/customers")

# Perform incremental MERGE
target_table.alias("target").merge(
    cdc_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(
    condition="source.operation = 'UPDATE'",
    set={
        "name": "source.name",
        "email": "source.email",
        "amount": "source.amount",
        "timestamp": "source.timestamp"
    }
).whenMatchedDelete(
    condition="source.operation = 'DELETE'"
).whenNotMatchedInsert(
    condition="source.operation = 'INSERT'",
    values={
        "id": "source.id",
        "name": "source.name",
        "email": "source.email",
        "amount": "source.amount",
        "operation": lit("INSERT"),
        "timestamp": "source.timestamp"
    }
).execute()

# Verify results
print("=== After Incremental MERGE ===")
spark.read.format("delta").load("/delta/customers").orderBy("id").show()
```

**Expected Output**:
```
+---+-----------+---------------------+------+---------+-------------------+
| id|       name|                email|amount|operation|          timestamp|
+---+-----------+---------------------+------+---------+-------------------+
|  1|Alice Smith|alice.smith@email.com|   150|   INSERT|2024-01-02 09:00:00|
|  3|    Charlie|    charlie@email.com|   300|   INSERT|2024-01-01 10:00:00|
|  4|      Diana|       diana@email.com|   400|   INSERT|2024-01-02 10:00:00|
|  5|        Eve|         eve@email.com|   500|   INSERT|2024-01-02 10:30:00|
+---+-----------+---------------------+------+---------+-------------------+
```

### Task 2: Query Historical Versions

```python
from delta.tables import DeltaTable

# Create a table and make several modifications
data_v0 = [(1, "Original A"), (2, "Original B")]
spark.createDataFrame(data_v0, ["id", "value"]).write.format("delta").mode("overwrite").save("/delta/history_demo")

# Version 1: Update
spark.sql("UPDATE delta.`/delta/history_demo` SET value = 'Updated A' WHERE id = 1")

# Version 2: Insert
spark.sql("INSERT INTO delta.`/delta/history_demo` VALUES (3, 'New C')")

# Version 3: Delete
spark.sql("DELETE FROM delta.`/delta/history_demo` WHERE id = 2")

# View complete history
print("=== Table History ===")
delta_table = DeltaTable.forPath(spark, "/delta/history_demo")
delta_table.history().select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

# Query each version
print("\n=== Version 0 (Initial) ===")
spark.read.format("delta").option("versionAsOf", 0).load("/delta/history_demo").show()

print("=== Version 1 (After Update) ===")
spark.read.format("delta").option("versionAsOf", 1).load("/delta/history_demo").show()

print("=== Version 2 (After Insert) ===")
spark.read.format("delta").option("versionAsOf", 2).load("/delta/history_demo").show()

print("=== Version 3 (After Delete - Current) ===")
spark.read.format("delta").option("versionAsOf", 3).load("/delta/history_demo").show()

# Compare two versions to find changes
print("=== Changes between Version 1 and Version 3 ===")
v1_df = spark.read.format("delta").option("versionAsOf", 1).load("/delta/history_demo")
v3_df = spark.read.format("delta").option("versionAsOf", 3).load("/delta/history_demo")

# Records in v1 but not in v3 (deleted)
print("Deleted records:")
v1_df.subtract(v3_df).show()

# Records in v3 but not in v1 (added)
print("Added records:")
v3_df.subtract(v1_df).show()

# Restore to Version 1 if needed
print("=== Restoring to Version 1 ===")
delta_table.restoreToVersion(1)
spark.read.format("delta").load("/delta/history_demo").show()
```

### Task 3: Optimize Tables

```python
from delta.tables import DeltaTable
import time

# Create a table with many small files (simulating streaming writes)
print("Creating table with small files...")
for i in range(20):
    data = [(j, f"value_{i}_{j}", i * 100 + j) for j in range(100)]
    df = spark.createDataFrame(data, ["id", "name", "score"])
    df.coalesce(1).write.format("delta").mode("append").save("/delta/optimize_demo")

# Check file count before optimization
print("\n=== Before OPTIMIZE ===")
files_before = spark.sql("DESCRIBE DETAIL delta.`/delta/optimize_demo`").select("numFiles").collect()[0][0]
print(f"Number of files: {files_before}")

# Run OPTIMIZE
print("\n=== Running OPTIMIZE ===")
delta_table = DeltaTable.forPath(spark, "/delta/optimize_demo")
delta_table.optimize().executeCompaction()

# Check file count after optimization
print("\n=== After OPTIMIZE ===")
files_after = spark.sql("DESCRIBE DETAIL delta.`/delta/optimize_demo`").select("numFiles").collect()[0][0]
print(f"Number of files: {files_after}")
print(f"File reduction: {files_before - files_after} files removed through compaction")

# Create a table for ZORDER demonstration
print("\n=== Creating table for ZORDER demo ===")
import random
data = [(i, f"customer_{i % 1000}", f"product_{i % 500}", random.randint(1, 1000)) 
        for i in range(100000)]
spark.createDataFrame(data, ["id", "customer_id", "product_id", "amount"]) \
    .write.format("delta").mode("overwrite").save("/delta/zorder_demo")

# Run OPTIMIZE with ZORDER
print("Running OPTIMIZE with ZORDER...")
delta_table_zorder = DeltaTable.forPath(spark, "/delta/zorder_demo")
delta_table_zorder.optimize().executeZOrderBy("customer_id", "product_id")

# Verify ZORDER effect by checking file statistics
print("\n=== File Statistics After ZORDER ===")
spark.sql("""
    SELECT 
        file_name,
        num_records,
        min(customer_id) as min_customer,
        max(customer_id) as max_customer
    FROM (
        SELECT 
            input_file_name() as file_name,
            count(*) as num_records,
            min(customer_id) as min_cust,
            max(customer_id) as max_cust,
            customer_id
        FROM delta.`/delta/zorder_demo`
        GROUP BY input_file_name(), customer_id
    )
    GROUP BY file_name, num_records
    LIMIT 5
""").show(truncate=False)

# Demonstrate query performance improvement
print("\n=== Query Performance Comparison ===")

# Query without ZORDER benefit (full scan)
start = time.time()
spark.sql("SELECT * FROM delta.`/delta/zorder_demo` WHERE customer_id = 'customer_500'").count()
no_zorder_time = time.time() - start
print(f"Query on ZORDERed column: {no_zorder_time:.3f} seconds")

# The same query benefits from data skipping due to ZORDER
# Delta Lake can skip files where customer_id = 'customer_500' is not in the min/max range
```

### Task 4: Clean Old Files with VACUUM

```python
from delta.tables import DeltaTable

# Create a table and perform operations that create stale files
print("=== Creating table and performing updates ===")
data = [(i, f"value_{i}") for i in range(1000)]
spark.createDataFrame(data, ["id", "value"]).write.format("delta").mode("overwrite").save("/delta/vacuum_demo")

# Make updates (creates new files, marks old ones as stale)
for i in range(5):
    spark.sql(f"UPDATE delta.`/delta/vacuum_demo` SET value = 'updated_{i}' WHERE id % 5 = {i}")

# Check table details
print("\n=== Table Details Before VACUUM ===")
spark.sql("DESCRIBE DETAIL delta.`/delta/vacuum_demo`").show(truncate=False)

# List all files in the directory (including stale ones)
import subprocess
print("\n=== All Files in Directory ===")
# Using dbutils in Databricks or direct file listing
files_info = spark.sql("DESCRIBE DETAIL delta.`/delta/vacuum_demo`")
files_info.select("location", "numFiles").show(truncate=False)

# Dry run VACUUM to see what would be deleted
print("\n=== VACUUM Dry Run (168 hours retention) ===")
delta_table = DeltaTable.forPath(spark, "/delta/vacuum_demo")

# Note: In a real scenario with files older than 7 days, you'd see files listed
# For demonstration, we'll use a shorter retention (requires disabling safety check)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# Dry run
print("Files that would be deleted (dry run):")
delta_table.vacuum(0, dry_run=True)

# Actual VACUUM (WARNING: This deletes files immediately!)
print("\n=== Executing VACUUM ===")
delta_table.vacuum(0)

# Re-enable safety check
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

# Verify time travel is affected
print("\n=== Time Travel After VACUUM ===")
print("Current version (works):")
spark.read.format("delta").load("/delta/vacuum_demo").show(5)

print("\nAttempting to read Version 0 (may fail if files were vacuumed):")
try:
    spark.read.format("delta").option("versionAsOf", 0).load("/delta/vacuum_demo").show(5)
except Exception as e:
    print(f"Error: {e}")
    print("Files for Version 0 have been removed by VACUUM")
```

### Complete Maintenance Pipeline

```python
from delta.tables import DeltaTable
from datetime import datetime

def run_delta_maintenance(table_path, zorder_columns=None, vacuum_retention_hours=168):
    """
    Complete maintenance pipeline for a Delta table
    
    Parameters:
    - table_path: Path to the Delta table
    - zorder_columns: List of columns to ZORDER by (optional)
    - vacuum_retention_hours: Retention period for VACUUM (default 7 days)
    """
    
    print(f"\n{'='*60}")
    print(f"Starting maintenance for: {table_path}")
    print(f"Time: {datetime.now()}")
    print(f"{'='*60}")
    
    delta_table = DeltaTable.forPath(spark, table_path)
    
    # Step 1: Get current table statistics
    print("\n[1/4] Current Table Statistics")
    print("-" * 40)
    details = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
    print(f"  Files: {details['numFiles']}")
    print(f"  Size: {details['sizeInBytes'] / (1024*1024):.2f} MB")
    print(f"  Partitions: {details['partitionColumns']}")
    
    # Step 2: Run OPTIMIZE
    print("\n[2/4] Running OPTIMIZE")
    print("-" * 40)
    if zorder_columns:
        print(f"  ZORDER columns: {zorder_columns}")
        delta_table.optimize().executeZOrderBy(*zorder_columns)
    else:
        delta_table.optimize().executeCompaction()
    
    # Get post-optimize statistics
    details_after_opt = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
    print(f"  Files after OPTIMIZE: {details_after_opt['numFiles']}")
    print(f"  Files compacted: {details['numFiles'] - details_after_opt['numFiles']}")
    
    # Step 3: VACUUM dry run
    print("\n[3/4] VACUUM Dry Run")
    print("-" * 40)
    print(f"  Retention: {vacuum_retention_hours} hours")
    # In production, capture dry run output
    delta_table.vacuum(vacuum_retention_hours)
    
    # Step 4: Final statistics
    print("\n[4/4] Final Table Statistics")
    print("-" * 40)
    final_details = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
    print(f"  Files: {final_details['numFiles']}")
    print(f"  Size: {final_details['sizeInBytes'] / (1024*1024):.2f} MB")
    
    # Get history summary
    history = delta_table.history(10)
    print(f"\n  Recent operations:")
    history.select("version", "operation", "timestamp").show(5, truncate=False)
    
    print(f"\n{'='*60}")
    print(f"Maintenance complete for: {table_path}")
    print(f"{'='*60}\n")

# Usage example
run_delta_maintenance(
    table_path="/delta/customers",
    zorder_columns=["customer_id", "region"],
    vacuum_retention_hours=168
)
```

---

## 7. Best Practices & Performance Tips

### Table Design Best Practices

**Partitioning Strategy**:
- Partition by low-cardinality columns frequently used in filters (date, region, status)
- Aim for partitions with 1GB+ of data each
- Avoid over-partitioning (more than 10,000 partitions)
- Don't partition by high-cardinality columns (use ZORDER instead)

**Column Ordering**:
- Place frequently filtered columns at the beginning of the schema
- Group related columns together for better compression
- Consider column pruning patterns in your queries

**File Size Targets**:

| Workload Type | Target File Size | Rationale |
|--------------|------------------|-----------|
| Analytical queries | 256MB - 1GB | Balance parallelism and overhead |
| Streaming ingestion | 64MB - 256MB | Faster writes, more frequent optimize |
| ML training | 128MB - 512MB | Efficient batch processing |

### MERGE Optimization Tips

**Pre-filter Source Data**: Reduce the source dataset before MERGE to minimize comparison overhead.

```python
# Instead of merging entire CDC stream
updates_df = spark.read.parquet("/cdc/all_updates")

# Filter to only relevant records
filtered_updates = updates_df.filter(col("timestamp") > last_merge_timestamp)

# Then MERGE
delta_table.merge(filtered_updates.alias("source"), ...)
```

**Use Partition Pruning**: Include partition columns in the merge condition.

```sql
-- Efficient: Partition pruning works
MERGE INTO sales AS target
USING updates AS source
ON target.date = source.date AND target.id = source.id
...

-- Inefficient: Full table scan
MERGE INTO sales AS target
USING updates AS source
ON target.id = source.id
...
```

**Batch Small Merges**: Instead of many small MERGEs, accumulate changes and merge periodically.

### OPTIMIZE Schedule Recommendations

| Table Update Pattern | OPTIMIZE Frequency | ZORDER Strategy |
|---------------------|-------------------|-----------------|
| Streaming (continuous) | Every 1-4 hours | Primary query column |
| Micro-batch (hourly) | Daily | Top 2-3 filter columns |
| Batch (daily) | After each load | Query-based selection |
| Infrequent updates | Weekly | May not be needed |

### VACUUM Guidelines

**Safety First**:
- Never VACUUM below 7 days without understanding the implications
- Always run DRY RUN first in production
- Coordinate VACUUM with time travel requirements

**Scheduling**:
- Run VACUUM during low-traffic periods
- Schedule after OPTIMIZE (to clean up compacted files)
- Consider storage costs vs. time travel needs

**Monitoring**:
```sql
-- Monitor storage growth
SELECT 
    date_trunc('day', timestamp) as day,
    sum(operationMetrics.numRemovedFiles) as files_marked_removed,
    sum(operationMetrics.numAddedFiles) as files_added
FROM (DESCRIBE HISTORY my_table)
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30;
```

### Performance Monitoring Queries

```sql
-- Table health check
SELECT 
    name,
    numFiles,
    sizeInBytes / (1024*1024*1024) as size_gb,
    numFiles / (sizeInBytes / (1024*1024*128)) as avg_file_mb
FROM (DESCRIBE DETAIL my_table);

-- Operation patterns
SELECT 
    operation,
    count(*) as count,
    avg(operationMetrics.numOutputRows) as avg_rows,
    avg(operationMetrics.numAddedFiles) as avg_files_added
FROM (DESCRIBE HISTORY my_table)
WHERE timestamp > current_timestamp() - INTERVAL 7 DAYS
GROUP BY operation
ORDER BY count DESC;
```

### Common Pitfalls to Avoid

| Pitfall | Problem | Solution |
|---------|---------|----------|
| Over-partitioning | Too many small files | Use fewer partitions, rely on ZORDER |
| ZORDER on partition columns | Redundant, no benefit | ZORDER on non-partition filter columns |
| VACUUM too aggressively | Lose time travel capability | Match retention to business needs |
| No OPTIMIZE schedule | Small file problem | Automate with Auto Optimize or jobs |
| MERGE without filters | Full table rewrite | Pre-filter source, use partition pruning |
| Ignoring file statistics | Poor data skipping | ZORDER on frequently filtered columns |

---

## Quick Reference Card

### Essential Commands

```sql
-- Time Travel
SELECT * FROM table VERSION AS OF 5;
SELECT * FROM table TIMESTAMP AS OF '2024-01-15';
DESCRIBE HISTORY table;
RESTORE TABLE table TO VERSION AS OF 5;

-- MERGE
MERGE INTO target USING source ON condition
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...

-- OPTIMIZE
OPTIMIZE table;
OPTIMIZE table ZORDER BY (col1, col2);
OPTIMIZE table WHERE partition_col = 'value';

-- VACUUM
VACUUM table;
VACUUM table RETAIN 168 HOURS;
VACUUM table DRY RUN;
```

### Key Table Properties

```sql
-- Time travel retention
ALTER TABLE t SET TBLPROPERTIES ('delta.logRetentionDuration' = '30 days');
ALTER TABLE t SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '7 days');

-- Auto optimization
ALTER TABLE t SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
ALTER TABLE t SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true');
```

---

*These notes provide comprehensive coverage of Delta Lake advanced features. Practice these concepts with real datasets to build proficiency in managing production Delta Lake tables.*