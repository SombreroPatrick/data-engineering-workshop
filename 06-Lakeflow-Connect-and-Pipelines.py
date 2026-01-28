# Databricks notebook source
# MAGIC %md
# MAGIC # 🌊 Lakeflow: Connect and Spark Declarative Pipelines
# MAGIC
# MAGIC **Level**: Intermediate to Advanced
# MAGIC **Duration**: 60 minutes
# MAGIC **Prerequisites**: Complete notebooks 00-05
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you will:
# MAGIC - ✅ Understand **Lakeflow** unified data engineering platform
# MAGIC - ✅ Learn **Lakeflow Connect** for no-code data ingestion (GA April 2025)
# MAGIC - ✅ Master **Spark Declarative Pipelines** with new `@dp` decorator syntax
# MAGIC - ✅ Implement **medallion architecture** (Bronze → Silver → Gold)
# MAGIC - ✅ Apply **data quality expectations** with built-in validation
# MAGIC - ✅ Use **CDC patterns** for incremental updates
# MAGIC - ✅ Understand **streaming tables vs materialized views**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Documentation Links
# MAGIC
# MAGIC | Resource | Link |
# MAGIC |----------|------|
# MAGIC | 🌊 Lakeflow Overview | [Intro Blog](https://www.databricks.com/blog/introducing-databricks-lakeflow) |
# MAGIC | 🔌 Lakeflow Connect GA | [GA Announcement](https://www.databricks.com/blog/announcing-general-availability-lakeflow-connect) |
# MAGIC | 📊 Salesforce Connector | [Salesforce Blog](https://databricks.com/blog/introducing-salesforce-connectors-lakehouse-federation-and-lakeflow-connect) |
# MAGIC | ⚡ Spark Declarative Pipelines | [Official Docs](https://docs.databricks.com/en/ldp/) |
# MAGIC | 🐍 Python Reference | [Python API](https://docs.databricks.com/en/ldp/developer/python-ref.html) |
# MAGIC | ✅ Data Quality | [Expectations Guide](https://docs.databricks.com/en/ldp/expectations.html) |
# MAGIC | 🔄 CDC Flows | [CDC Reference](https://docs.databricks.com/en/ldp/developer/ldp-python-ref-apply-changes.html) |

# COMMAND ----------

# DBTITLE 1,Setup: Load and Prepare Data for This Tutorial
from pyspark.sql.functions import explode, col, from_unixtime, sum as _sum

sales_raw = spark.read.json("/databricks-datasets/retail-org/sales_orders/")
customers = (
    spark.read.format("csv")
    .option("header", "true")
    .load("/databricks-datasets/retail-org/customers/")
)

sales_with_customers = (
    sales_raw.join(customers, on=["customer_id", "customer_name"], how="left")
    .withColumn(
        "order_datetime_ts",
        from_unixtime(col("order_datetime").cast("long")).cast("timestamp"),
    )
    .withColumn(
        "order_date", from_unixtime(col("order_datetime").cast("long")).cast("date")
    )
)

orders_exploded = sales_with_customers.select(
    "order_number",
    "customer_id",
    "customer_name",
    "order_datetime_ts",
    "order_date",
    "state",
    "city",
    "loyalty_segment",
    explode("ordered_products").alias("product"),
).select(
    "*",
    col("product.name").alias("product_name"),
    col("product.price").alias("price"),
    col("product.qty").alias("quantity"),
    (col("product.price") * col("product.qty")).alias("line_total"),
)

print("✅ Data loaded and prepared")
print(f"📊 Total orders: {sales_with_customers.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌊 Part 1: Understanding Lakeflow
# MAGIC
# MAGIC ### What is Lakeflow?
# MAGIC
# MAGIC **Lakeflow** is Databricks' unified data engineering solution combining three components:
# MAGIC
# MAGIC | Component | Purpose | Status |
# MAGIC |-----------|---------|--------|
# MAGIC | **Lakeflow Connect** | No-code data ingestion from SaaS apps and databases | GA April 2025 |
# MAGIC | **Spark Declarative Pipelines** | Declarative ETL framework (formerly Delta Live Tables) | GA |
# MAGIC | **Lakeflow Jobs** | Orchestration and monitoring | GA |
# MAGIC
# MAGIC ### Traditional ETL vs Lakeflow
# MAGIC
# MAGIC | Traditional ETL | Lakeflow Approach |
# MAGIC |----------------|-------------------|
# MAGIC | ❌ Write custom connectors | ✅ Pre-built, managed connectors |
# MAGIC | ❌ Imperative code (step-by-step) | ✅ Declarative code (define what you want) |
# MAGIC | ❌ Manual dependency management | ✅ Automatic DAG generation |
# MAGIC | ❌ Custom data quality checks | ✅ Built-in expectations framework |
# MAGIC | ❌ Complex orchestration | ✅ Integrated job scheduling |
# MAGIC | ❌ Manual infrastructure | ✅ Serverless by default |
# MAGIC
# MAGIC 💡 **Philosophy**: "Define WHAT you want, not HOW to build it"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔌 Part 2: Lakeflow Connect (GA April 2025)
# MAGIC
# MAGIC ### What is Lakeflow Connect?
# MAGIC
# MAGIC **Lakeflow Connect** is a fully managed, serverless data ingestion service that connects enterprise applications to your lakehouse with **zero code**.
# MAGIC
# MAGIC ### Key Features
# MAGIC
# MAGIC - ✨ **Serverless**: No infrastructure to manage, automatic scaling
# MAGIC - 🔄 **Built-in CDC**: Capture inserts, updates, deletes automatically
# MAGIC - 🔒 **Unity Catalog Native**: Automatic schema registration and governance
# MAGIC - 🎯 **Pre-built Connectors**: Enterprise-grade reliability
# MAGIC
# MAGIC ### Available Connectors (2025)
# MAGIC
# MAGIC | Connector | Status | Use Cases |
# MAGIC |-----------|--------|-----------|
# MAGIC | **Salesforce Platform** | ✅ GA | CRM data, customer records, sales pipeline |
# MAGIC | **Workday Reports** | ✅ GA | HR data, employee records, payroll |
# MAGIC | **SQL Server** | 🔬 Public Preview | Transactional databases, legacy systems |
# MAGIC | **Google Analytics** | 🚧 Coming Soon | Web analytics, user behavior |
# MAGIC | **ServiceNow** | 🚧 Coming Soon | IT service management |
# MAGIC | **SharePoint** | 🚧 Coming Soon | Document management |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lakeflow Connect Architecture
# MAGIC
# MAGIC ```
# MAGIC Source System (Salesforce, SQL Server, etc.)
# MAGIC          ↓
# MAGIC   Lakeflow Connect (Serverless, managed)
# MAGIC          ↓
# MAGIC   Bronze Layer (Raw Delta Table with CDC metadata)
# MAGIC          ↓
# MAGIC   Spark Declarative Pipelines (Transform)
# MAGIC          ↓
# MAGIC   Silver Layer (Cleaned, validated)
# MAGIC          ↓
# MAGIC   Gold Layer (Business-ready analytics)
# MAGIC ```
# MAGIC
# MAGIC ### Setting Up Lakeflow Connect (UI-Based)
# MAGIC
# MAGIC Since Lakeflow Connect is **no-code**, configuration happens through the Databricks UI:
# MAGIC
# MAGIC **Step 1: Create Connection**
# MAGIC - Navigate to: Workspace → Data Engineering → Lakeflow Connect
# MAGIC - Select connector type (e.g., Salesforce)
# MAGIC - Provide credentials and test connection
# MAGIC
# MAGIC **Step 2: Configure Ingestion**
# MAGIC - Select objects to sync (tables, reports)
# MAGIC - Choose sync mode: Full Refresh or Incremental (CDC)
# MAGIC - Set schedule: continuous, hourly, daily
# MAGIC - Choose destination: Unity Catalog location
# MAGIC
# MAGIC **Step 3: Monitor**
# MAGIC - View sync history and status
# MAGIC - Monitor data volume and latency
# MAGIC - Review Unity Catalog lineage

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDC Metadata from Lakeflow Connect
# MAGIC
# MAGIC When using incremental sync, Lakeflow Connect adds CDC metadata:
# MAGIC
# MAGIC | Column | Description | Example |
# MAGIC |--------|-------------|---------|
# MAGIC | `_change_type` | Type of change | `insert`, `update`, `delete` |
# MAGIC | `_commit_version` | Delta Lake version | `42` |
# MAGIC | `_commit_timestamp` | When change was captured | `2025-01-27 10:30:00` |
# MAGIC
# MAGIC **Example Bronze Table**:
# MAGIC ```python
# MAGIC display(spark.read.table("bronze.salesforce_accounts"))
# MAGIC
# MAGIC # Output:
# MAGIC # | Id  | Name      | Industry   | _change_type | _commit_timestamp   |
# MAGIC # |-----|-----------|------------|--------------|---------------------|
# MAGIC # | 001 | Acme Corp | Tech       | insert       | 2025-01-27 09:00:00 |
# MAGIC # | 001 | Acme Corp | Technology | update       | 2025-01-27 10:00:00 |
# MAGIC # | 002 | Beta Inc  | Finance    | insert       | 2025-01-27 10:30:00 |
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚡ Part 3: Spark Declarative Pipelines
# MAGIC
# MAGIC ### Imperative vs Declarative
# MAGIC
# MAGIC **Imperative (Traditional)**:
# MAGIC ```python
# MAGIC # Tell Spark HOW to do everything
# MAGIC df = spark.read.table("source")
# MAGIC df = df.filter(col("amount") > 0)
# MAGIC df = df.withColumn("processed_at", current_timestamp())
# MAGIC df.write.mode("overwrite").table("target")
# MAGIC
# MAGIC # Problems: Manual dependencies, no incremental processing, custom error handling
# MAGIC ```
# MAGIC
# MAGIC **Declarative (Spark Declarative Pipelines)**:
# MAGIC ```python
# MAGIC # Declare WHAT you want, framework handles HOW
# MAGIC @dp.materialized_view()
# MAGIC def target():
# MAGIC     return (
# MAGIC         spark.read.table("source")
# MAGIC         .filter(col("amount") > 0)
# MAGIC         .withColumn("processed_at", current_timestamp())
# MAGIC     )
# MAGIC
# MAGIC # Benefits: Automatic dependencies, incremental processing, built-in error handling
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### NEW Python Syntax (2024+)
# MAGIC
# MAGIC ⚠️ **IMPORTANT**: The old `@dlt` decorator is **deprecated**. Use new `@dp` syntax:
# MAGIC
# MAGIC | Old (Deprecated) | New (Current) |
# MAGIC |------------------|---------------|
# MAGIC | `import dlt` | `from pyspark import pipelines as dp` |
# MAGIC | `@dlt.table()` | `@dp.table()` |
# MAGIC | `@dlt.view()` | `@dp.materialized_view()` |
# MAGIC | `dlt.read()` | `spark.read.table()` |
# MAGIC | `dlt.read_stream()` | `spark.readStream.table()` |
# MAGIC
# MAGIC **Why?** Better Spark integration, clearer streaming vs batch distinction

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Tables vs Materialized Views
# MAGIC
# MAGIC | Feature | Streaming Table (`@dp.table`) | Materialized View (`@dp.materialized_view`) |
# MAGIC |---------|-------------------------------|---------------------------------------------|
# MAGIC | **Processing** | Continuous, incremental | Batch, full refresh |
# MAGIC | **Read API** | `spark.readStream.table()` | `spark.read.table()` |
# MAGIC | **Use Cases** | Append-only logs, CDC, real-time | Aggregations, snapshots, dimensions |
# MAGIC | **Update** | Continuous (as data arrives) | On schedule or trigger |
# MAGIC | **Cost** | Lower (incremental) | Higher (full scan) |
# MAGIC
# MAGIC **Decision Tree**:
# MAGIC - Data continuously arriving? → Use `@dp.table` (streaming)
# MAGIC - Batch processing/aggregations? → Use `@dp.materialized_view`

# COMMAND ----------

# DBTITLE 1,Example: Streaming Table
# This would be in a Spark Declarative Pipeline file (not executed here)
"""
from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    comment="Raw loan data ingested continuously"
)
def bronze_loans():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load("/databricks-datasets/learning-spark-v2/loans/")
    )
"""

print("✅ Streaming table example (conceptual)")

# COMMAND ----------

# DBTITLE 1,Example: Materialized View
# This would be in a Spark Declarative Pipeline file (not executed here)
"""
from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.materialized_view(
    comment="Daily loan statistics by state"
)
def gold_loan_stats():
    return (
        spark.read
            .table("silver_loans")
            .groupBy("addr_state")
            .agg(
                count("*").alias("total_loans"),
                avg("loan_amnt").alias("avg_loan_amount")
            )
    )
"""

print("✅ Materialized view example (conceptual)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Data Quality with Expectations
# MAGIC
# MAGIC ### What are Expectations?
# MAGIC
# MAGIC **Expectations** are declarative data quality rules that validate data in your pipeline.
# MAGIC
# MAGIC **Syntax**:
# MAGIC ```python
# MAGIC @dp.table(
# MAGIC     expectations={
# MAGIC         "valid_amount": "loan_amnt > 0",
# MAGIC         "valid_state": "addr_state IS NOT NULL"
# MAGIC     }
# MAGIC )
# MAGIC def my_table():
# MAGIC     return spark.readStream.table("source")
# MAGIC ```
# MAGIC
# MAGIC ### Expectation Actions
# MAGIC
# MAGIC | Action | Behavior | When to Use |
# MAGIC |--------|----------|-------------|
# MAGIC | **warn** (default) | Log violation, keep record | Soft validation, monitoring |
# MAGIC | **drop** | Drop violating records | Hard validation, critical quality |
# MAGIC | **fail** | Fail entire pipeline | Zero tolerance for bad data |
# MAGIC
# MAGIC **Syntax with Actions**:
# MAGIC ```python
# MAGIC @dp.table(
# MAGIC     expectations={
# MAGIC         "valid_amount": ("loan_amnt > 0", "drop"),      # Drop bad records
# MAGIC         "valid_term": ("term IN ('36 months', '60 months')", "warn")  # Just warn
# MAGIC     }
# MAGIC )
# MAGIC def silver_loans():
# MAGIC     return spark.readStream.table("bronze_loans")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Example: Expectations in Action
# This would be in a Spark Declarative Pipeline file (not executed here)
"""
from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    comment="Validated loan data with quality checks",
    expectations={
        # Critical validations - drop bad records
        "valid_loan_amount": ("loan_amnt > 0", "drop"),
        "valid_state": ("addr_state IS NOT NULL", "drop"),
        "valid_term": ("term IN ('36 months', '60 months')", "drop"),
        
        # Soft validations - warn but keep
        "reasonable_interest_rate": ("int_rate BETWEEN 0 AND 50", "warn")
    }
)
def silver_loans_validated():
    return (
        spark.readStream
            .table("bronze_loans")
            .withColumn("validation_timestamp", current_timestamp())
    )
"""

print("✅ Expectations example (conceptual)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitoring Expectations
# MAGIC
# MAGIC Spark Declarative Pipelines automatically tracks violations:
# MAGIC
# MAGIC ```sql
# MAGIC -- View expectation metrics
# MAGIC SELECT
# MAGIC   expectation_name,
# MAGIC   passed_records,
# MAGIC   failed_records,
# MAGIC   failed_records * 100.0 / (passed_records + failed_records) as failure_rate
# MAGIC FROM event_log
# MAGIC WHERE event_type = 'expectation'
# MAGIC ORDER BY failure_rate DESC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Change Data Capture (CDC) Patterns
# MAGIC
# MAGIC ### What is CDC?
# MAGIC
# MAGIC **Change Data Capture** tracks inserts, updates, and deletes from source systems.
# MAGIC
# MAGIC ### CDC with Spark Declarative Pipelines
# MAGIC
# MAGIC ```python
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target = "silver_loans",           # Destination table
# MAGIC     source = "bronze_loans_cdc",       # Source with CDC metadata
# MAGIC     keys = ["loan_id"],                # Primary key(s)
# MAGIC     sequence_by = "updated_timestamp", # Order changes
# MAGIC     stored_as_scd_type = 1             # SCD Type 1 or 2
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### SCD Type 1 vs Type 2
# MAGIC
# MAGIC | Feature | SCD Type 1 | SCD Type 2 |
# MAGIC |---------|------------|------------|
# MAGIC | **History** | No history (overwrite) | Full history preserved |
# MAGIC | **Storage** | Lower (one row per key) | Higher (multiple rows) |
# MAGIC | **Use Cases** | Current state only | Audit trails, time-based analysis |
# MAGIC | **Columns** | None added | `_start_at`, `_end_at`, `_is_current` |

# COMMAND ----------

# DBTITLE 1,Example: CDC Flow (SCD Type 1)
# This would be in a Spark Declarative Pipeline file (not executed here)
"""
from pyspark import pipelines as dp

# Assume bronze_loans_cdc has CDC metadata from Lakeflow Connect
dp.create_auto_cdc_flow(
    target = "silver_loans",
    source = "bronze_loans_cdc",
    keys = ["loan_id"],
    sequence_by = "_commit_timestamp",
    stored_as_scd_type = 1  # Keep only current state
)
"""

print("✅ CDC flow example (conceptual)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Medallion Architecture Pattern
# MAGIC
# MAGIC ### What is Medallion Architecture?
# MAGIC
# MAGIC A data design pattern organizing data into three layers:
# MAGIC
# MAGIC ```
# MAGIC Bronze (Raw) → Silver (Cleaned) → Gold (Business-Ready)
# MAGIC ```
# MAGIC
# MAGIC | Layer | Purpose | Characteristics |
# MAGIC |-------|---------|-----------------|
# MAGIC | **Bronze** | Raw ingestion | Exact copy, no transformations, append-only |
# MAGIC | **Silver** | Cleaned, validated | Quality checks, standardized schemas, deduplicated |
# MAGIC | **Gold** | Business aggregates | Aggregated metrics, denormalized, optimized for BI |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Complete Example: Medallion Pipeline
# MAGIC
# MAGIC Let's build a complete pipeline using the Lending Club dataset.

# COMMAND ----------

# DBTITLE 1,Complete Pipeline Definition (Conceptual)
# This would be saved as loan_pipeline.py and deployed as a pipeline

"""
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# ============================================================================
# BRONZE LAYER: Raw Data Ingestion
# ============================================================================

@dp.table(
    comment="Raw loan data ingested from source system"
)
def bronze_loans():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load("/databricks-datasets/learning-spark-v2/loans/")
            .withColumn("ingestion_timestamp", current_timestamp())
    )


# ============================================================================
# SILVER LAYER: Cleaned and Validated Data
# ============================================================================

@dp.table(
    comment="Cleaned loan data with quality checks",
    expectations={
        "valid_loan_amount": ("loan_amnt > 0 AND loan_amnt < 1000000", "drop"),
        "valid_state": ("addr_state IS NOT NULL", "drop"),
        "valid_term": ("term IN ('36 months', '60 months')", "drop"),
        "reasonable_interest_rate": ("int_rate BETWEEN 0 AND 50", "warn")
    }
)
def silver_loans():
    return (
        spark.readStream
            .table("bronze_loans")
            .select(
                col("loan_id"),
                col("loan_amnt").cast("decimal(10,2)").alias("loan_amount"),
                col("funded_amnt").cast("decimal(10,2)").alias("funded_amount"),
                col("term").alias("loan_term"),
                col("int_rate").cast("decimal(5,2)").alias("interest_rate"),
                col("grade").alias("loan_grade"),
                col("addr_state").alias("state"),
                col("dti").cast("decimal(5,2)").alias("debt_to_income_ratio"),
                col("ingestion_timestamp")
            )
            .withColumn("processing_timestamp", current_timestamp())
            .withColumn(
                "risk_category",
                when(col("loan_grade").isin("A", "B"), "Low")
                .when(col("loan_grade").isin("C", "D"), "Medium")
                .otherwise("High")
            )
    )


# ============================================================================
# GOLD LAYER: Business Aggregations
# ============================================================================

@dp.materialized_view(
    comment="Loan statistics by state"
)
def gold_loans_by_state():
    return (
        spark.read
            .table("silver_loans")
            .groupBy("state")
            .agg(
                count("*").alias("total_loans"),
                sum("loan_amount").alias("total_loan_amount"),
                avg("loan_amount").alias("avg_loan_amount"),
                avg("interest_rate").alias("avg_interest_rate")
            )
            .orderBy(desc("total_loan_amount"))
    )


@dp.materialized_view(
    comment="Loan statistics by grade"
)
def gold_loans_by_grade():
    return (
        spark.read
            .table("silver_loans")
            .groupBy("loan_grade", "loan_term")
            .agg(
                count("*").alias("total_loans"),
                avg("loan_amount").alias("avg_loan_amount"),
                avg("interest_rate").alias("avg_interest_rate")
            )
            .orderBy("loan_grade", "loan_term")
    )
"""

print("✅ Complete medallion pipeline defined (conceptual)")
print("\n📝 Deploy using Lakeflow Jobs in Databricks UI")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Deploying Spark Declarative Pipelines
# MAGIC
# MAGIC ### Deployment via Databricks UI
# MAGIC
# MAGIC 1. Navigate to: **Workflows → Lakeflow Jobs**
# MAGIC 2. Click **"Create Pipeline"**
# MAGIC 3. Configure:
# MAGIC    - **Name**: "Loan Processing Pipeline"
# MAGIC    - **Source Code**: Upload `loan_pipeline.py`
# MAGIC    - **Target**: `catalog.schema` (Unity Catalog location)
# MAGIC    - **Compute**: Serverless (recommended)
# MAGIC    - **Trigger**: Continuous or Scheduled
# MAGIC 4. Click **"Create"**
# MAGIC
# MAGIC ### Deployment via CLI
# MAGIC
# MAGIC ```bash
# MAGIC databricks pipelines create \
# MAGIC   --name "Loan Processing Pipeline" \
# MAGIC   --source loan_pipeline.py \
# MAGIC   --target catalog.schema \
# MAGIC   --serverless true \
# MAGIC   --continuous true
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Monitoring and Observability
# MAGIC
# MAGIC ### Built-in Monitoring Features
# MAGIC
# MAGIC **1. Pipeline DAG Visualization**
# MAGIC - Visual data flow representation
# MAGIC - Dependency relationships
# MAGIC - Execution status per table
# MAGIC
# MAGIC **2. Event Log**
# MAGIC ```sql
# MAGIC SELECT timestamp, event_type, message
# MAGIC FROM event_log
# MAGIC WHERE pipeline_id = '<your-pipeline-id>'
# MAGIC ORDER BY timestamp DESC
# MAGIC ```
# MAGIC
# MAGIC **3. Data Quality Metrics**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   dataset,
# MAGIC   expectation,
# MAGIC   passed_records,
# MAGIC   failed_records
# MAGIC FROM event_log
# MAGIC WHERE event_type = 'expectation'
# MAGIC ```
# MAGIC
# MAGIC **4. Unity Catalog Lineage**
# MAGIC - Automatic lineage from source to gold
# MAGIC - Column-level lineage
# MAGIC - Impact analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎓 Best Practices
# MAGIC
# MAGIC ### Pipeline Design
# MAGIC
# MAGIC ✅ **DO**:
# MAGIC - Use medallion architecture (Bronze → Silver → Gold)
# MAGIC - Apply expectations at Silver layer
# MAGIC - Use streaming tables for continuous data
# MAGIC - Use materialized views for aggregations
# MAGIC - Enable Predictive Optimization
# MAGIC - Store pipelines in Git
# MAGIC
# MAGIC ❌ **DON'T**:
# MAGIC - Mix streaming and batch in same table
# MAGIC - Skip Bronze layer
# MAGIC - Put business logic in Bronze
# MAGIC - Use `fail` action on all expectations
# MAGIC - Hardcode credentials
# MAGIC
# MAGIC ### Performance Optimization
# MAGIC
# MAGIC ```python
# MAGIC @dp.table(
# MAGIC     table_properties={
# MAGIC         "pipelines.autoOptimize.managed": "true",
# MAGIC         "delta.autoOptimize.optimizeWrite": "true"
# MAGIC     }
# MAGIC )
# MAGIC def my_table():
# MAGIC     return spark.readStream.table("source")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Comparison: Traditional ETL vs Lakeflow
# MAGIC
# MAGIC | Aspect | Traditional ETL | Lakeflow |
# MAGIC |--------|----------------|----------|
# MAGIC | **Code Style** | Imperative (step-by-step) | Declarative (define outputs) |
# MAGIC | **Connectors** | Custom code | Pre-built, managed |
# MAGIC | **Dependencies** | Manual orchestration | Automatic DAG |
# MAGIC | **Data Quality** | Custom validation | Built-in expectations |
# MAGIC | **Incremental** | Complex checkpointing | Automatic |
# MAGIC | **Error Handling** | Try/catch everywhere | Framework-managed |
# MAGIC | **Observability** | Custom logging | Built-in event log |
# MAGIC | **Infrastructure** | Manage clusters | Serverless |
# MAGIC | **CDC** | Custom tracking | Built-in flows |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛠️ Troubleshooting
# MAGIC
# MAGIC ### Issue 1: Pipeline Fails with "Table Not Found"
# MAGIC
# MAGIC **Solution**: Use `spark.readStream.table()` which waits for table creation
# MAGIC
# MAGIC ```python
# MAGIC @dp.table()
# MAGIC def silver_loans():
# MAGIC     return spark.readStream.table("bronze_loans")  # Waits automatically
# MAGIC ```
# MAGIC
# MAGIC ### Issue 2: High Expectation Failure Rate
# MAGIC
# MAGIC **Solution**: Analyze failures and adjust expectations
# MAGIC
# MAGIC ```sql
# MAGIC SELECT expectation, failed_records
# MAGIC FROM event_log
# MAGIC WHERE event_type = 'expectation' AND failed_records > 0
# MAGIC ```
# MAGIC
# MAGIC ### Issue 3: Slow Pipeline Performance
# MAGIC
# MAGIC **Solutions**:
# MAGIC - Enable Predictive Optimization
# MAGIC - Use liquid clustering
# MAGIC - Partition large tables
# MAGIC
# MAGIC ```python
# MAGIC @dp.table(
# MAGIC     table_properties={"pipelines.autoOptimize.managed": "true"},
# MAGIC     partition_cols=["state"]
# MAGIC )
# MAGIC def silver_loans():
# MAGIC     return spark.readStream.table("bronze_loans")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Additional Resources
# MAGIC
# MAGIC ### Documentation
# MAGIC
# MAGIC - [Lakeflow Overview](https://www.databricks.com/blog/introducing-databricks-lakeflow)
# MAGIC - [Lakeflow Connect GA](https://www.databricks.com/blog/announcing-general-availability-lakeflow-connect)
# MAGIC - [Spark Declarative Pipelines](https://docs.databricks.com/en/ldp/)
# MAGIC - [Python API Reference](https://docs.databricks.com/en/ldp/developer/python-ref.html)
# MAGIC - [Expectations Guide](https://docs.databricks.com/en/ldp/expectations.html)
# MAGIC - [CDC Flows](https://docs.databricks.com/en/ldp/developer/ldp-python-ref-apply-changes.html)
# MAGIC
# MAGIC ### Training
# MAGIC
# MAGIC - [Databricks Academy](https://www.databricks.com/learn/training/home)
# MAGIC - [Free Certification Prep](https://www.databricks.com/learn/certification)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Key Takeaways
# MAGIC
# MAGIC ✅ **Lakeflow** unifies data engineering:
# MAGIC - **Lakeflow Connect**: No-code ingestion (GA April 2025)
# MAGIC - **Spark Declarative Pipelines**: Declarative ETL
# MAGIC - **Lakeflow Jobs**: Orchestration
# MAGIC
# MAGIC ✅ **Declarative > Imperative**:
# MAGIC - Define WHAT, not HOW
# MAGIC - Framework handles dependencies and errors
# MAGIC
# MAGIC ✅ **New `@dp` Syntax** (2024+):
# MAGIC - `@dp.table()` for streaming
# MAGIC - `@dp.materialized_view()` for batch
# MAGIC - Old `@dlt` is deprecated
# MAGIC
# MAGIC ✅ **Data Quality Built-in**:
# MAGIC - Expectations validate declaratively
# MAGIC - Actions: `warn`, `drop`, `fail`
# MAGIC
# MAGIC ✅ **Medallion Architecture**:
# MAGIC - Bronze: Raw (streaming)
# MAGIC - Silver: Cleaned (streaming + expectations)
# MAGIC - Gold: Aggregates (materialized views)
# MAGIC
# MAGIC ✅ **CDC Made Easy**:
# MAGIC - `create_auto_cdc_flow()` handles changes
# MAGIC - SCD Type 1 or Type 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Environment Verification

# COMMAND ----------

# DBTITLE 1,Verify Environment
print("🔍 Checking environment for Spark Declarative Pipelines...\n")

# Check Databricks Runtime
dbr_version = spark.conf.get(
    "spark.databricks.clusterUsageTags.sparkVersion", "unknown"
)
print(f"✅ Databricks Runtime: {dbr_version}")

# Check Unity Catalog
try:
    current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    print(f"✅ Unity Catalog: {current_catalog}")
except:
    print("⚠️  Unity Catalog: Not available")

# Check Delta Lake
print(f"✅ Delta Lake: Available")

# Check sample data
try:
    sample_path = (
        "/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet"
    )
    sample_df = spark.read.parquet(sample_path)
    record_count = sample_df.count()
    print(f"✅ Sample Dataset: {record_count:,} records available")
except Exception as e:
    print(f"⚠️  Sample Dataset: Not accessible")

print("\n🎉 Environment check complete!")
print("\n💡 Note: Spark Declarative Pipelines are deployed via Lakeflow Jobs")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🎓 Tutorial Complete!
# MAGIC
# MAGIC You've learned modern declarative data pipelines with Lakeflow.
# MAGIC
# MAGIC **Next Steps**:
# MAGIC - Apply these patterns to your data
# MAGIC - Explore Lakeflow Connect for your sources
# MAGIC - Build production pipelines with medallion architecture
# MAGIC
# MAGIC **Questions?** Visit [Databricks Community Forums](https://community.databricks.com/)
# MAGIC
# MAGIC ---
