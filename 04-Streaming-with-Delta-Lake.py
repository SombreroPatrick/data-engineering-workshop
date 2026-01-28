# Databricks notebook source
# MAGIC %md
# MAGIC # 🌊 Streaming with Delta Lake
# MAGIC
# MAGIC **Level**: Intermediate to Advanced
# MAGIC **Duration**: 50 minutes
# MAGIC **Prerequisites**: Complete notebooks 00-03
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you will:
# MAGIC - ✅ Understand Structured Streaming fundamentals
# MAGIC - ✅ Read and write streaming data with Delta Lake
# MAGIC - ✅ Implement **Liquid Clustering in streaming** (NEW - 2024)
# MAGIC - ✅ Build real-time aggregations with windowing
# MAGIC - ✅ Handle late-arriving data with watermarking
# MAGIC - ✅ Perform stream-to-stream joins
# MAGIC - ✅ Implement Change Data Capture (CDC) patterns
# MAGIC - ✅ Monitor and manage streaming queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Documentation Links
# MAGIC
# MAGIC | Resource | Link |
# MAGIC |----------|------|
# MAGIC | 📘 Structured Streaming | [Databricks Streaming Guide](https://docs.databricks.com/en/structured-streaming/) |
# MAGIC | 🌊 Delta Streaming | [Delta Lake Streaming](https://docs.databricks.com/en/structured-streaming/delta-lake.html) |
# MAGIC | ⚡ Liquid Clustering Streaming | [Clustering in Streaming](https://docs.databricks.com/en/delta/clustering.html#streaming) |
# MAGIC | 🔄 Auto Loader | [Auto Loader Guide](https://docs.databricks.com/en/ingestion/auto-loader/) |
# MAGIC | 💧 Watermarking | [Handling Late Data](https://docs.databricks.com/en/structured-streaming/watermarking.html) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup and Configuration

# COMMAND ----------

# DBTITLE 1,Configure Spark for Streaming
# Reduce shuffle partitions for faster demos
spark.conf.set("spark.sql.shuffle.partitions", "1")

print("✅ Spark configured for streaming demo")

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import time

print("✅ Libraries imported")

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

# DBTITLE 1,Setup Directories
# Define paths for streaming demo
base_path = "/tmp/streaming_demo"
source_path = f"{base_path}/source"
checkpoint_path = f"{base_path}/checkpoints"
output_path = f"{base_path}/output"

# Clean up existing data
dbutils.fs.rm(base_path, recurse=True)

# Create directories
dbutils.fs.mkdirs(source_path)
dbutils.fs.mkdirs(checkpoint_path)

print(f"✅ Created directories:")
print(f"   Source: {source_path}")
print(f"   Checkpoints: {checkpoint_path}")
print(f"   Output: {output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌊 Structured Streaming Fundamentals
# MAGIC
# MAGIC ### What is Structured Streaming?
# MAGIC
# MAGIC **Structured Streaming** treats real-time data as an unbounded table:
# MAGIC
# MAGIC ```
# MAGIC Batch Processing:
# MAGIC ┌─────────────┐
# MAGIC │ Static Table│ → Query → Result
# MAGIC └─────────────┘
# MAGIC
# MAGIC Stream Processing:
# MAGIC ┌─────────────┐
# MAGIC │ Unbounded   │ → Continuous Query → Continuous Results
# MAGIC │ Table       │    (always running)
# MAGIC │ (growing)   │
# MAGIC └─────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC | Concept | Description |
# MAGIC |---------|-------------|
# MAGIC | 📥 **Source** | Where data comes from (files, Kafka, Delta) |
# MAGIC | 🔄 **Transformation** | Same DataFrame API as batch |
# MAGIC | 📤 **Sink** | Where results go (Delta, console, memory) |
# MAGIC | ✅ **Checkpoint** | Fault tolerance and exactly-once processing |
# MAGIC | 💧 **Watermark** | Handle late-arriving data |
# MAGIC | 🪟 **Window** | Time-based aggregations |
# MAGIC
# MAGIC ### Why Delta Lake for Streaming?
# MAGIC
# MAGIC ✅ **ACID transactions** – No partial writes or data corruption
# MAGIC ✅ **Exactly-once processing** – With checkpoints
# MAGIC ✅ **Schema enforcement** – Reject bad streaming data
# MAGIC ✅ **Time travel** – Query historical streaming data
# MAGIC ✅ **Upserts** – Handle CDC and late updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Creating a Streaming Source
# MAGIC
# MAGIC Let's simulate a stream of loan applications:

# COMMAND ----------

# DBTITLE 1,Prepare Data for Streaming Simulation
# We've already loaded orders_exploded in the setup cell above
# Now we'll use it to simulate a streaming source

print(f"✅ Using {orders_exploded.count():,} records for streaming simulation")

# COMMAND ----------

# DBTITLE 1,Write Data as JSON Files (Simulating Streaming Source)
# Split data into batches and write as JSON files
batch_size = 1000
num_batches = 5

for i in range(num_batches):
    batch_df = (
        orders_exploded.limit(batch_size)
        .withColumn("batch_id", lit(i))
        .withColumn("event_time", current_timestamp())
    )

    batch_path = f"{source_path}/batch_{i}"
    batch_df.write.format("json").mode("overwrite").save(batch_path)

    print(f"✅ Wrote batch {i} to {batch_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Reading Streaming Data

# COMMAND ----------

# DBTITLE 1,Define Schema for Streaming Source
# Define explicit schema (best practice for streaming)
orders_schema = StructType(
    [
        StructField("order_number", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("order_datetime_ts", TimestampType(), True),
        StructField("order_date", DateType(), True),
        StructField("state", StringType(), True),
        StructField("city", StringType(), True),
        StructField("loyalty_segment", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("line_total", DoubleType(), True),
        StructField("batch_id", IntegerType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)

print("✅ Schema defined for streaming source")

# COMMAND ----------

# DBTITLE 1,Create Streaming DataFrame
# Read streaming data from JSON files
streaming_df = (
    spark.readStream.format("json")
    .schema(orders_schema)
    .option("maxFilesPerTrigger", 1)
    .load(source_path)
)

print("✅ Created streaming DataFrame")
print(f"   Is streaming: {streaming_df.isStreaming}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📤 Writing Streaming Data to Delta Lake

# COMMAND ----------

# DBTITLE 1,Write Stream to Delta Table (Append Mode)
# Define output table
output_table = "orders_streaming"
spark.sql(f"DROP TABLE IF EXISTS {output_table}")

# Write streaming data to Delta table
query = (
    streaming_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/append")
    .trigger(processingTime="5 seconds")
    .toTable(output_table)
)

print(f"✅ Started streaming query: {query.name}")
print(f"   Output table: {output_table}")
print(f"   Checkpoint: {checkpoint_path}/append")

# COMMAND ----------

# DBTITLE 1,Monitor Streaming Query
# Wait for some data to be processed
time.sleep(10)

# Check query status
print(f"📊 Query Status: {query.status}")
print(f"📊 Is Active: {query.isActive}")

# Check record count
record_count = spark.table(output_table).count()
print(f"📊 Records in table: {record_count:,}")

# COMMAND ----------

# DBTITLE 1,View Streaming Data
# Display the streaming data
display(spark.table(output_table).limit(20))

# COMMAND ----------

# DBTITLE 1,Stop Streaming Query
# Stop the query
query.stop()
print("✅ Streaming query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚡ Liquid Clustering in Streaming (NEW - 2024)
# MAGIC
# MAGIC ### Why Liquid Clustering for Streaming?
# MAGIC
# MAGIC Traditional streaming writes create many small files:
# MAGIC
# MAGIC ```
# MAGIC Without Liquid Clustering:
# MAGIC ├── part-001.parquet (CA data)
# MAGIC ├── part-002.parquet (NY data)
# MAGIC ├── part-003.parquet (CA data)  ← CA data scattered
# MAGIC ├── part-004.parquet (TX data)
# MAGIC └── part-005.parquet (CA data)  ← Slow queries
# MAGIC
# MAGIC With Liquid Clustering:
# MAGIC ├── part-001.parquet (all CA data together)
# MAGIC ├── part-002.parquet (all NY data together)
# MAGIC └── part-003.parquet (all TX data together)
# MAGIC → Fast queries, organized as data arrives!
# MAGIC ```
# MAGIC
# MAGIC ### Benefits
# MAGIC
# MAGIC ✅ **Data organized as it arrives** – No post-processing needed
# MAGIC ✅ **Faster queries** – Even on streaming data
# MAGIC ✅ **Automatic optimization** – Works with Predictive Optimization
# MAGIC ✅ **No manual tuning** – Set clustering keys once
# MAGIC
# MAGIC 📖 **Learn More**: [Liquid Clustering in Streaming](https://docs.databricks.com/en/delta/clustering.html#streaming)

# COMMAND ----------

# DBTITLE 1,Create Streaming Table with Liquid Clustering
# Create table with liquid clustering
clustered_table = "orders_streaming_clustered"
spark.sql(f"DROP TABLE IF EXISTS {clustered_table}")

# Create table with clustering
spark.sql(f"""
    CREATE TABLE {clustered_table} (
        order_number INT,
        customer_id INT,
        customer_name STRING,
        order_datetime_ts TIMESTAMP,
        order_date DATE,
        state STRING,
        city STRING,
        loyalty_segment STRING,
        product_name STRING,
        price DOUBLE,
        quantity INT,
        line_total DOUBLE,
        batch_id INT,
        event_time TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (state, product_name)
""")

print(f"✅ Created table with liquid clustering: {clustered_table}")
print("✅ Clustered by: state, product_name")

# COMMAND ----------

# DBTITLE 1,Write Stream with Liquid Clustering
# Write streaming data with liquid clustering
clustered_query = (
    streaming_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/clustered")
    .trigger(processingTime="5 seconds")
    .toTable(clustered_table)
)

print(f"✅ Started streaming query with liquid clustering")
print("⚡ Data will be automatically organized by addr_state and term")

# COMMAND ----------

# DBTITLE 1,Monitor Clustered Streaming
# Wait for data to be processed
time.sleep(10)

# Check record count
clustered_count = spark.table(clustered_table).count()
print(f"📊 Records in clustered table: {clustered_count:,}")

# View table details
display(spark.sql(f"DESCRIBE DETAIL {clustered_table}"))

# COMMAND ----------

# DBTITLE 1,Stop Clustered Query
clustered_query.stop()
print("✅ Clustered streaming query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪟 Windowed Aggregations
# MAGIC
# MAGIC ### Why Windows?
# MAGIC
# MAGIC Windows enable time-based analytics on streaming data:
# MAGIC - 📊 **Tumbling windows**: Non-overlapping (e.g., hourly totals)
# MAGIC - 🔄 **Sliding windows**: Overlapping (e.g., 5-min average over last 10 min)
# MAGIC - 📅 **Session windows**: Based on activity gaps
# MAGIC
# MAGIC ### Real-Time Financial Analytics Example

# COMMAND ----------

# DBTITLE 1,Create Streaming Aggregation with Windows
# Calculate order statistics per state in 1-minute windows
windowed_aggregation = (
    streaming_df.withWatermark("event_time", "10 minutes")
    .groupBy(window(col("event_time"), "1 minute"), col("state"))
    .agg(
        count("*").alias("order_count"),
        round(avg("line_total"), 2).alias("avg_order_value"),
        round(sum("line_total"), 2).alias("total_revenue"),
        round(avg("price"), 2).alias("avg_product_price"),
    )
)

print("✅ Created windowed aggregation")

# COMMAND ----------

# DBTITLE 1,Write Windowed Aggregation to Delta
# Write aggregated results to Delta table
agg_table = "orders_streaming_aggregates"
spark.sql(f"DROP TABLE IF EXISTS {agg_table}")

agg_query = (
    windowed_aggregation.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/aggregates")
    .trigger(processingTime="5 seconds")
    .toTable(agg_table)
)

print(f"✅ Started aggregation query: {agg_table}")

# COMMAND ----------

# DBTITLE 1,View Aggregated Results
# Wait for aggregations to process
time.sleep(10)

# Display aggregated results
display(
    spark.sql(f"""
    SELECT 
        window.start as window_start,
        window.end as window_end,
        state,
        order_count,
        avg_order_value,
        total_revenue,
        avg_product_price
    FROM {agg_table}
    ORDER BY window_start DESC, total_revenue DESC
    LIMIT 20
""")
)

# COMMAND ----------

# DBTITLE 1,Stop Aggregation Query
agg_query.stop()
print("✅ Aggregation query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💧 Watermarking for Late Data
# MAGIC
# MAGIC ### Why Watermarking?
# MAGIC
# MAGIC Real-world data arrives late:
# MAGIC
# MAGIC ```
# MAGIC Expected:  Event 1 → Event 2 → Event 3 → Event 4
# MAGIC Reality:   Event 1 → Event 3 → Event 2 → Event 4
# MAGIC                                  ↑ Late arrival!
# MAGIC ```
# MAGIC
# MAGIC **Watermarking** defines how long to wait for late data:
# MAGIC
# MAGIC ```
# MAGIC Watermark = "10 minutes"
# MAGIC → Wait up to 10 minutes for late events
# MAGIC → Drop events older than 10 minutes
# MAGIC ```
# MAGIC
# MAGIC ### Trade-offs
# MAGIC
# MAGIC | Watermark | Accuracy | Memory | Latency |
# MAGIC |-----------|----------|--------|---------|
# MAGIC | Short (1 min) | ⚠️ May miss late data | ✅ Low | ✅ Fast |
# MAGIC | Long (1 hour) | ✅ Captures late data | ⚠️ High | ⚠️ Slow |

# COMMAND ----------

# DBTITLE 1,Aggregation with Watermarking
# Create aggregation with watermark for late data
watermarked_agg = (
    streaming_df.withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("state"),
    )
    .agg(
        count("*").alias("order_count"),
        round(avg("line_total"), 2).alias("avg_order_value"),
    )
)

print("✅ Created watermarked aggregation")
print("💧 Watermark: 10 minutes")
print("🪟 Window: 5 minutes (sliding every 1 minute)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Stream-to-Stream Joins
# MAGIC
# MAGIC ### Why Stream Joins?
# MAGIC
# MAGIC Join multiple real-time data sources:
# MAGIC - 📊 Enrich loan applications with credit scores
# MAGIC - 🔗 Combine user activity with transaction data
# MAGIC - 🎯 Match events across systems

# COMMAND ----------

# DBTITLE 1,Create Second Streaming Source (Product Reviews)
# Simulate product review ratings
product_reviews = orders_exploded.select(
    col("product_name"),
    col("state"),
    col("event_time"),
    (lit(1) + (rand() * 4)).cast("int").alias("rating"),
).limit(2000)

# Write reviews as streaming source
reviews_path = f"{base_path}/product_reviews"
dbutils.fs.rm(reviews_path, recurse=True)

for i in range(3):
    batch = product_reviews.limit(500).withColumn("batch_id", lit(i))
    batch.write.format("json").mode("overwrite").save(f"{reviews_path}/batch_{i}")
    print(f"✅ Wrote product review batch {i}")

# COMMAND ----------

# DBTITLE 1,Create Product Review Stream
# Define schema for product reviews
reviews_schema = StructType(
    [
        StructField("product_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("rating", IntegerType(), True),
        StructField("batch_id", IntegerType(), True),
    ]
)

# Create streaming DataFrame for product reviews
reviews_stream = (
    spark.readStream.format("json")
    .schema(reviews_schema)
    .option("maxFilesPerTrigger", 1)
    .load(reviews_path)
)

print("✅ Created product review stream")

# COMMAND ----------

# DBTITLE 1,Perform Stream-to-Stream Join
# Join orders with product reviews
joined_stream = (
    streaming_df.withWatermark("event_time", "10 minutes")
    .join(
        reviews_stream.withWatermark("event_time", "10 minutes"),
        expr("""
            product_name = product_name AND
            state = state AND
            event_time >= event_time - interval 5 minutes AND
            event_time <= event_time + interval 5 minutes
        """),
        "inner",
    )
    .select(
        streaming_df["order_number"],
        streaming_df["customer_name"],
        streaming_df["product_name"],
        streaming_df["line_total"],
        streaming_df["state"],
        reviews_stream["rating"],
        streaming_df["event_time"],
    )
)

print("✅ Created stream-to-stream join")

# COMMAND ----------

# DBTITLE 1,Write Joined Stream to Delta
# Write enriched data to Delta table
enriched_table = "orders_enriched"
spark.sql(f"DROP TABLE IF EXISTS {enriched_table}")

enriched_query = (
    joined_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/enriched")
    .trigger(processingTime="5 seconds")
    .toTable(enriched_table)
)

print(f"✅ Started enriched stream: {enriched_table}")

# COMMAND ----------

# DBTITLE 1,View Enriched Data
# Wait for data to be processed
time.sleep(10)

# Display enriched data
display(
    spark.sql(f"""
    SELECT 
        order_number,
        customer_name,
        product_name,
        line_total,
        state,
        rating,
        event_time
    FROM {enriched_table}
    ORDER BY event_time DESC
    LIMIT 20
""")
)

# COMMAND ----------

# DBTITLE 1,Stop Enriched Query
enriched_query.stop()
print("✅ Enriched query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔀 Change Data Capture (CDC) with MERGE
# MAGIC
# MAGIC ### Why CDC?
# MAGIC
# MAGIC CDC captures changes from source systems:
# MAGIC - ➕ **INSERT**: New records
# MAGIC - 🔄 **UPDATE**: Modified records
# MAGIC - ❌ **DELETE**: Removed records
# MAGIC
# MAGIC ### CDC Pattern with Delta Lake
# MAGIC
# MAGIC ```
# MAGIC Source System → CDC Stream → Delta MERGE → Target Table
# MAGIC                              ↓
# MAGIC                    Upsert (INSERT + UPDATE)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Create Target Table for CDC
# Create target table
cdc_table = "orders_cdc_target"
spark.sql(f"DROP TABLE IF EXISTS {cdc_table}")

# Initialize with some data
orders_exploded.limit(5000).write.format("delta").mode("overwrite").saveAsTable(
    cdc_table
)

print(f"✅ Created CDC target table: {cdc_table}")
print(f"📊 Initial record count: {spark.table(cdc_table).count():,}")

# COMMAND ----------

# DBTITLE 1,Simulate CDC Stream
# Create CDC stream with updates and new records
cdc_stream = streaming_df.withColumn(
    "operation", when(col("batch_id") % 2 == 0, lit("INSERT")).otherwise(lit("UPDATE"))
).withColumn(
    "updated_line_total",
    when(col("operation") == "UPDATE", col("line_total") * 0.95).otherwise(
        col("line_total")
    ),
)

print("✅ Created CDC stream with INSERT and UPDATE operations")

# COMMAND ----------


# DBTITLE 1,Apply CDC with foreachBatch and MERGE
# Define function to merge CDC data
def merge_cdc_batch(batch_df, batch_id):
    # Load target table
    target_table = DeltaTable.forName(spark, cdc_table)

    # Perform MERGE
    target_table.alias("target").merge(
        batch_df.alias("source"),
        "target.order_number = source.order_number AND target.state = source.state",
    ).whenMatchedUpdate(
        set={"line_total": "source.updated_line_total"}
    ).whenNotMatchedInsertAll().execute()

    print(f"✅ Merged batch {batch_id}")


# Write stream with foreachBatch
cdc_query = (
    cdc_stream.writeStream.foreachBatch(merge_cdc_batch)
    .option("checkpointLocation", f"{checkpoint_path}/cdc")
    .trigger(processingTime="5 seconds")
    .start()
)

print("✅ Started CDC streaming query")

# COMMAND ----------

# DBTITLE 1,Monitor CDC Results
# Wait for CDC to process
time.sleep(15)

# Check record count
final_count = spark.table(cdc_table).count()
print(f"📊 Final record count: {final_count:,}")

# View some updated records
display(
    spark.sql(f"""
    SELECT order_number, customer_name, product_name, line_total, state
    FROM {cdc_table}
    WHERE line_total > 0
    LIMIT 20
""")
)

# COMMAND ----------

# DBTITLE 1,Stop CDC Query
cdc_query.stop()
print("✅ CDC query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Monitoring Streaming Queries
# MAGIC
# MAGIC ### Key Metrics to Monitor
# MAGIC
# MAGIC | Metric | What It Means | Action If High |
# MAGIC |--------|---------------|----------------|
# MAGIC | **Input Rate** | Records/second arriving | Scale up if backlog grows |
# MAGIC | **Process Rate** | Records/second processed | Optimize query or add resources |
# MAGIC | **Batch Duration** | Time to process each batch | Reduce batch size or optimize |
# MAGIC | **Trigger Interval** | Time between batches | Adjust based on latency needs |

# COMMAND ----------

# DBTITLE 1,View Active Streaming Queries
# List all active streaming queries
active_queries = spark.streams.active

print(f"📊 Active streaming queries: {len(active_queries)}")
for query in active_queries:
    print(f"   - {query.name}: {query.status}")

# COMMAND ----------

# DBTITLE 1,View Query Progress
# Get detailed progress for a query (if any are running)
if len(active_queries) > 0:
    query = active_queries[0]
    progress = query.lastProgress

    if progress:
        print(f"📊 Query: {query.name}")
        print(f"📊 Batch ID: {progress['batchId']}")
        print(f"📊 Input Rows: {progress['numInputRows']}")
        print(
            f"📊 Process Rate: {progress.get('processedRowsPerSecond', 'N/A')} rows/sec"
        )
        print(f"📊 Batch Duration: {progress.get('batchDuration', 'N/A')} ms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Cleanup

# COMMAND ----------

# DBTITLE 1,Stop All Streaming Queries
# Stop all active queries
for query in spark.streams.active:
    query.stop()
    print(f"✅ Stopped query: {query.name}")

print("✅ All streaming queries stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC ✅ **Structured Streaming Fundamentals**
# MAGIC - Understood streaming as unbounded tables
# MAGIC - Read and wrote streaming data with Delta Lake
# MAGIC - Implemented checkpoints for fault tolerance
# MAGIC
# MAGIC ✅ **⚡ Liquid Clustering in Streaming (NEW - 2024)**
# MAGIC - Enabled automatic data organization as data arrives
# MAGIC - Achieved faster queries on streaming data
# MAGIC - No post-processing optimization needed
# MAGIC
# MAGIC ✅ **Windowed Aggregations**
# MAGIC - Built real-time analytics with time windows
# MAGIC - Calculated rolling statistics
# MAGIC - Implemented tumbling and sliding windows
# MAGIC
# MAGIC ✅ **Watermarking**
# MAGIC - Handled late-arriving data gracefully
# MAGIC - Balanced accuracy vs. memory usage
# MAGIC - Set appropriate watermark thresholds
# MAGIC
# MAGIC ✅ **Stream-to-Stream Joins**
# MAGIC - Joined multiple real-time data sources
# MAGIC - Enriched streaming data with additional context
# MAGIC - Used time-based join conditions
# MAGIC
# MAGIC ✅ **Change Data Capture (CDC)**
# MAGIC - Implemented upsert patterns with MERGE
# MAGIC - Handled INSERT and UPDATE operations
# MAGIC - Used foreachBatch for complex logic
# MAGIC
# MAGIC ✅ **Monitoring**
# MAGIC - Tracked streaming query metrics
# MAGIC - Monitored input and process rates
# MAGIC - Managed query lifecycle
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 💡 **Structured Streaming** = Same DataFrame API for batch and streaming
# MAGIC 💡 **Liquid Clustering** = Automatic data organization in streaming workloads
# MAGIC 💡 **Watermarking** = Essential for handling late data in aggregations
# MAGIC 💡 **Delta Lake** = ACID guarantees and exactly-once processing for streams
# MAGIC 💡 **foreachBatch** = Enables complex operations like MERGE in streaming
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 🎯 **Ready for production?** Open **05-Production-Ready-Patterns.py** to learn:
# MAGIC - **Predictive Optimization** (NEW - GA 2024) for automatic maintenance
# MAGIC - **Serverless Compute** (NEW - GA 2025) for zero infrastructure management
# MAGIC - **Unity Catalog Iceberg Tables** (NEW - 2024) for interoperability
# MAGIC - Performance tuning and monitoring
# MAGIC - Data governance and security
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Outstanding! 🚀 You've mastered streaming with Delta Lake!**
