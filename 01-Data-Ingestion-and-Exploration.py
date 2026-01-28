# Databricks notebook source
# MAGIC %md
# MAGIC # 📥 Data Ingestion and Exploration
# MAGIC
# MAGIC **Level**: Beginner
# MAGIC **Duration**: 30 minutes
# MAGIC **Prerequisites**: Complete notebook 00-Getting-Started
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you will:
# MAGIC - ✅ Load Parquet files into Spark DataFrames
# MAGIC - ✅ Understand schema inference vs. explicit schemas
# MAGIC - ✅ Perform data quality checks (nulls, duplicates, data types)
# MAGIC - ✅ Apply basic transformations (filter, select, aggregate)
# MAGIC - ✅ Create visualizations with the `display()` function
# MAGIC - ✅ Follow best practices for data ingestion
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Documentation Links
# MAGIC
# MAGIC | Resource | Link |
# MAGIC |----------|------|
# MAGIC | 📘 Reading Data | [Databricks Data Sources](https://docs.databricks.com/en/data/data-sources/) |
# MAGIC | 🐍 PySpark DataFrame API | [DataFrame Guide](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html) |
# MAGIC | 📊 Data Exploration | [Exploratory Data Analysis](https://docs.databricks.com/en/exploratory-data-analysis/) |
# MAGIC | 🔍 Data Quality | [Data Quality Best Practices](https://docs.databricks.com/en/lakehouse/data-quality.html) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup and Configuration
# MAGIC
# MAGIC Let's configure our environment for optimal demo performance:

# COMMAND ----------

# DBTITLE 1,Configure Spark for Demo
# Reduce shuffle partitions for faster demos
spark.conf.set("spark.sql.shuffle.partitions", "1")

# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Show more rows in display() output
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", "20")

print("✅ Spark configured for demo environment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Data Ingestion Fundamentals
# MAGIC
# MAGIC ### Why Parquet?
# MAGIC
# MAGIC **Parquet** is the gold standard for big data storage:
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|---------|
# MAGIC | 📦 **Columnar Storage** | Read only columns you need (faster queries) |
# MAGIC | 🗜️ **Compression** | 10x smaller than CSV (saves storage & I/O) |
# MAGIC | 📊 **Schema Embedded** | No need to specify data types manually |
# MAGIC | 🚀 **Predicate Pushdown** | Skip irrelevant data (faster filters) |
# MAGIC | 🔢 **Type Safety** | Preserves integers, decimals, dates correctly |
# MAGIC
# MAGIC ### Parquet vs. CSV
# MAGIC
# MAGIC ```
# MAGIC CSV:  Read entire file → Parse text → Convert types → Filter
# MAGIC       ❌ Slow, memory-intensive
# MAGIC
# MAGIC Parquet: Read only needed columns → Already typed → Filter during read
# MAGIC          ✅ Fast, efficient
# MAGIC ```
# MAGIC
# MAGIC 📖 **Learn More**: [Parquet Documentation](https://parquet.apache.org/docs/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 Loading Data from Parquet
# MAGIC
# MAGIC ### Method 1: Schema Inference (Recommended for Exploration)

# COMMAND ----------

# DBTITLE 1,Load Parquet with Schema Inference
# Define the dataset path
dataset_path = "/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet"

# Load the Parquet file (schema is automatically inferred)
df = spark.read.format("parquet").load(dataset_path)

# Alternative shorthand syntax:
# df = spark.read.parquet(dataset_path)

print(f"✅ Loaded {df.count():,} records")
print(f"✅ Schema inferred from Parquet metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: Explicit Schema (Recommended for Production)
# MAGIC
# MAGIC 💡 **Why explicit schemas?**
# MAGIC - ✅ Faster (no schema inference overhead)
# MAGIC - ✅ Safer (reject unexpected data)
# MAGIC - ✅ Documented (schema is code)

# COMMAND ----------

# DBTITLE 1,Define Explicit Schema
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)

# Define the schema explicitly
loan_schema = StructType(
    [
        StructField("loan_amnt", IntegerType(), True),
        StructField("funded_amnt", IntegerType(), True),
        StructField("paid_amnt", DoubleType(), True),
        StructField("addr_state", StringType(), True),
        StructField("closed", StringType(), True),
        StructField("annual_inc", DoubleType(), True),
        StructField("emp_length", DoubleType(), True),
        StructField("dti", DoubleType(), True),
        StructField("delinq_2yrs", DoubleType(), True),
        StructField("revol_util", DoubleType(), True),
        StructField("total_acc", DoubleType(), True),
        StructField("credit_length_in_years", DoubleType(), True),
        StructField("term", StringType(), True),
        StructField("home_ownership", StringType(), True),
        StructField("purpose", StringType(), True),
        StructField("verification_status", StringType(), True),
        StructField("application_type", StringType(), True),
    ]
)

# Load with explicit schema
df_explicit = spark.read.format("parquet").schema(loan_schema).load(dataset_path)

print("✅ Loaded with explicit schema")
print(f"   Records: {df_explicit.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️ **Best Practice**: Use schema inference for exploration, explicit schemas for production pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Schema Exploration
# MAGIC
# MAGIC Understanding your data structure is the first step in any data engineering workflow.

# COMMAND ----------

# DBTITLE 1,Display Schema Tree
# Print the schema in a readable tree format
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Get Column Names and Types
# Extract column names and data types
columns_info = [
    (field.name, field.dataType.simpleString()) for field in df.schema.fields
]

# Display as a DataFrame for better visualization
from pyspark.sql import Row

schema_df = spark.createDataFrame(
    [Row(column=name, data_type=dtype) for name, dtype in columns_info]
)
display(schema_df)

# COMMAND ----------

# DBTITLE 1,Count Columns by Data Type
from pyspark.sql.functions import lit, count

# Group columns by data type
type_counts = {}
for field in df.schema.fields:
    dtype = field.dataType.simpleString()
    type_counts[dtype] = type_counts.get(dtype, 0) + 1

print("📊 Column Distribution by Type:")
for dtype, count in sorted(type_counts.items()):
    print(f"   {dtype}: {count} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Data Exploration
# MAGIC
# MAGIC ### Preview Sample Records

# COMMAND ----------

# DBTITLE 1,Display First 10 Records
# Use display() for interactive table with sorting and filtering
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Pro Tip**: Click column headers to sort, use the filter icon to search, and click the chart icons to visualize!

# COMMAND ----------

# DBTITLE 1,Show Random Sample
# Get a random sample (useful for large datasets)
sample_df = df.sample(fraction=0.01, seed=42)
display(sample_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic Statistics

# COMMAND ----------

# DBTITLE 1,Summary Statistics for All Columns
# Get count, mean, stddev, min, max for numeric columns
display(df.summary())

# COMMAND ----------

# DBTITLE 1,Detailed Statistics for Key Columns
# Focus on important numeric columns
key_columns = ["loan_amnt", "funded_amnt", "paid_amnt", "annual_inc", "dti"]
display(df.select(key_columns).describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Record Counts

# COMMAND ----------

# DBTITLE 1,Total Record Count
total_records = df.count()
print(f"📊 Total Records: {total_records:,}")

# COMMAND ----------

# DBTITLE 1,Count by Categorical Columns
from pyspark.sql.functions import count, col

# Count loans by state
state_counts = (
    df.groupBy("addr_state")
    .agg(count("*").alias("loan_count"))
    .orderBy(col("loan_count").desc())
)

display(state_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Visualization**: Click the bar chart icon above to see which states have the most loans!

# COMMAND ----------

# DBTITLE 1,Count by Loan Term
term_counts = df.groupBy("term").agg(count("*").alias("count")).orderBy("term")

display(term_counts)

# COMMAND ----------

# DBTITLE 1,Count by Home Ownership
ownership_counts = (
    df.groupBy("home_ownership")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)

display(ownership_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Data Quality Checks
# MAGIC
# MAGIC ### Why Data Quality Matters
# MAGIC
# MAGIC ⚠️ **Bad data leads to**:
# MAGIC - Incorrect analytics and reports
# MAGIC - Failed ML model training
# MAGIC - Production pipeline failures
# MAGIC - Lost business value
# MAGIC
# MAGIC ✅ **Good data quality ensures**:
# MAGIC - Trustworthy insights
# MAGIC - Reliable predictions
# MAGIC - Smooth operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for Null Values

# COMMAND ----------

# DBTITLE 1,Count Nulls in Each Column
from pyspark.sql.functions import col, sum as spark_sum, count, when

# Count nulls for each column
null_counts = df.select(
    [spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
)

# Transpose for better readability
null_df = null_counts.first().asDict()
null_summary = spark.createDataFrame(
    [
        Row(column=k, null_count=v, null_percentage=round(v / total_records * 100, 2))
        for k, v in null_df.items()
    ]
).orderBy(col("null_count").desc())

display(null_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for Duplicates

# COMMAND ----------

# DBTITLE 1,Check for Duplicate Records
# Count total records
total = df.count()

# Count distinct records (all columns)
distinct = df.distinct().count()

# Calculate duplicates
duplicates = total - distinct

print(f"📊 Total Records: {total:,}")
print(f"📊 Distinct Records: {distinct:,}")
print(f"📊 Duplicate Records: {duplicates:,}")

if duplicates > 0:
    print(f"⚠️  Warning: {duplicates} duplicate records found!")
else:
    print("✅ No duplicates found")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Type Validation

# COMMAND ----------

# DBTITLE 1,Check for Invalid Numeric Values
from pyspark.sql.functions import isnan, isnull

# Check for NaN and null in numeric columns
numeric_columns = ["loan_amnt", "funded_amnt", "paid_amnt", "annual_inc", "dti"]

for col_name in numeric_columns:
    nan_count = df.filter(isnan(col(col_name))).count()
    null_count = df.filter(isnull(col(col_name))).count()

    if nan_count > 0 or null_count > 0:
        print(f"⚠️  {col_name}: {nan_count} NaN, {null_count} NULL")
    else:
        print(f"✅ {col_name}: No NaN or NULL values")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Value Range Validation

# COMMAND ----------

# DBTITLE 1,Check for Negative Loan Amounts
# Loan amounts should be positive
negative_loans = df.filter(col("loan_amnt") < 0).count()

if negative_loans > 0:
    print(f"⚠️  Warning: {negative_loans} records with negative loan amounts!")
else:
    print("✅ All loan amounts are positive")

# COMMAND ----------

# DBTITLE 1,Check for Unrealistic Values
# Annual income should be reasonable (e.g., > $0 and < $10M)
unrealistic_income = df.filter(
    (col("annual_inc") <= 0) | (col("annual_inc") > 10000000)
).count()

if unrealistic_income > 0:
    print(f"⚠️  Warning: {unrealistic_income} records with unrealistic income!")
    display(
        df.filter((col("annual_inc") <= 0) | (col("annual_inc") > 10000000)).limit(5)
    )
else:
    print("✅ All income values are realistic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Basic Transformations
# MAGIC
# MAGIC ### Selecting Columns

# COMMAND ----------

# DBTITLE 1,Select Specific Columns
# Select only the columns we need
loan_basics = df.select(
    "loan_amnt", "funded_amnt", "paid_amnt", "addr_state", "term", "purpose"
)

display(loan_basics.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering Data

# COMMAND ----------

# DBTITLE 1,Filter by Single Condition
# Get loans from California
ca_loans = df.filter(col("addr_state") == "CA")

print(f"📊 California Loans: {ca_loans.count():,}")
display(ca_loans.limit(10))

# COMMAND ----------

# DBTITLE 1,Filter by Multiple Conditions
# Get large loans (>$20k) from California with 36-month term
large_ca_loans = df.filter(
    (col("addr_state") == "CA")
    & (col("loan_amnt") > 20000)
    & (col("term") == " 36 months")
)

print(f"📊 Large CA Loans (36-month): {large_ca_loans.count():,}")
display(large_ca_loans.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Calculated Columns

# COMMAND ----------

# DBTITLE 1,Calculate Loan Repayment Percentage
from pyspark.sql.functions import round as spark_round

# Calculate what percentage of the loan has been repaid
df_with_repayment = df.withColumn(
    "repayment_pct", spark_round((col("paid_amnt") / col("funded_amnt")) * 100, 2)
)

display(
    df_with_repayment.select(
        "loan_amnt", "funded_amnt", "paid_amnt", "repayment_pct"
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 1,Categorize Loans by Size
from pyspark.sql.functions import when

# Create loan size categories
df_with_category = df.withColumn(
    "loan_size_category",
    when(col("loan_amnt") < 10000, "Small")
    .when((col("loan_amnt") >= 10000) & (col("loan_amnt") < 20000), "Medium")
    .otherwise("Large"),
)

# Count by category
category_counts = (
    df_with_category.groupBy("loan_size_category")
    .agg(count("*").alias("count"))
    .orderBy("loan_size_category")
)

display(category_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregations

# COMMAND ----------

# DBTITLE 1,Aggregate by State
from pyspark.sql.functions import (
    avg,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
)

# Calculate statistics by state
state_stats = (
    df.groupBy("addr_state")
    .agg(
        count("*").alias("loan_count"),
        spark_round(avg("loan_amnt"), 2).alias("avg_loan_amount"),
        spark_round(spark_sum("loan_amnt"), 2).alias("total_loan_amount"),
        spark_min("loan_amnt").alias("min_loan_amount"),
        spark_max("loan_amnt").alias("max_loan_amount"),
    )
    .orderBy(col("loan_count").desc())
)

display(state_stats.limit(10))

# COMMAND ----------

# DBTITLE 1,Aggregate by Loan Purpose
# Calculate average loan amount by purpose
purpose_stats = (
    df.groupBy("purpose")
    .agg(
        count("*").alias("count"),
        spark_round(avg("loan_amnt"), 2).alias("avg_amount"),
        spark_round(avg("annual_inc"), 2).alias("avg_income"),
    )
    .orderBy(col("count").desc())
)

display(purpose_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Data Visualizations
# MAGIC
# MAGIC ### Distribution Analysis

# COMMAND ----------

# DBTITLE 1,Loan Amount Distribution
# Create bins for loan amounts
from pyspark.sql.functions import floor

loan_distribution = (
    df.withColumn("amount_bin", (floor(col("loan_amnt") / 5000) * 5000).cast("int"))
    .groupBy("amount_bin")
    .agg(count("*").alias("count"))
    .orderBy("amount_bin")
)

display(loan_distribution)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Try this**: Click the histogram icon to see the distribution visually!

# COMMAND ----------

# DBTITLE 1,Income vs. Loan Amount
# Analyze relationship between income and loan amount
income_loan = df.select(
    spark_round(col("annual_inc"), -3).alias("income_rounded"), "loan_amnt"
).filter((col("annual_inc") > 0) & (col("annual_inc") < 200000))

display(income_loan.limit(1000))

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Scatter Plot**: Click the scatter plot icon to see the correlation!

# COMMAND ----------

# DBTITLE 1,Top 10 States by Total Loan Volume
top_states = (
    df.groupBy("addr_state")
    .agg(spark_round(spark_sum("loan_amnt") / 1000000, 2).alias("total_millions"))
    .orderBy(col("total_millions").desc())
    .limit(10)
)

display(top_states)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Best Practices for Data Ingestion
# MAGIC
# MAGIC ### 1. Schema Management
# MAGIC
# MAGIC | Scenario | Recommendation |
# MAGIC |----------|----------------|
# MAGIC | 🔍 Exploration | Use schema inference for speed |
# MAGIC | 🏭 Production | Use explicit schemas for safety |
# MAGIC | 📊 Analytics | Schema inference is fine |
# MAGIC | 🔄 ETL Pipelines | Always use explicit schemas |
# MAGIC
# MAGIC ### 2. Data Quality Checks
# MAGIC
# MAGIC ✅ **Always check**:
# MAGIC - Null counts and percentages
# MAGIC - Duplicate records
# MAGIC - Value ranges (min, max)
# MAGIC - Data type consistency
# MAGIC - Referential integrity
# MAGIC
# MAGIC ### 3. Performance Optimization
# MAGIC
# MAGIC ```python
# MAGIC # ❌ Bad: Read entire dataset then filter
# MAGIC df = spark.read.parquet(path)
# MAGIC filtered = df.filter(col("state") == "CA")
# MAGIC
# MAGIC # ✅ Good: Use predicate pushdown
# MAGIC df = spark.read.parquet(path).filter(col("state") == "CA")
# MAGIC ```
# MAGIC
# MAGIC ### 4. Memory Management
# MAGIC
# MAGIC ```python
# MAGIC # ❌ Bad: Load everything into memory
# MAGIC df.collect()  # Don't do this on large datasets!
# MAGIC
# MAGIC # ✅ Good: Use display() or limit()
# MAGIC display(df.limit(100))
# MAGIC ```
# MAGIC
# MAGIC ### 5. Documentation
# MAGIC
# MAGIC ✅ **Document**:
# MAGIC - Data source and update frequency
# MAGIC - Schema definitions
# MAGIC - Known data quality issues
# MAGIC - Transformation logic

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC ✅ **Data Ingestion**
# MAGIC - Loaded Parquet files with schema inference and explicit schemas
# MAGIC - Understood the benefits of Parquet over CSV
# MAGIC
# MAGIC ✅ **Schema Exploration**
# MAGIC - Inspected data types and column structure
# MAGIC - Analyzed schema composition
# MAGIC
# MAGIC ✅ **Data Quality**
# MAGIC - Checked for nulls, duplicates, and invalid values
# MAGIC - Validated data ranges and types
# MAGIC
# MAGIC ✅ **Transformations**
# MAGIC - Selected, filtered, and aggregated data
# MAGIC - Created calculated columns and categories
# MAGIC
# MAGIC ✅ **Visualizations**
# MAGIC - Used `display()` for interactive tables and charts
# MAGIC - Analyzed distributions and relationships
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 💡 **Parquet** = Columnar, compressed, schema-embedded (perfect for big data)
# MAGIC 💡 **Schema inference** = Fast exploration, explicit schemas = Production safety
# MAGIC 💡 **Data quality** = Always check nulls, duplicates, and value ranges
# MAGIC 💡 **display()** = Databricks magic for interactive visualizations
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 🎯 **Ready for Delta Lake?** Open **02-Delta-Lake-Fundamentals.py** to learn about:
# MAGIC - Creating Delta tables
# MAGIC - CRUD operations (INSERT, UPDATE, DELETE, MERGE)
# MAGIC - Unity Catalog integration
# MAGIC - Table optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Great job! 🚀 You're now ready to work with Delta Lake!**
