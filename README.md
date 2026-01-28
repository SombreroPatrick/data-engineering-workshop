# 📊 Databricks Data Engineering Tutorials

A comprehensive, hands-on tutorial series for learning data engineering concepts with Databricks, Delta Lake, and PySpark using **financial datasets**.

## 🎯 What You'll Learn

By completing these tutorials, you will:
- ✅ Master data ingestion and transformation with PySpark
- ✅ Build production-grade Delta Lake tables with ACID guarantees
- ✅ Implement time travel, schema evolution, and data versioning
- ✅ Process streaming financial data in real-time
- ✅ Apply data quality checks and optimization techniques
- ✅ Follow Databricks best practices for production workflows

## 📚 Tutorial Series

### **Current Notebooks**

| # | Notebook | Topics Covered | Duration |
|---|----------|----------------|----------|
| 00 | [Getting Started](./00-Getting-Started.py) | Introduction, setup, dataset overview, documentation resources | ~20 min |
| 01 | [Data Ingestion & Exploration](./01-Data-Ingestion-and-Exploration.py) | Load data, explore schemas, basic transformations, data quality checks | ~30 min |
| 02 | [Delta Lake Fundamentals](./02-Delta-Lake-Fundamentals.py) | Create Delta tables, CRUD operations, Unity Catalog integration | ~40 min |
| 03 | [Delta Lake Advanced Features](./03-Delta-Lake-Advanced-Features.py) | ⚡ Liquid Clustering, 🤖 Automatic Clustering, time travel, schema evolution | ~45 min |
| 04 | [Streaming with Delta Lake](./04-Streaming-with-Delta-Lake.py) | Structured Streaming, ⚡ liquid clustering in streams, windowing, CDC | ~45 min |
| 05 | [Production-Ready Patterns](./05-Production-Ready-Patterns.py) | 🤖 Predictive Optimization, 💻 Serverless, 🧊 Iceberg tables, governance | ~60 min |
| 06 | [Lakeflow Connect & Pipelines](./06-Lakeflow-Connect-and-Pipelines.py) | 🌊 Lakeflow Connect, Spark Declarative Pipelines, medallion architecture | ~60 min |

## 💰 Dataset: Lending Club Loan Data

These tutorials use **real financial data** from Lending Club, featuring:

- **14,705 loan records** with risk indicators
- **Loan amounts, terms, and interest rates**
- **Borrower information** (income, employment, credit history)
- **Risk classification** (safe vs. risky loans)

**Why this dataset?**
- ✅ Built-in to Databricks (no download required)
- ✅ Real-world financial data
- ✅ Perfect size for learning (~1.5 MB)
- ✅ Rich features for analysis (numeric, categorical, dates)
- ✅ Used in 50+ official Databricks tutorials

**Access Path:**
```python
/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet
```

## 🚀 Getting Started

### **Prerequisites**

- Databricks workspace (Community Edition works!)
- Basic Python knowledge
- Familiarity with SQL (helpful but not required)

### **How to Use These Notebooks**

1. **Clone or download** this repository
2. **Upload to Databricks** (drag and drop into workspace)
3. **Attach to a cluster** (any runtime version)
4. **Run notebooks in order** (00 → 01 → 02)
5. **Follow along** with the markdown explanations

### **Quick Start**

```python
# In Databricks notebook
%run ./00-Getting-Started
```

## 📖 Key Concepts Covered

### **Data Engineering Fundamentals**
- Data ingestion patterns (batch, incremental, streaming)
- Schema inference vs. explicit schemas
- Data transformation with PySpark
- Medallion architecture (Bronze → Silver → Gold)

### **Delta Lake**
- ACID transactions for data lakes
- Time travel and data versioning
- Schema evolution and enforcement
- Merge/upsert operations (CDC patterns)
- Optimization (compaction, Z-ordering)

### **PySpark**
- DataFrame API and transformations
- SQL integration
- User-defined functions (UDFs)
- Performance optimization

### **Databricks Platform**
- Unity Catalog for data governance
- Databricks File System (DBFS)
- Widgets for parameterization
- Visualization with display()

## 🔗 Documentation Links

### **Official Databricks Resources**
- [Databricks Documentation](https://docs.databricks.com/)
- [Data Engineering Guide](https://docs.databricks.com/en/data-engineering/)
- [Delta Lake Tutorial](https://docs.databricks.com/en/delta/tutorial.html)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

### **Delta Lake**
- [Delta Lake Official Site](https://delta.io/)
- [Delta Lake GitHub](https://github.com/delta-io/delta)
- [Delta Lake Best Practices](https://docs.databricks.com/en/delta/best-practices.html)

### **Training & Certification**
- [Databricks Academy](https://www.databricks.com/learn/training/home)
- [Free Data Engineering Course](https://www.databricks.com/learn/training/getting-started-with-data-engineering)
- [Certification Programs](https://www.databricks.com/learn/certification)

## 🎨 Notebook Structure

Each notebook follows this pattern:

```
1. Title & Learning Objectives
2. Documentation Links
3. Prerequisites & Setup
4. Concepts Explained (with "why")
5. Hands-on Examples
6. Visualizations
7. Best Practices
8. Summary & Next Steps
```

### **Markdown Features**
- 📊 Clear section headers with emojis
- 💡 "Why this matters" explanations
- ⚠️ Common pitfalls and warnings
- 📖 Links to official documentation
- 🔑 Key concept highlights
- ✅ Best practice callouts

## 🛠️ Technical Details

### **Spark Configuration**
These notebooks use demo-optimized settings:
```python
spark.conf.set("spark.sql.shuffle.partitions", "1")
```

**Note:** Production environments should use higher values (200+) for large datasets.

### **File Paths**
All notebooks use temporary paths for demos:
```python
/tmp/delta/loan_data/
```

**Production:** Use Unity Catalog managed tables or external locations.

## 🤝 Contributing

Have suggestions or improvements? We'd love to hear from you!

- Found a bug? Open an issue
- Want to add a notebook? Submit a pull request
- Have questions? Start a discussion

## 📄 License

This project is licensed under the MIT License - feel free to use for learning and teaching!

## 🙏 Acknowledgments

- **Databricks** for excellent documentation and sample datasets
- **Delta Lake Community** for open-source contributions
- **Learning Spark V2** for the loan dataset

## 💬 Community & Support

- [Databricks Community Forums](https://community.databricks.com/)
- [Databricks Community Slack](https://databricks.com/slack)
- [Stack Overflow - Databricks Tag](https://stackoverflow.com/questions/tagged/databricks)
- [Stack Overflow - Delta Lake Tag](https://stackoverflow.com/questions/tagged/delta-lake)

---

**Happy Learning! 🚀**

*Last Updated: January 2026*
