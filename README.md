
# 🛒 Ecommerce Lakehouse Data Engineering Project

A complete end-to-end **Databricks Lakehouse Medallion Architecture Project** built for processing, transforming, and analyzing Ecommerce transactional data using **PySpark**, **Delta Lake**, and **Databricks SQL**.

This project demonstrates how raw ecommerce datasets move through the **Bronze → Silver → Gold** layers while applying data cleaning, standardization, enrichment, and business-ready analytics transformations.

---

# 📌 Project Overview

The project follows the **Medallion Architecture** pattern:

- **Bronze Layer** → Raw data ingestion
- **Silver Layer** → Data cleaning and transformation
- **Gold Layer** → Business-ready dimensional and fact tables
- **Analytics Layer** → Materialized views for reporting and BI dashboards

The pipeline processes:

- Customers
- Products
- Brands
- Categories
- Order Transactions

---

# 🏗️ Architecture

```text
Raw CSV Files
      │
      ▼
Bronze Layer (Raw Delta Tables)
      │
      ▼
Silver Layer (Cleaned & Standardized Tables)
      │
      ▼
Gold Layer (Business & Analytical Tables)
      │
      ▼
Materialized Views / BI Reporting
```

---

# ⚙️ Technologies Used

| Technology | Purpose |
|---|---|
| Databricks | Unified data engineering platform |
| PySpark | Distributed data processing |
| Delta Lake | ACID-compliant storage layer |
| Databricks SQL | Analytics and materialized views |
| Unity Catalog | Data governance and catalog management |
| Medallion Architecture | Layered data pipeline design |

---

# 📂 Project Structure

```bash
Ecommerce-main/
│
├── Setup/
│   └── setup_process.py
│
├── Medalion_processing/
│   ├── 1_bronze.py
│   ├── 1_silver.py
│   └── 1_gold.py
│
├── Medalion_processing_fact/
│   ├── 1_fact_bronze.py
│   ├── 2_fact_silver.py
│   └── 3_fact_gold.py
│
├── SQL_Query/
│   └── query_for_transaction_view.sql
│
└── start.py
```

---

# 🔄 Data Pipeline Workflow

# 1️⃣ Setup Layer

The setup process creates:

- Unity Catalog
- Bronze schema
- Silver schema
- Gold schema
- Source data schema

### File:
`Setup/setup_process.py`

---

# 2️⃣ Bronze Layer — Raw Data Ingestion

The Bronze layer ingests raw CSV files directly into Delta tables.

### Raw datasets:
- Brands
- Categories
- Customers
- Products
- Order Items

### Key Features:
- Schema enforcement
- Metadata tracking
- File lineage capture
- Ingestion timestamps

### Example Tables:
- `ecommerce.bronze.bronz_brands`
- `ecommerce.bronze.bronz_products`
- `ecommerce.bronze.bronz_customers`
- `ecommerce.bronze.bronz_order_items`

---

# 3️⃣ Silver Layer — Data Cleaning & Standardization

The Silver layer performs transformation and quality improvement.

## ✅ Data Cleaning Operations

### Products
- Removed invalid characters
- Standardized category and brand codes
- Corrected material spelling anomalies
- Converted datatypes
- Cleaned numeric columns
- Fixed negative rating counts

### Categories
- Removed duplicates
- Converted category codes to uppercase

### Brands
- Trimmed spaces
- Standardized brand codes
- Fixed inconsistent category mappings

### Orders
- Removed duplicate order rows
- Converted quantity into integer
- Standardized sales channels
- Cleaned percentage and currency fields
- Converted timestamps and dates

---

# 4️⃣ Gold Layer — Business-Ready Tables

The Gold layer creates analytical tables optimized for reporting.

## 🟨 Gold Product Table
Combines:
- Products
- Brands
- Categories

Provides:
- Product dimension
- Brand enrichment
- Category enrichment

---

## 🟨 Gold Customer Table

Adds:
- Regional mapping
- Country-state standardization

Supported regions:
- India
- United States
- United Kingdom
- Australia
- Canada
- UAE
- Singapore

---

## 🟨 Gold Fact Order Table

Business metrics generated:

- Gross Amount
- Discount Amount
- Net Amount
- Tax Amount
- Coupon Flag
- Currency Conversion
- INR Sales Calculation
- Date Key Generation

### Final Fact Table:
`ecommerce.gold.gld_fact_order_items`

---

# 📊 Materialized View for Analytics

The SQL layer creates a business-ready reporting view by joining:

- Fact transactions
- Date dimension
- Product dimension

### Generated Insights:
- Hourly sales
- Product category performance
- Brand analysis
- Weekend trends
- Quarterly analysis

### File:
`SQL_Query/query_for_transaction_view.sql`

---

# 💡 Key Data Engineering Concepts Demonstrated

- Medallion Architecture
- ETL / ELT Pipeline Design
- Delta Lake Processing
- Data Quality Management
- Slowly Changing Business Dimensions
- Fact & Dimension Modeling
- Materialized Views
- Data Standardization
- Currency Conversion Logic
- Databricks Lakehouse Design

---

# 🚀 How to Run the Project

## Step 1 — Create Databricks Environment

Create:
- Catalog
- Schemas
- Volumes

Run:

```python
Setup/setup_process.py
```

---

## Step 2 — Load Raw CSV Files

Upload datasets into:

```text
/Volumes/ecommerce/source_data/raw/
```

Required folders:
- brands/
- category/
- customers/
- products/
- order_items/

---

## Step 3 — Run Bronze Pipelines

Run:
- `1_bronze.py`
- `1_fact_bronze.py`

---

## Step 4 — Run Silver Pipelines

Run:
- `1_silver.py`
- `2_fact_silver.py`

---

## Step 5 — Run Gold Pipelines

Run:
- `1_gold.py`
- `3_fact_gold.py`

---

## Step 6 — Create Reporting View

Execute:

```sql
query_for_transaction_view.sql
```

---

# 📈 Business Use Cases

This project can support:

- Ecommerce sales analytics
- Customer segmentation
- Product performance analysis
- Regional sales analysis
- Coupon effectiveness tracking
- BI dashboard integration
- Financial reporting

---

# 🧠 Skills Demonstrated

- Data Engineering
- Databricks
- PySpark
- SQL
- Delta Lake
- Data Warehousing
- ETL Pipelines
- Data Transformation
- Analytics Engineering

---

# 📷 Add Project Screenshots

You can add your Databricks screenshots or dashboard images here.
[Dashboard](im)
## Example

```markdown
## Dashboard Preview

![Dashboard](images/dashboard.png)

```

---

# 📌 Recommended Screenshots to Add

## 1️⃣ Databricks Workflow Screenshot
Show:
- Bronze
- Silver
- Gold tables
- Pipeline execution

Suggested filename:
```text
images/workflow.png
```

---

## 2️⃣ Dashboard / Query Result Screenshot
Show:
- Sales analytics
- Product analysis
- SQL output
- BI visualization

Suggested filename:
```text
images/dashboard.png
```

---

# 🛠️ How to Add Images to GitHub README

## Step 1
Create a folder inside your repository:

```text
images/
```

---

## Step 2
Add your screenshots inside the folder.

Example:
```text
images/workflow.png
images/dashboard.png
```

---

## Step 3
Add image markdown inside README.md

```markdown
## Workflow

![Workflow](images/workflow.png)

## Dashboard

![Dashboard](images/dashboard.png)
```

---

# ✨ Future Improvements

- Add streaming ingestion
- Implement Auto Loader
- Add orchestration with Workflows
- Add data quality expectations
- Build Power BI dashboards
- Add CI/CD integration
- Implement Slowly Changing Dimensions (SCD Type 2)

---

# 👨‍💻 Author

## Raihan Kabir

Data Engineering & Analytics Enthusiast  
Focused on:
- Databricks
- Lakehouse Architecture
- Data Engineering
- Machine Learning
- Analytics Engineering

---

# ⭐ If You Like This Project

Give the repository a ⭐ on GitHub and share your feedback.
