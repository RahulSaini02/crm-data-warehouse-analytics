# 🚴 Mountain Riders BI, Data Warehouse and Analytics Project

A full-stack Business Intelligence case study that walks through the design of a PostgreSQL-based data warehouse and the development of interactive Tableau dashboards for a specialty bicycle retailer — *Mountain Riders*. This project covers the entire data pipeline: data modeling, ETL development, SQL analytics, and visual storytelling.  

## 🏗️ Data Architecture

The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:

![Data Warehouse High Level Architecure](data-warehouse/docs/data-warehouse-architecture.png)

1. **Bronze Layer:** Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer:** This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer:** Houses business-ready data modeled into a star schema required for reporting and analytics.

## 📊 Project Overview

**Objective:**  
To build a centralized analytics system that enables *Mountain Riders* to track sales, customer behavior, product performance, and segment growth through interactive dashboards.

**Key Deliverables:**
- 🗃️ PostgreSQL Data Warehouse (star schema)
- 🔄 ETL Scripts for structured loading of CRM data
- 📈 Tableau Dashboards (Executive Summary + Customer/Product Insights)
- ☁️ Future roadmap for AWS automation (S3, Glue, RDS)

---

## 🏗️ Data Engineering

### 📁 Data Source
CRM datasets with customer info, sales transactions, product details, and timestamps.

### 🧱 Data Warehouse Design
Star schema with the following core tables:
- `fact_sales`
- `dim_product`
- `dim_customer`
- `dim_date`

### ⚙️ ETL Workflow
- Clean raw data (handled missing values, data types)
- Load into PostgreSQL tables
- Used Python (Pandas + SQLAlchemy) for ETL scripting

---

## 🔌 Connecting to PostgreSQL

Use the following steps to connect to the PostgreSQL database:

### Python (via SQLAlchemy):
```python
from sqlalchemy import create_engine

# Replace with your actual credentials
engine = create_engine("postgresql://username:password@host:port/database_name")

# Example usage
import pandas as pd
df = pd.read_sql("SELECT * FROM fact_sales", con=engine)
```

### psql CLI:
```bash
psql -h host -p port -U username -d database_name
```

---

## 🧠 SQL Analytics

Used advanced SQL queries for:
- Monthly and yearly revenue trends
- Top-selling products and high-value customers
- Segmentation by gender, geography, and customer type
- Revenue vs Order volume breakdown

Sample SQL:
```sql
SELECT dp.category, SUM(fs.revenue) AS total_revenue
FROM fact_sales fs
JOIN dim_product dp ON fs.product_id = dp.product_id
GROUP BY dp.category;
```

---

## 📈 Tableau Dashboards

### 📊 Executive Summary
- Total sales: `$29M+`
- Average product price: `$486`
- Total customers: `18K+`
- Revenue trends by month and category
- Sales by country

### 👥 Product & Customer Analysis
- Top/low-performing products
- Top 5 customers by revenue
- Gender-wise customer distribution
- Customer segments: New, VIP, Regular

📸 Screenshots:
- `/assets/executive-summary.png`
- `/assets/product-customer-analysis.png`

---

## 🔮 Future Work: Cloud Automation

Planned migration to AWS cloud for scalability:
- **AWS S3** for raw data storage  
- **AWS Glue** for serverless ETL pipelines  
- **Amazon RDS (PostgreSQL)** for managed data warehousing  
- **Scheduled jobs** for automated refreshes  
- **Tableau Cloud** or **Server** for sharing live dashboards  

---

## 🧰 Tech Stack

| Tool         | Use Case                        |
|--------------|----------------------------------|
| PostgreSQL   | Data warehouse                  |
| Python       | ETL scripting                   |
| SQL          | Business logic & KPIs           |
| Tableau      | Dashboarding & storytelling     |
| AWS (planned)| Cloud automation & hosting      |

---

## 📂 Folder Structure

```
mountain-riders-bi/
│
├── data/                     # Raw input files
├── etl/                      # Python ETL scripts
├── sql/                      # SQL queries and schema files
├── dashboards/               # Tableau workbook files
├── assets/                   # Dashboard images, ERD, thumbnails
└── README.md                 # Project overview
```

---

## 🙋‍♂️ Author

**Rahul Saini**  
Data Engineer | BI Developer  
🔗 [LinkedIn](https://linkedin.com/in/rahulsaini02)  

---

## 📝 License

This project is licensed under the [MIT License](LICENSE).
