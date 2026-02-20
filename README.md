# challenge-data-engineer
Liquor Distribution Analytics: a data engineering solution for analyzing profits and margins in a liquor distribution business.




## Table of Contents
- [Overview](#overview)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Data Files](#data-files)
- [Reports Generated](#reports-generated)
- [Testing](#testing)
- [Database Schemas](#database-schemas)
- [Performance Notes](#performance-notes)
- [Scalability Considerations](#scalability-considerations)
- [Architectural & Processing Decisions](#architectural--processing-decisions)



## Overview
This project ingests CSV data into PostgreSQL, transforms it through a multi-layer OLAP schema, and generates insights into product and brand performance. The solution implements a three-tier data warehouse architecture:
- **Bronze Schema**: Raw data ingestion from CSV files
- **Silver Schema**: Cleaned and normalized data with star schema relationships
- **Gold Schema**: Aggregated data optimized for reporting and analysis

### Key Features
- Automated CSV ingestion into PostgreSQL using Docker Compose
- Multi-level data transformation pipeline
- Comprehensive test suite with pytest
- Excel report generation with visualizations using matplotlib




## Project Structure
```
data_pipeline/
├── data/                           # Input CSV files
├── reports/                        # Generated reports
├── sql/
│   ├── create_olap_bronze.sql      # Base table schemas
│   ├── create_olap_silver.sql      # Normalized star schema
│   └── create_olap_gold.sql        # Reporting aggregations
├── src/
│   ├── __init__.py
│   ├── db.py                       # Database connection and setup
│   ├── ingest.py                   # CSV ingestion logic
│   ├── transform.py                # Data transformation (silver & gold)
│   └── report.py                   # Report generation
├── tests/
│   ├── __init__.py
│   ├── test_db.py                  # Database tests
│   ├── test_ingest.py              # Database tests
│   ├── test_transform.py           # Database tests
│   └── test_report.py              # Transformation tests
├── .coveragerc
├── .env
├── .gitignore
├── docker-compose.yaml
├── main.py                         # Main pipeline orchestrator
├── pytest.ini
├── requirements.txt
└── README.md
```




## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.8+

### 1. Environment Setup
Create a `.env` file in the project root:
```env
POSTGRES_USER=<user>
POSTGRES_PASSWORD=<pwd>
POSTGRES_HOST=<host>
POSTGRES_PORT=<port_number>
POSTGRES_DB=<db>
PGADMIN_DEFAULT_EMAIL=<email@domain.com>
PGADMIN_DEFAULT_PASSWORD=<pwd>
PGADMIN_PORT=<port_number>
```

### 2. Install Dependencies
```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
source .venv/bin/activate # Windows -> .venv\Scripts\activate

# Install packages
pip install -r requirements.txt
```

### 3. Start Database
```bash
docker-compose up -d
```
This starts PostgreSQL and PgAdmin. Access PgAdmin at `http://localhost:<port_number>`

### 4. Run the Full Pipeline
```bash
python main.py
```
This executes the complete pipeline:
- **Ingest**: Loads CSV files into `olap_bronze` schema
- **Transform (Silver)**: Cleans and normalizes data into `olap_silver`
- **Transform (Gold)**: Aggregates data into `olap_gold` for reporting
- **Report**: Generates reports

#### Individual Steps
```bash
# Run specific step
python main.py --step <step>

# Change ditectories
python main.py --data-dir <data_dir/> --reports-dir <report_dir/>
```




## Data Files
Required CSV inputs ([download](https://www.pwc.com/us/en/careers/university-relations/data-and-analytics-case-studies-files.html)):
- `SalesFINAL12312016.csv` - Sales transactions
- `PurchasesFINAL12312016.csv` - Purchase data
- `BegInvFINAL12312016.csv` - Beginning inventory
- `EndInvFINAL12312016.csv` - Ending inventory
- `InvoicePurchases12312016.csv` - Invoice details
- `2017PurchasePricesDec.csv` - Purchase pricing




## Reports Generated
The Excel report includes:
- **Top 10 Products by Profit ($)** - Highest grossing products
- **Top 10 Products by Margin (%)** - Most profitable % products
- **Top 10 Brands by Profit ($)** - Best performing brands
- **Top 10 Brands by Margin (%)** - Most profitable brand margins
- **Unprofitable Items** - Products and brands losing money




## Testing
```bash
pytest
```

```bash
# Run specific test file
pytest tests/<file>.py
```




## Database Schemas

### Bronze (Raw Data)
Raw tables directly from CSV files, minimal transformation (like castings or triming).

### Silver
Star schema with fact and dimension tables (the schema is not itself related with keys since the idea is to emulate an analytics DWH):
- Fact tables: Sales, Purchases
- Dimension tables: Products, Brands, Dates, Inventory

### Gold (Reporting)
Pre-aggregated tables and views for quick reporting (profitability analysis in this case):
- Product performance metrics
- Brand performance metrics




## Performance Notes

### Database Optimization
- **Indexing Strategy**
  - Consider the creation of indexes on frequently filtered and joined columns.
  - Consider composite indexes for common analytical query patterns.
- **Bulk Insert Strategy**
  - Keep the use of batch inserts (`chunksize` in pandas) to avoid memory spikes and excessive transaction overhead.
- **Transaction Management**
  - Keep ingestion steps in controlled transactions to avoid committing per row.
  - Keep automatic rollback in case of failures to preserve data consistency.

### Pipeline Optimization
- **Chunked Processing**
  - Consider reading the CSV files using chunked loading (`chunksize`) to reduce memory footprint.
  - Consider processing data incrementally instead of loading entire datasets into memory.
- **Step Isolation**
  - Keep the pipeline modular (`--step` argument), allowing selective execution (reduce unnecessary recomputation).




## Scalability Considerations
- Designed to allow "future migration" to:
  - Columnar storage formats.
  - Workflow orchestration tools.
  - Cloud data warehouses.
- Future scalability improvements:
  - Table partitioning.
  - Incremental load strategy instead of full refresh.
  - Migration to distributed processing for larger datasets.




## Architectural & Processing Decisions
- **Pandas vs Distributed Engines**
  - Pandas chosen for simplicity and readability.
  - Spark or similar frameworks recommended for significantly larger datasets.
- **Full Refresh vs Incremental Loads**
  - Current implementation uses full refresh for consistency and simplicity.
- **OLTP vs OLAP Separation**
  - Analytical schemas (Silver/Gold) are separated from raw ingestion (Bronze).
  - Optimized for read-heavy analytical workloads rather than transactional updates.