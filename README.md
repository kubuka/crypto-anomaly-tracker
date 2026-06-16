# Crypto-Anomaly-Detector 👽
Crypto Anomaly Detector is an automated data engineering pipeline designed to spot unusual activities across the cryptocurrency market.

 The pipeline implements Medallion Architecture combined with data warehouse. It periodically fetches live market data directly from crypto APIs, stores the raw files in a local file-based data lake, refines them through progressive staging layers, and finally loads them into a structured star schema within a local data warehouse. With everything centralized, modeled, and organized into data marts, a dedicated dashboard lets you monitor and analyze specific cryptocurrencies, making it easy to catch market anomalies the moment they happen.

### Key Features
- **Automated Ingestion** - Scheduled scripts automatically pull fresh market data without any manual work.

- **Medallion Pipeline** - Data flows systematically through progressive refining stages (Bronze and Silver) stored directly on disk.

- **Dimensional Modeling** - The final (gold) serving layer is structured into a clean Star Schema with optimized Data Marts for fast querying.

- **Visual Monitoring** - An intuitive dashboard gives you a clear view of market behavior and highlights outliers.

---

# Table of Contents

1. [Project Structure](#project-structure)

2. [Tech Stack](#tech-stack)

3. [The ETL Process](#the-etl-process)

4. [Data Warehousing and dbt Modeling](#data-warehousing-and-dbt-modeling)

5. [Airflow Orchestration](#airflow-orchestration)

6. [Dashboard](#dashboard)

7. [Dockerization](#dockerization)

8. [Summary and Key Learnings](#summary-and-key-lernings)

9. [Before You Run](#before-you-run)

---
# Project Structure
```
.
├── airflow/                      # Airflow orchestration layer
│   ├── dags/
│   │   └── crypto_pipeline.py    # Main DAG defining the ingestion and modeling workflow
│   ├── docker-compose.yml        # Multi-container setup for Airflow services
│   ├── Dockerfile                # Custom Airflow image with required dependencies
│
├── dashboard/                    # Visualization layer
│   ├── app.py                    # Analytical dashboard application
│   └── Dockerfile                # Isolated container setup for the dashboard
│
├── data/                         # File-based Data Lake storage
│   ├── bronze/                   # Raw API data ingested directly from the web (JSON)
│   ├── silver/                   # Cleaned, structured data partitioned by date (Parquet)
│
├── dbt_crypto/                   # Data transformation and modeling layer
│   ├── dbt_project.yml           # Main configuration file for the dbt project
│   ├── profiles.yml              # Database connection profile (DuckDB setup)
│   ├── packages.yml              # Declared external dbt package dependencies
│   ├── package-lock.yml          # Frozen package versions ensuring build reproducibility
│   └── models/                   # dbt SQL models structured as a Star Schema
│       └── example/
│           ├── dim_coins.sql     # Dimension table containing cryptocurrency metadata
│           ├── fact_prices.sql   # Fact table capturing market price records over time
│           ├── mart_crypto_anomalies.sql # Analytical Data Mart isolating market anomalies
│           ├── sources.yml       # Configuration and mapping of raw data sources
│           └── schema.yml        # Data quality tests and documentation mappings
│
├── scripts/                      # Core ETL logic executed by the pipeline
│   ├── pull.py                   # Ingestion script fetching data from crypto APIs
│   ├── parquet_cleaner.py        # Utility script handling file maintenance tasks
│   └── silver_transform.py       # Transformation script refining Bronze into Silver data
│
├── pyproject.toml                # Python project dependency management configurations
└── uv.lock                       # Lockfile ensuring strict Python package reproducibility
```
---
# Tech Stack
- ***Apache Airflow*** - Manages the workflow, schedules task execution, and handles automated retries and dependencies between components.
- ***PySpark*** - Used to gain hands-on experience with big data tools. It handles reading raw JSON data, data cleaning, and writing optimized, partitioned Parquet files.
- ***dbt*** - Manages the SQL transformation layer, turns staging data into a Star Schema, runs data quality tests, and generates documentation.
- ***DuckDB*** - A fast, serverless analytical engine (OLAP) that queries and models Parquet files directly on the local disk without database server overhead.
- ***Streamlit*** - Used to build a clean, interactive web application completely in Python for quick prototyping and visualization.
- ***Docker*** - Containerizes Airflow, the Python environment, and the dashboard, ensuring the entire stack runs exactly the same way on any operating system.
- ***UV*** - Provides modern Python package management with fast installations and strict dependency locking for environment stability.

---
# The ETL Process
The data pipeline runs a progressive three-step process to ingest, structure, and maintain cryptocurrency market statistics.
```text
[ API CoinGecko ] ──(pull.py)──> [ Bronze: JSON ] ──(silver_transform.py/Spark)──> [ Silver: Partitioned Parquet ]
```
### 1.  Ingestion (Bronze Layer)
The pipeline initiates inside `pull.py` by querying the CoinGecko API to fetch top cryptocurrency assets ranked by market capitalization. The raw JSON payload is staged locally:
```python
# pull.py
url = f"https://api.coingecko.com/api/v3/coins/markets?x_cg_demo_api_key={api_key}"
# ... fetching pages ...
with open("/opt/data/bronze/coins.json", "w", encoding="utf-8") as f:
    print(f"Saving {len(allcoins)} coins to coins.json")
    json.dump(allcoins, f, indent=4)
```
### 2. Transformation (Silver Layer)
Once raw data is staged, `silver_transform.py` opens up a local PySpark session to enforce a strict schema, derive time attributes for partition physical paths, and append the records into compressed Parquet files:
```python
# silver_transform.py
json_schema = StructType([
    StructField("id", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", LongType(), True),
    StructField("total_volume", LongType(), True),
    StructField("last_updated", TimestampType(), True),
])

df_silver = (
    spark.read.schema(json_schema)
    .option("multiline", "true")
    .json(bronze_path)
    .withColumn("year", year(col("last_updated")))
    .withColumn("month", month(col("last_updated")))
    .withColumn("day", day(col("last_updated")))
)

df_silver.write.mode("append").partitionBy("year", "month", "day").parquet(output_path)
```
### 3. Data Retention (Maintenance)
To control disk utilization in the local environment, `parquet_cleaner.py` evaluates directory dates against an 11-day rolling retention rule and purges old historical paths:
```python
# parquet_cleaner.py
deadline = datetime.now() - timedelta(days=retention_period_days)
# ... walking through partition tree ...
folder_date = datetime(y, m, d)
if folder_date < deadline:
    print(f"Deleting old folder: {day_path}")
    os.system(f"rm -rf {day_path}")
```
---
# Data Warehousing and dbt Modeling
This layer represents the `Load (L)` stage of the ETL pipeline, where refined data is mounted into the database and transformed into the final `Gold` analytical layer.

DuckDB plays a crucial role here. Instead of running a heavy external database server, DuckDB reads the partitioned Parquet files directly from the Silver layer directory using structural path wildcards. This approach eliminates the need for a separate staging database tool:
```yml
# sources.yml
sources:
  - name: silver_layer
    schema: silver
    tables:
      - name: crypto_prices
        config:
          external_location: '/opt/data/silver/**/*.parquet'
```
### The Star Schema (Dimensional Modeling)
Using dbt-core, the structured data is transformed into a clean Star Schema designed for fast analytical queries and simple reporting.

- dim_coins (Dimension) - Stores unique cryptocurrency metadata, capturing asset IDs, official names, and standardized uppercase ticker symbols.

- fact_prices (Fact) - Holds historical time-series records of asset prices, market caps, and trading volumes. It enforces data deduplication and automatically generates a unique deterministic fact_id using an MD5 hash function.
```sql
select 
    md5(concat(coin_id, cast(price_timestamp as varchar))) as fact_id,
    coin_id,
    current_price,
    market_cap,
    total_volume,
    price_timestamp
from source_data
where row_num = 1
```
### Data Mart: Statistical Anomaly Detection
The final analytical powerhouse of the project lives within the Gold Layer as a dedicated Data Mart (`mart_crypto_anomalies.sql`).

This model analyze price volatility in real time. It utilizes SQL window functions to calculate a moving average and a rolling standard deviation across a 10-row window. An anomaly is flagged automatically if the price delta exceeds 3 standard deviations (3-sigma rule) from the rolling average:
```sql
anomaly_detection as (
    select
        *,
        abs(current_price - rolling_avg) as price_delta,
        case
            when rolling_stddev > 0 and abs(current_price - rolling_avg) > (3 * rolling_stddev) then true
            else false
        end as is_anomaly
    from historical_metrics 
)
```
---
# Airflow Orchestration
The entire data pipeline is fully automated and orchestrated using Apache Airflow. The workflow is defined as a DAG that manages dependencies, execution order, and error handling for each stage of the pipeline.
```text
[ clean_old_parquet_files ] ──> [ api_ingestion ] ──> [ pyspark_silver_transform ] ──> [ dbt_run ] ──> [ dbt_test ]
```

### Workflow Tasks
The pipeline executes five linear tasks sequentially to guarantee data consistency:

1. `clean_old_parquet_files (PythonOperator)` - Runs first to clean up local storage by purging old partition folders that fall outside the 11-day retention limit.
2. `api_ingestion (PythonOperator)` - Connects to the external crypto API, downloads the latest market statistics, and overwrites the raw staging JSON file.
3. `pyspark_silver_transform (PythonOperator)` - Initializes a local Apache Spark session to process the new JSON data and append structured, partitioned Parquet logs onto the disk.
4. `dbt_run (BashOperator)` - Triggers dbt to load the new Parquet files, rebuild the Star Schema tables, and refresh the statistical anomaly data mart inside DuckDB.
5. `dbt_test (BashOperator)` - Executes data quality validations (such as checking for non-null values, unique primary keys, and historical consistency) to ensure the integrity of the updated records.

### Pipeline Resiliency
To ensure continuous operation without manual intervention, the DAG incorporates built-in production safeguards:

- ***max_active_runs = 1*** - Prevents overlapping executions if a previous pipeline run takes longer than expected, avoiding database write locks.
- ***Automated Retries*** - Every task is configured to automatically retry once after a short delay in case of network hiccups or temporary API rate limits.
---
# Dashboard
The serving layer of the project is implemented as an interactive web application built with Streamlit. It acts as the user-facing interface that reads directly from the DuckDB analytical warehouse to visualize calculated market metrics and statistical outliers.
```text
[ DuckDB (Gold Mart) ] ──(duckdb.connect)──> [ Streamlit App ] ──> [ Plotly Interactive Charts ]
```

### Dashboard Core Functionality
The application processes data directly from the optimized anomaly mart and provides three core diagnostic features:

- ***Asset Selection Layer*** - Dynamically extracts a list of unique processed cryptocurrencies from the warehouse, providing a dropdown selection filter.
- ***Plotly Time-Series Visualization*** - Renders a synchronized multi-line chart tracking the real-time asset price alongside its calculated 10-row rolling average.
- ***Visual Anomaly Indication*** - Filters the dataset for flagged anomaly flags and overlays red marker indicators directly on the historical price line chart to instantly catch statistical spikes or crashes.
```python
def load_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    query = """
        SELECT * FROM gold.mart_crypto_anomalies 
    """
    df = conn.execute(query).df()
    conn.close()
    return df
```
---
# Dockerization
To ensure total environment reproducibility across different operating systems, the entire project infrastructure is fully containerized. Instead of managing local runtime environments, Python libraries, or database system services, the stack spins up encapsulation profiles using Docker Compose.

```text

                     │       DOCKER COMPOSE NETWORK           │
                     └───────────────────┬────────────────────┘
                                         │
       ┌─────────────────────────────────┼─────────────────────────────────┐
       ▼                                 ▼                                 ▼
 [ PostgreSQL ] ◄─────────────── [ Apache Airflow ] ───────────────► [ Crypto Dashboard ]
                           
       │                                 │                                 │
       └─────────────────────────────────┼─────────────────────────────────┘
                                         │
                                         ▼
                      [ Shared Storage Volume Mapping Layer ]
                      (/opt/data  |  /opt/airflow/dbt_crypto)
```
### 1. Configuration DRY Principle (YAML Anchors)
To eliminate redundant configuration entries and maintain code clean, the setup utilizes native YAML anchors (`&airflow-common`) and extension keys (`<<: *airflow-common`). This pattern guarantees that core environment variables, underlying user permissions, and directory mount paths are securely inherited across all Airflow sub-components (webserver, scheduler, and init jobs) from a single source of truth.

### 2. Service Dependency and Mesh Health Routing
The orchestrator prevents race conditions during environment initialization by establishing explicit container dependency chains:

- ***Metadata Health Verification*** - The Airflow core processing containers are locked behind active system routing gates, ensuring they pause operational boots until the backend PostgreSQL server successfully handles internal operational loop connectivity tests (pg_isready).

- ***Automated Bootstrap Sequence*** - A dedicated `dbt-init` execution step automatically handles system backend database migrations, provisions operational administrator credentials, and triggers the download of external dependencies (`dbt deps`) before launching the final scheduling engines and the web user interface.
```yml
# docker-compose.yml (Dependency Mesh Implementation Example)
webserver:
  <<: *airflow-common
  command: webserver
  depends_on:
    - dbt-init

dbt-init:
  <<: *airflow-common
  command: bash -c "airflow db migrate && dbt deps"
```

### 3. Cross-Container Shared State via Shared Volumes
The most important part of this architecture is that the containers are not isolated islands – they can look into the exact same folders on your computer. By mapping volumes in the Compose file, data flows smoothly without any delays:

- The `/opt/data` folder: The Airflow script downloads raw JSON files here and generates partitioned Parquet files, making them instantly available to the other tools.
- The `/opt/airflow/dbt_crypto` folder: When Airflow runs dbt and updates tables inside the DuckDB file, the dashboard container (crypto_dashboard) immediately sees those changes and refreshes the charts for the user. There is no need to copy files or transfer them over the network.
---
# Summary and Key Lernings
Building this local data "lakehouse" stack from scratch was an intense hands-on experience that bridged the gap between isolated script writing and production-grade data systems engineering.

## Key Technical Challenges
### 1. The Containerization Puzzle
Orchestrating a multi-service container network was by far the most challenging part of the project. Forcing Airflow to coordinate with Spark engine required custom root privilege management to install OpenJDK 17 directly inside the official Airflow image. Additionally, troubleshooting cross-container storage permissions to allow Spark workers, dbt nodes, and the independent Streamlit service to concurrently read and write to the same database files/ flies storage required a deep dive into Docker Compose bind mount mechanics.

### 2. First-Time with dbt
Working with `dbt-core` for the first time introduced a completely new paradigm of data transformation. The main learning curve involved moving away from procedural python cleaning scripts toward a declarative SQL-only modeling layer. Mastering model materialization,incremental models, designing functional testing logic, and understanding how DuckDB acts as a serverless execution engine for dbt files significantly upgraded my dimensional modeling workflow.

## Key Takeaways
- ***Data Isolation is Crucial*** - Learned how to maintain clean operational boundaries by breaking data into strict Bronze (raw JSON), Silver (partitioned Parquet), and Gold (analytical Star Schema) layers.

- ***Resource Optimization*** - Gained an understanding of local storage upkeep by writing custom cleanup logic to balance file retention with machine limits.

- ***Enterprise Mindset*** - Realized that even if a local setup feels like an overkill for small data batches, building reproducible, automated, and tested pipelines is the only way to scale enterprise data operations securely.
---

# Before You Run
### 1. Configure Environment Variables

The ingestion script requires an authenticated connection to the CoinGecko API.

- Create a new text file named `.env` inside the `scripts/` directory.

- Add your personal API key into the file using the following parameter format:
```text
API_KEY=your_coingecko_api_key_here
```

### 2. Launch the Container
Once the environment variable configuration file is ready, spin up the global orchestrated multi-container application layer:

- Navigate into the `airflow/` directory containing the primary automation orchestration profile.

- Execute the initialization command to compile custom component layers and launch all underlying background services seamlessly in the background:
```bash
docker compose up -d --build
```

The service orchestration network will automatically verify cluster infrastructure health, deploy required backend database schemas, fetch necessary dbt configuration dependencies, and expose the operational interfaces:

Apache Airflow Interface: Accessible locally at `http://localhost:8080`.

Interactive Analytics Dashboard: Accessible locally at `http://localhost:8501`.