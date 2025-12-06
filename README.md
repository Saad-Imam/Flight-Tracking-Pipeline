**A Big Data Architecture Project for Real-Time Ingestion, Processing, Storage, and Predictive Analytics of Flight Data.**

---
### 🎯 Business Problem
**Domain:** Aviation & Supply Chain  
**Problem:** Flight delays cost the aviation industry billions annually and cause significant passenger dissatisfaction. Identifying the root causes (weather, carrier efficiency, traffic) in real-time is difficult due to data silos.
**Goal:** 
1. Provide Managers with a **Live Dashboard** of flight operations and delays.
2. Provide a **Predictive Model** that estimates delay duration for upcoming flights to allow for proactive resource allocation.

---

## 🏗 Architecture

The pipeline is fully dockerized and orchestrated by Airflow. It separates "Hot" (Real-time) and "Cold" (Archival) storage paths.

### 🛠 Tech Stack
| Component | Technology | Role |
| :--- | :--- | :--- |
| **Ingestion** | **Kafka** (w/ Zookeeper) | Buffers high-velocity streaming data from the generator. |
| **Orchestration** | **Apache Airflow** | Schedules micro-batches, model training, and archival jobs. |
| **Processing** | **Apache Spark** (PySpark) | Performs ETL, Joins, Aggregations, and ML Inference. |
| **Hot Storage** | **MongoDB** | Stores processed, recent flight data for the dashboard API. |
| **Cold Storage** | **HDFS** (Hadoop) | Long-term archival of raw data (Parquet format). |
| **Metadata** | **Hive Metastore** | Manages schema definitions for analytical querying. |
| **Caching** | **Redis** | Caches static dimensions (Airports, Airlines) for fast joins. |
| **Visualization** | **Streamlit / Custom API** | Front-end dashboard fetching data from Mongo/Spark. |

### 🔄 Data Flow
1.  **Generation:** A Python producer (seeded with statistical stats from the [Kaggle Flight Dataset 2019-2023](https://www.kaggle.com/datasets/patrickzel/flight-delay-and-cancellation-dataset-2019-2023/data)) pushes JSON events to **Kafka** topics (`flights`, `weather`).
2.  **Ingestion & Processing:** Airflow triggers a **Spark** job every 60 seconds (Micro-batch).
    *   Spark reads new offsets from Kafka.
    *   Fetches static dimensions (Airline names, Airport cities) from **Redis**.
    *   Joins Flight Stream + Weather Stream + Dimensions.
    *   Runs the **ML Regressor** to predict `delay_minutes`.
3.  **Storage:**
    *   **Hot:** Enriched data with predictions is upserted into **MongoDB**.
    *   **Cold:** Raw batch data is appended to **HDFS** in Parquet format.
4.  **Archival:** A separate Airflow DAG monitors MongoDB size. If >300MB, old records are moved to HDFS and purged from Mongo.
5.  **Visualization:** The Dashboard polls MongoDB every minute to update charts.

---

## 📊 Data Dictionary & Schema

To satisfy the requirement for complex join-based queries, the schema is normalized.

### 1. Fact Table (Streaming)
**Source:** Kafka Topic `flight_events`  
**Description:** Generated in real-time based on statistical probabilities.

| Column | Type | Description |
| :--- | :--- | :--- |
| `flight_id` | UUID | Unique identifier. |
| `timestamp` | Timestamp | Event generation time. |
| `carrier_code` | String (FK) | Join Key for Airlines. |
| `tail_num` | String (FK) | Join Key for Aircraft. |
| `origin_airport_id` | Int (FK) | Join Key for Airports. |
| `dest_airport_id` | Int (FK) | Join Key for Airports. |
| `dep_delay` | Float | Actual departure delay (Target Variable). |
| `taxi_out` | Float | Minutes spent taxiing. |
| `distance` | Float | Flight distance. |

### 2. Dimension Tables (Static/Cached in Redis)
**Source:** Pre-loaded from CSVs to Postgres/Redis.

*   **Dim_Airlines:** `carrier_code`, `airline_name`, `country`, `fleet_size`.
*   **Dim_Airports:** `airport_id`, `city`, `state`, `latitude`, `longitude`.
*   **Dim_Aircraft:** `tail_num`, `manufacturer`, `model`, `engine_type`, `year_built`.
*   **Dim_Weather:** (Streamed & Joined) `airport_id`, `timestamp`, `temperature`, `visibility`, `wind_speed`.

---

## 📈 KPIs & Dashboard

The dashboard visualizes the following 5 KPIs derived via Spark SQL aggregations:

1.  **Average Delay Minutes:** (Grouped by Airline & Airport).
2.  **On-Time Performance (OTP) %:** Percentage of flights with `<15 min` delay.
3.  **Predicted vs. Actual Delay:** Accuracy metric for the ML model.
4.  **Weather Impact Score:** Correlation between `wind_speed` and `dep_delay`.
5.  **Total Flight Volume:** Rolling count of active flights per hour.

---

## 🤖 Machine Learning Model

*   **Type:** Regression (Random Forest / Linear Regression).
*   **Features:** `month`, `day_of_week`, `distance`, `carrier_code`, `origin_weather_wind`, `dep_hour`.
*   **Target:** `arrival_delay` (minutes).
*   **Pipeline:**
    *   **Training:** Batch job runs daily on HDFS historical data to retrain the model.
    *   **Inference:** Real-time Spark job loads the model and predicts delays for incoming Kafka messages.

---

## 🚀 How to Run

### Prerequisites
*   Docker & Docker Compose (allocated min 8GB RAM).
*   Python 3.8+

### Steps
1.  **Clone the Repo:**
    ```bash
    git clone https://github.com/your-username/skystream.git
    cd skystream
    ```

2.  **Start Services:**
    ```bash
    docker-compose up -d --build
    ```
    *This spins up: Zookeeper, Kafka, Spark Master/Workers, MongoDB, Airflow, Redis, HDFS Namenode/Datanode.*

3.  **Initialize Metadata:**
    ```bash
    # Run the script to load static dimensions into Redis
    python scripts/init_dimensions.py
    ```

4.  **Start the Generator:**
    ```bash
    # Starts generating synthetic flight data to Kafka
    python producer/generator.py
    ```

5.  **Access Interfaces:**
    *   **Airflow UI:** `http://localhost:8080` (Trigger the `flight_processing_dag`)
    *   **Spark Master:** `http://localhost:8081`
    *   **Dashboard:** `http://localhost:8501`

---

## 📦 Archiving Policy
*   **Threshold:** 300 MB of data in MongoDB (Hot Store).
*   **Format:** Parquet (Snappy Compression) on HDFS.
*   **Metadata:** JSON sidecar files stored in HDFS `/metadata/` containing batch IDs and schema versions.

---

## 👥 Contributors
*   [Your Name]
*   [Teammate Name]
*   [Teammate Name]

*Big Data Architecture Course Final Project.*