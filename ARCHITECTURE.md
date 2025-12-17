# Big Data Streaming Analytics Architecture - Flight Tracking Pipeline

## Executive Summary

This document describes a comprehensive big-data streaming analytics architecture designed for real-time flight tracking and delay prediction. The system maintains a minimum of 300 MB of active data in MongoDB, automatically archives older data to HDFS when the threshold is exceeded, and provides real-time analytics through a multi-page Streamlit dashboard. The entire pipeline is orchestrated using Apache Airflow and deployed through Docker containers.

---

## 1. Architecture Overview

### 1.1 System Components

The architecture consists of the following major components:

1. **Data Generation Layer**: Synthetic flight data generator producing continuous streaming events
2. **Message Queue**: Apache Kafka for buffering high-velocity streaming data
3. **Processing Engine**: Apache Spark for real-time streaming, batch ETL, OLAP queries, and ML inference
4. **Operational Data Store**: MongoDB for fresh, real-time data storage
5. **Cold Storage**: HDFS for long-term archival storage
6. **Data Warehouse**: Hive-based data warehouse for analytical queries
7. **Metadata Management**: Hive tables and HDFS JSON files for schema and archive metadata
8. **Caching Layer**: Redis for dimension data and query result caching
9. **Orchestration**: Apache Airflow for workflow management
10. **Visualization**: Streamlit dashboard with 6 comprehensive pages

### 1.2 Data Flow Architecture

```
┌─────────────┐
│   Producer  │ ────┐
│  (Synthetic │     │
│   Data Gen) │     │
└─────────────┘     │
                    ▼
            ┌──────────────┐
            │    Kafka     │
            │  (Buffering) │
            └──────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   Apache Spark        │
        │  (Processing Engine)  │
        │  - Stream Processing  │
        │  - ML Inference       │
        │  - Data Enrichment    │
        └───────────────────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
        ▼                       ▼
┌──────────────┐      ┌──────────────┐
│   MongoDB    │      │     HDFS     │
│  (Hot Store) │      │ (Cold Store) │
│  < 300 MB    │      │  (Archive)   │
└──────────────┘      └──────────────┘
        │                       │
        │                       │
        ▼                       ▼
┌──────────────┐      ┌──────────────┐
│    Hive      │◄─────┤   Metadata   │
│Data Warehouse│      │  (Hive/JSON) │
└──────────────┘      └──────────────┘
        │
        ▼
┌──────────────┐
│  Streamlit   │
│  Dashboard   │
└──────────────┘
```

---

## 2. Component Design Decisions

### 2.1 Data Ingestion: Apache Kafka

**Justification**: Kafka provides a distributed, fault-tolerant message queue capable of handling high-velocity streaming data. It decouples producers from consumers, allowing the Spark processing engine to consume data at its own pace while buffering incoming events.

**Configuration**:
- Single broker setup for development (scalable to cluster)
- Topic: `flight_events`
- Retention policy: 7 days
- Replication factor: 1 (development)

**Rationale**: Kafka's log-structured storage and consumer offset management enable exactly-once processing semantics and allow replay of historical data if needed.

### 2.2 Processing Engine: Apache Spark

**Justification**: Apache Spark serves as the core processing engine for multiple use cases:

1. **Streaming Analytics**: Structured Streaming API for real-time micro-batch processing
2. **Batch ETL**: Spark SQL for data transformation and enrichment
3. **OLAP Queries**: Spark SQL with Hive integration for analytical queries
4. **ML Inference**: Spark MLlib and custom model loading for prediction
5. **Archiving Operations**: Spark for reading from MongoDB and writing to HDFS

**Key Design Decisions**:

- **Micro-batch Processing**: Uses Spark Structured Streaming with `availableNow` trigger for micro-batch processing (every 60 seconds)
- **Unified API**: Single Spark application handles both streaming and batch operations
- **Hive Integration**: Spark SQL with Hive support enables seamless querying of archived data

**Spark Configuration**:
```python
SparkSession.builder
    .config("spark.sql.warehouse.dir", "hdfs://hdfs-namenode:9000/user/hive/warehouse")
    .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
```

### 2.3 Operational Data Store: MongoDB

**Justification**: MongoDB provides a flexible, document-based storage system ideal for operational workloads. It offers:
- Fast write performance for streaming data
- Flexible schema for evolving data structures
- Native JSON support matching Kafka message format
- Efficient querying for dashboard visualization

**Architecture**:
- Database: `flight_tracking`
- Collection: `flight_events`
- Indexes: `timestamp`, `flight_id`, `ingestion_timestamp`
- Size Monitoring: Automated monitoring via Airflow DAG (5-minute intervals)

**Data Retention Policy**: 
- Active data maintained in MongoDB (target: < 300 MB)
- Older data automatically archived to HDFS when threshold exceeded
- 7-day sliding window by default (configurable)

### 2.4 Cold Storage: HDFS (Hadoop Distributed File System)

**Justification**: HDFS provides scalable, distributed storage for large volumes of historical data. It offers:
- Fault tolerance through replication
- Cost-effective storage for long-term archival
- Integration with Hadoop ecosystem (Hive, Spark)
- Support for columnar formats (Parquet) for analytical queries

**Storage Structure**:
```
hdfs://hdfs-namenode:9000/
├── archive/
│   ├── flight_data/
│   │   └── archive_<timestamp>/
│   │       └── part-*.parquet
│   └── mongodb_archive/
│       └── archive_<timestamp>/
│           └── part-*.parquet
└── metadata/
    ├── batch_<batch_id>.json
    └── archives/
        └── <archive_id>_metadata.json
```

**Format**: Parquet (columnar, compressed with Snappy)
- Benefits: Efficient columnar storage, schema evolution support, predicate pushdown

### 2.5 Data Warehouse: Apache Hive

**Justification**: Hive provides SQL-like interface over HDFS data, enabling:
- Familiar SQL syntax for analytical queries
- Schema-on-read flexibility
- Integration with Spark for distributed processing
- Support for partitioned tables for performance

**Data Warehouse Schema**:

**Staging Table** (`flight_dw.staging_flight_events`):
- Purpose: Intermediate storage after data cleaning and enrichment
- Partitioned by: `processing_date`
- Contains: Raw data + enrichment fields + quality scores

**Fact Table** (`flight_dw.fact_flight_events`):
- Purpose: Cleaned, validated data for analytics
- Partitioned by: `processing_date`
- Contains: Flight facts + predictions + accuracy metrics

**Predictions Table** (`flight_dw.predictions`):
- Purpose: Store ML predictions separately for analysis
- Partitioned by: `prediction_date`
- Contains: Prediction ID, flight ID, predicted/actual delays, model version

**Metadata Tables**:
- `schema_metadata`: Schema definitions and versions
- `batch_metadata`: Batch processing metadata
- `archive_metadata`: Archive operation metadata

### 2.6 Metadata Management Strategy

**Design Principle**: Comprehensive metadata tracking enables data lineage, schema evolution tracking, and operational monitoring.

**Storage Locations**:

1. **HDFS JSON Files**: 
   - Location: `hdfs://hdfs-namenode:9000/metadata/`
   - Format: JSON
   - Contains: Batch metadata, archive metadata with timestamps, record counts, file sizes, schema definitions

2. **Hive Metadata Tables**:
   - Database: `flight_dw`
   - Tables: `schema_metadata`, `batch_metadata`, `archive_metadata`
   - Format: Delta tables (or Parquet)
   - Enables: SQL-based metadata queries and joins with fact tables

**Metadata Schema**:
```json
{
  "batch_id": "batch_1234567890",
  "timestamp": "2024-01-15T10:30:00Z",
  "record_count": 1000,
  "file_size_mb": 5.2,
  "archive_id": "archive_batch_1234567890",
  "schema": [{"name": "flight_id", "type": "string"}, ...],
  "storage_location": "hdfs://hdfs-namenode:9000/archive/flight_data/archive_..."
}
```

### 2.7 Staging Layer Design

**Purpose**: Data cleaning, validation, enrichment, and transformation before loading into the data warehouse.

**Processing Steps**:

1. **Data Cleaning**:
   - Handle missing values (defaults or imputation)
   - Validate data types and ranges
   - Remove duplicates based on `flight_id`

2. **Data Validation**:
   - Check for required fields
   - Validate airport codes, airline codes
   - Range checks for delays, distances

3. **Data Enrichment**:
   - Add processing timestamps
   - Extract temporal features (hour, day of week, month)
   - Calculate derived metrics

4. **Data Quality Scoring**:
   - Compute quality score based on completeness, validity
   - Flag records with low quality scores
   - Store scores for downstream filtering

**Implementation**: Spark DataFrame transformations in `spark_processor.py`:
```python
df_enriched = process_and_enrich_data(df_parsed, spark)
```

### 2.8 Machine Learning Inference

**Model**: Pre-trained XGBoost regression model for delay prediction
- Format: Serialized `.pkl` file
- Location: Available to Spark workers via shared volume
- Features: Month, DayOfWeek, Distance, Scheduled Departure Time, Weather factors

**Inference Strategy**:

1. **Model Loading**: Load model once per Spark application (or use broadcast variables for distributed inference)
2. **Feature Extraction**: Extract features from incoming Kafka messages
3. **Prediction**: Generate delay predictions using the model
4. **Confidence Scoring**: Calculate prediction confidence based on feature quality and model output variance
5. **Storage**: Write predictions to both MongoDB (for real-time dashboards) and Hive (for historical analysis)

**Challenges Addressed**:
- **Serialization**: XGBoost models can be large; use efficient serialization
- **Scalability**: Consider Spark MLlib pipelines or MLeap for production scale
- **Versioning**: Track model version in predictions table for A/B testing

### 2.9 Archiving Policy

**Trigger Condition**: MongoDB data size exceeds 300 MB

**Archiving Process**:

1. **Monitoring**: Airflow DAG (`mongodb_archiving_dag`) runs every 5 minutes
2. **Size Check**: Query MongoDB `dbStats` to get current size
3. **Threshold Check**: If size > 300 MB, trigger archiving
4. **Data Selection**: Select records older than threshold date (default: 7 days)
5. **Export**: Spark job reads selected records from MongoDB
6. **Write to HDFS**: Write as Parquet to `hdfs://hdfs-namenode:9000/archive/mongodb_archive/<archive_id>/`
7. **Metadata Update**: Write archive metadata to HDFS and Hive
8. **Cleanup**: Delete archived records from MongoDB

**Design Rationale**:
- **300 MB Threshold**: Balances query performance (larger MongoDB = slower queries) with operational overhead (frequent archiving = more overhead)
- **7-Day Window**: Ensures recent data is always available in MongoDB for real-time dashboards
- **Parquet Format**: Efficient storage and querying of archived data

### 2.10 Caching Layer: Redis

**Purpose**: Cache frequently accessed dimension data and query results to reduce database load.

**Cached Data**:
- Airline codes → Airline names
- Airport codes → Airport details (city, state, coordinates)
- Aggregated query results (TTL: 5 minutes)

**Benefits**:
- Faster dashboard load times
- Reduced MongoDB/Hive query load
- Improved user experience

### 2.11 Orchestration: Apache Airflow

**DAGs**:

1. **`flight_processing_dag`**:
   - Schedule: Every 1 minute
   - Purpose: Process streaming data from Kafka
   - Tasks: Spark job for ingestion, processing, MongoDB write, HDFS write, Hive write

2. **`mongodb_archiving_dag`**:
   - Schedule: Every 5 minutes
   - Purpose: Monitor MongoDB size and archive old data
   - Tasks: Size check → Decision → Spark archiving job

**Design Decisions**:
- **Micro-batch Frequency**: 1-minute intervals balance latency and throughput
- **Separate DAGs**: Separation of concerns (processing vs. archiving)
- **Dependency Management**: Archiving DAG doesn't depend on processing DAG to avoid blocking

### 2.12 Visualization: Streamlit Dashboard

**Architecture**: Multi-page application with 6 comprehensive pages:

1. **Real-Time Overview**:
   - KPIs: Incoming data rate, total records, MongoDB size, system latency
   - Visualizations: Data ingestion rate over time, latest records table

2. **Streaming Analytics**:
   - Time-series charts with configurable time ranges
   - Rolling aggregates (moving averages)
   - Window-based metrics
   - Trend analysis with regression

3. **Historical Analytics**:
   - Daily and hourly aggregations
   - Day-of-week analysis
   - OLAP-style drill-downs (by airline, origin, destination, route)
   - Comparative analytics

4. **Prediction & Insights**:
   - Prediction distribution histograms
   - Actual vs. predicted scatter plots
   - Accuracy metrics (MAE, RMSE, MAPE)
   - Anomaly detection based on prediction errors
   - Confidence-based analysis

5. **Data Quality & Metadata**:
   - Schema information
   - Missing value analysis
   - Data freshness indicators
   - Record counts per batch
   - Archive metadata
   - Overall data quality score

6. **Operations & Monitoring**:
   - Pipeline health indicators
   - Airflow DAG status
   - Spark job execution summaries
   - Archive events timeline
   - Cache hit ratios
   - System resource usage
   - Pipeline health score

**Technical Implementation**:
- **Auto-refresh**: JavaScript-based auto-refresh every 60 seconds
- **Data Sources**: MongoDB (real-time), Hive via Spark SQL (historical)
- **Visualization Library**: Plotly for interactive charts
- **Caching**: Streamlit's `@st.cache_resource` for connection pooling

---

## 3. Data Pipeline Workflow

### 3.1 Real-Time Processing Pipeline

1. **Data Generation**: Producer generates synthetic flight data and publishes to Kafka
2. **Kafka Buffering**: Events are buffered in Kafka topic `flight_events`
3. **Airflow Trigger**: `flight_processing_dag` triggers Spark job every 60 seconds
4. **Spark Processing**:
   - Read from Kafka (latest offsets)
   - Parse JSON and apply schema
   - Staging layer processing (cleaning, validation, enrichment)
   - ML inference (predict delays)
   - Write to MongoDB (fresh data)
   - Write batch to HDFS (raw archival)
   - Write to Hive staging table
   - Write to Hive fact table
   - Write predictions to Hive predictions table
   - Write metadata to HDFS and Hive

### 3.2 Archiving Pipeline

1. **Monitoring**: `mongodb_archiving_dag` runs every 5 minutes
2. **Size Check**: Query MongoDB size
3. **Decision**: If size > 300 MB, proceed with archiving
4. **Data Export**: Spark job reads old records from MongoDB
5. **HDFS Write**: Write to HDFS as Parquet
6. **Metadata Update**: Write archive metadata
7. **Cleanup**: Delete archived records from MongoDB

### 3.3 Dashboard Query Pipeline

1. **User Request**: User accesses Streamlit dashboard page
2. **Data Source Selection**: 
   - Real-time data: Query MongoDB
   - Historical data: Query Hive via Spark SQL
   - Cached data: Check Redis first
3. **Data Processing**: Aggregate and transform data
4. **Visualization**: Render charts and tables
5. **Auto-refresh**: Page refreshes every 60 seconds

---

## 4. Scalability and Performance Considerations

### 4.1 Horizontal Scalability

- **Kafka**: Can be scaled to multiple brokers and partitions
- **Spark**: Worker nodes can be added to Spark cluster
- **MongoDB**: Can be configured as replica set or sharded cluster
- **HDFS**: Datanodes can be added for storage scaling
- **Hive**: Multiple HiveServer2 instances for query load distribution

### 4.2 Performance Optimizations

1. **MongoDB**:
   - Indexes on frequently queried fields
   - TTL indexes for automatic cleanup
   - Connection pooling

2. **Spark**:
   - Partitioning strategies (by date, by airline)
   - Broadcast joins for dimension tables
   - Caching frequently accessed DataFrames

3. **Hive**:
   - Partitioned tables (by date)
   - Bucketing for join optimization
   - Columnar format (Parquet) for analytical queries

4. **Dashboard**:
   - Query result caching (Redis)
   - Limit data retrieval (pagination, time windows)
   - Lazy loading of visualizations

### 4.3 Fault Tolerance

- **Kafka**: Message replication (production: replication factor > 1)
- **Spark**: Checkpointing for streaming jobs
- **MongoDB**: Replica sets for high availability
- **HDFS**: Data replication (default: 3x)
- **Airflow**: Task retries and alerting

---

## 5. Security Considerations

1. **Network Isolation**: Docker network isolation between services
2. **Authentication**: MongoDB authentication (configured in production)
3. **Data Encryption**: TLS for inter-service communication (production)
4. **Access Control**: Airflow role-based access control
5. **Secret Management**: Environment variables for credentials (use secrets manager in production)

---

## 6. Deployment Architecture

### 6.1 Docker Compose Configuration

All services are containerized and orchestrated via Docker Compose:

- **Services**: 17 containers (Zookeeper, Kafka, Spark Master/Worker, Airflow, MongoDB, Redis, HDFS, Hive, Streamlit)
- **Networking**: Bridge network (`bigdata-network`) for service communication
- **Volumes**: Persistent volumes for data (MongoDB, HDFS, Hive metadata)

### 6.2 Service Dependencies

```
Kafka → Zookeeper
Spark Worker → Spark Master
Airflow → Postgres (metadata DB)
Hive → Postgres (Hive Metastore) + HDFS
Streamlit → MongoDB + Redis + Hive
```

### 6.3 Initialization Sequence

1. Infrastructure services (Postgres, Zookeeper, HDFS)
2. Data services (Kafka, MongoDB, Redis, Hive)
3. Processing services (Spark, Airflow)
4. Application services (Producer, Streamlit)

---

## 7. Monitoring and Observability

### 7.1 Metrics

- **Throughput**: Records processed per minute
- **Latency**: End-to-end processing time
- **Storage**: MongoDB size, HDFS usage
- **Quality**: Data quality scores, missing value rates
- **ML**: Prediction accuracy, model performance

### 7.2 Logging

- **Spark**: Spark UI and logs
- **Airflow**: DAG execution logs
- **MongoDB**: MongoDB logs
- **Dashboard**: Streamlit logs

### 7.3 Alerting

- MongoDB size approaching threshold
- DAG failures
- Service health degradation
- Data quality below threshold

---

## 8. Future Enhancements

1. **Real-time ML Model Updates**: Online learning or periodic retraining
2. **Advanced Analytics**: Complex event processing, anomaly detection
3. **Data Lineage**: Track data flow from source to dashboard
4. **Multi-tenant Support**: Isolation for different airlines/organizations
5. **API Layer**: REST API for programmatic access
6. **Stream Processing**: Switch to true streaming (Kafka Streams or Flink) for lower latency

---

## 9. Conclusion

This architecture provides a comprehensive, scalable, and maintainable solution for real-time flight tracking and delay prediction. The separation of hot and cold storage, comprehensive metadata management, and rich visualization capabilities make it suitable for production deployment with appropriate scaling and security configurations.

The design emphasizes:
- **Separation of Concerns**: Clear boundaries between ingestion, processing, storage, and visualization
- **Scalability**: Horizontal scaling capabilities at each layer
- **Fault Tolerance**: Resilience through replication and checkpointing
- **Operational Excellence**: Monitoring, logging, and automated archiving
- **Data Quality**: Staging layer ensures data cleanliness before warehouse loading
- **Analytical Capabilities**: OLAP-style queries and historical analysis support

---

## References

- Apache Kafka Documentation: https://kafka.apache.org/documentation/
- Apache Spark Documentation: https://spark.apache.org/docs/latest/
- MongoDB Documentation: https://docs.mongodb.com/
- Apache Hive Documentation: https://hive.apache.org/
- HDFS Documentation: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
- Apache Airflow Documentation: https://airflow.apache.org/docs/
- Streamlit Documentation: https://docs.streamlit.io/

