# Initialize Hive metadata tables for schema, timestamps, record counts, archive IDs, storage locations

from pyspark.sql import SparkSession

def init_metadata_tables():
    """Initialize metadata tables in Hive"""
    
    spark = SparkSession.builder \
        .appName("MetadataInitializer") \
        .config("spark.sql.warehouse.dir", "hdfs://hdfs-namenode:9000/user/hive/warehouse") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS flight_dw")
        spark.sql("USE flight_dw")
        
        # Schema metadata table
        spark.sql("""
            CREATE TABLE IF NOT EXISTS flight_dw.schema_metadata (
                schema_id STRING,
                schema_version STRING,
                table_name STRING,
                schema_definition STRING,
                created_timestamp TIMESTAMP,
                is_active BOOLEAN
            ) USING DELTA
        """)
        
        # Batch metadata table
        spark.sql("""
            CREATE TABLE IF NOT EXISTS flight_dw.batch_metadata (
                batch_id STRING,
                timestamp TIMESTAMP,
                record_count BIGINT,
                file_size_mb DOUBLE,
                archive_id STRING,
                storage_location STRING,
                schema_id STRING,
                processing_status STRING
            ) USING DELTA
        """)
        
        # Archive metadata table
        spark.sql("""
            CREATE TABLE IF NOT EXISTS flight_dw.archive_metadata (
                archive_id STRING,
                timestamp TIMESTAMP,
                source STRING,
                record_count BIGINT,
                storage_location STRING,
                mongodb_size_before_mb DOUBLE,
                schema_definition STRING,
                threshold_date TIMESTAMP
            ) USING DELTA
        """)
        
        print("Metadata tables initialized successfully")
        
    except Exception as e:
        print(f"Error initializing metadata tables: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    init_metadata_tables()

