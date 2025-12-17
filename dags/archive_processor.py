"""
Spark job to archive old data from MongoDB to HDFS
"""
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_sub
import pymongo
from pymongo import MongoClient

def archive_mongodb_to_hdfs():
    """Archive old MongoDB data to HDFS"""
    
    spark = SparkSession.builder \
        .appName("MongoDBArchiver") \
        .config("spark.jars.packages", 
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Connect to MongoDB
        client = MongoClient("mongodb://mongodb:27017/")
        db = client.flight_tracking
        collection = db.flight_events
        
        # Get current size
        stats = db.command("dbStats")
        size_mb = stats.get("dataSize", 0) / (1024 * 1024)
        print(f"Current MongoDB size: {size_mb:.2f} MB")
        
        if size_mb <= 300:
            print("MongoDB size is below threshold. No archiving needed.")
            client.close()
            spark.stop()
            return
        
        # Calculate threshold date (keep last 7 days, archive older)
        threshold_date = datetime.now() - timedelta(days=7)
        
        # Find old records
        old_records_query = {"timestamp": {"$lt": threshold_date}}
        old_records_count = collection.count_documents(old_records_query)
        
        if old_records_count == 0:
            print("No old records to archive.")
            client.close()
            spark.stop()
            return
        
        print(f"Found {old_records_count} records to archive (older than {threshold_date})")
        
        # Read old records from MongoDB using Spark
        df = spark.read.format("mongo") \
            .option("uri", "mongodb://mongodb:27017/flight_tracking.flight_events") \
            .option("pipeline", json.dumps([{"$match": old_records_query}])) \
            .load()
        
        if df.count() == 0:
            print("No records found in Spark DataFrame")
            client.close()
            spark.stop()
            return
        
        # Create archive ID
        archive_id = f"archive_{int(datetime.now().timestamp())}"
        hdfs_path = f"hdfs://hdfs-namenode:9000/archive/mongodb_archive/{archive_id}"
        
        # Write to HDFS as Parquet
        df.write.mode("overwrite").parquet(hdfs_path)
        
        # Create metadata
        record_count = df.count()
        metadata = {
            "archive_id": archive_id,
            "timestamp": datetime.now().isoformat(),
            "source": "mongodb",
            "threshold_date": threshold_date.isoformat(),
            "record_count": record_count,
            "storage_location": hdfs_path,
            "mongodb_size_before_mb": size_mb,
            "schema": [{"name": f.name, "type": str(f.dataType)} for f in df.schema.fields]
        }
        
        # Write metadata to HDFS
        metadata_path = f"hdfs://hdfs-namenode:9000/metadata/archives/{archive_id}_metadata.json"
        metadata_json = json.dumps(metadata)
        spark.sparkContext.parallelize([metadata_json]).saveAsTextFile(metadata_path)
        
        print(f"Archived {record_count} records to {hdfs_path}")
        print(f"Metadata written to {metadata_path}")
        
        # Delete archived records from MongoDB
        result = collection.delete_many(old_records_query)
        print(f"Deleted {result.deleted_count} records from MongoDB")
        
        # Update Hive metadata table if it exists
        try:
            spark.sql("USE flight_dw")
            spark.sql(f"""
                INSERT INTO TABLE flight_dw.archive_metadata
                VALUES ('{archive_id}', '{metadata['timestamp']}', 'mongodb', 
                        {record_count}, '{hdfs_path}', {size_mb}, 
                        '{json.dumps(metadata['schema'])}')
            """)
        except Exception as e:
            print(f"Could not update Hive metadata table (may not exist): {e}")
        
        client.close()
        spark.stop()
        
    except Exception as e:
        print(f"Error in archiving process: {e}")
        spark.stop()
        raise

if __name__ == "__main__":
    archive_mongodb_to_hdfs()

