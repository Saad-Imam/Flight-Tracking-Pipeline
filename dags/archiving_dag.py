"""
Archiving DAG: Monitors MongoDB size and archives old data to HDFS when > 300 MB
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import pymongo
from pymongo import MongoClient
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_mongodb_size(**context):
    """Check MongoDB database size in MB"""
    try:
        client = MongoClient("mongodb://mongodb:27017/")
        db = client.flight_tracking
        
        # Get database stats
        stats = db.command("dbStats")
        size_mb = stats.get("dataSize", 0) / (1024 * 1024)
        
        print(f"Current MongoDB size: {size_mb:.2f} MB")
        
        # Push size to XCom for downstream tasks
        context['ti'].xcom_push(key='mongodb_size_mb', value=size_mb)
        context['ti'].xcom_push(key='needs_archiving', value=size_mb > 300)
        
        client.close()
        return size_mb
    except Exception as e:
        print(f"Error checking MongoDB size: {e}")
        raise

def archive_old_data(**context):
    """Archive old data from MongoDB to HDFS using Spark"""
    try:
        # Pull MongoDB size from XCom
        ti = context['ti']
        size_mb = ti.xcom_pull(key='mongodb_size_mb', task_ids='check_mongodb_size')
        needs_archiving = ti.xcom_pull(key='needs_archiving', task_ids='check_mongodb_size')
        
        if not needs_archiving:
            print(f"MongoDB size ({size_mb:.2f} MB) is below 300 MB threshold. No archiving needed.")
            return
        
        print(f"MongoDB size ({size_mb:.2f} MB) exceeds 300 MB. Initiating archiving...")
        
        # The actual archiving will be done by the Spark job
        return True
    except Exception as e:
        print(f"Error in archive decision: {e}")
        raise

# Define the DAG
dag = DAG(
    'mongodb_archiving_dag',
    default_args=default_args,
    description='Monitor MongoDB and archive old data to HDFS',
    schedule_interval=timedelta(minutes=5),  # Check every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=['archiving', 'mongodb', 'hdfs']
)

# Task 1: Check MongoDB size
check_size_task = PythonOperator(
    task_id='check_mongodb_size',
    python_callable=check_mongodb_size,
    dag=dag
)

# Task 2: Decision task
archive_decision_task = PythonOperator(
    task_id='archive_decision',
    python_callable=archive_old_data,
    dag=dag
)

# Task 3: Spark job to archive data
spark_conf = {
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.ui.port": "4041",
}

archive_spark_job = SparkSubmitOperator(
    task_id='archive_to_hdfs',
    conn_id='spark_default',
    application='/opt/airflow/dags/archive_processor.py',
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1',
    conf=spark_conf,
    verbose=True,
    dag=dag
)

# Set task dependencies
check_size_task >> archive_decision_task >> archive_spark_job

