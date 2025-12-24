from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

dag = DAG(
    'flight_processing_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1
)

# Spark Configuration
# Networking fixes only. We do NOT need to set spark.master here anymore.
spark_conf = {
    "spark.driver.host": "airflow-scheduler", 
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.ui.port": "4040",
    "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true"
}

spark_job = SparkSubmitOperator(
    task_id='process_flight_data',
    # We use the connection we defined in docker-compose
    conn_id='spark_default', 
    
    application='/opt/airflow/dags/spark_processor.py',
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1',
    conf=spark_conf,
    verbose=True,
    dag=dag
)

spark_job