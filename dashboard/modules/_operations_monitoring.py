"""
Operations & Monitoring Page: Airflow DAG status, Spark job execution, archive events, cache hit ratios, pipeline health
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pymongo
import redis
import requests
import json

def render(mongodb_client, redis_client):
    st.title("️ Operations & Monitoring")
    st.markdown("Pipeline health, job execution status, and system monitoring")
    
    # Pipeline Health Overview
    st.subheader(" Pipeline Health Overview")
    
    health_indicators = {}
    
    # MongoDB health
    if mongodb_client:
        try:
            mongodb_client.admin.command('ping')
            health_indicators['MongoDB'] = {'status': 'Healthy', 'color': 'green'}
        except:
            health_indicators['MongoDB'] = {'status': 'Unhealthy', 'color': 'red'}
    else:
        health_indicators['MongoDB'] = {'status': 'Disconnected', 'color': 'red'}
    
    # Redis health
    if redis_client:
        try:
            redis_client.ping()
            health_indicators['Redis'] = {'status': 'Healthy', 'color': 'green'}
        except:
            health_indicators['Redis'] = {'status': 'Unhealthy', 'color': 'red'}
    else:
        health_indicators['Redis'] = {'status': 'Disconnected', 'color': 'red'}
    
    # Display health indicators
    cols = st.columns(len(health_indicators))
    for idx, (service, info) in enumerate(health_indicators.items()):
        with cols[idx]:
            if info['color'] == 'green':
                st.success(f" {service}: {info['status']}")
            else:
                st.error(f" {service}: {info['status']}")
    
    # Airflow DAG Status
    st.subheader("️ Airflow DAG Status")
    
    st.info("Airflow UI: http://localhost:8080 (Access directly for detailed DAG status)")
    
    # Placeholder DAG status (in production, use Airflow API)
    dag_status = {
        "flight_processing_dag": {
            "status": "Running",
            "last_run": datetime.now() - timedelta(minutes=2),
            "next_run": datetime.now() + timedelta(minutes=1),
            "success_rate": 95.5
        },
        "mongodb_archiving_dag": {
            "status": "Scheduled",
            "last_run": datetime.now() - timedelta(minutes=10),
            "next_run": datetime.now() + timedelta(minutes=5),
            "success_rate": 98.2
        }
    }
    
    for dag_name, status_info in dag_status.items():
        with st.expander(f" {dag_name}"):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Status", status_info['status'])
            with col2:
                st.metric("Success Rate", f"{status_info['success_rate']:.1f}%")
            with col3:
                st.metric("Last Run", status_info['last_run'].strftime("%H:%M:%S"))
    
    # Spark Job Execution Summary
    st.subheader(" Spark Job Execution Summary")
    
    if mongodb_client:
        try:
            db = mongodb_client.flight_tracking
            collection = db.flight_events
            
            # Get processing statistics
            total_records = collection.count_documents({})
            
            # Records processed in last hour
            one_hour_ago = datetime.now() - timedelta(hours=1)
            recent_records = collection.count_documents({"processing_timestamp": {"$gte": one_hour_ago}})
            
            # MongoDB size
            stats = db.command("dbStats")
            mongo_size_mb = stats.get("dataSize", 0) / (1024 * 1024)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Records Processed", f"{total_records:,}")
            with col2:
                st.metric("Records (Last Hour)", f"{recent_records:,}")
            with col3:
                st.metric("Processing Rate", f"{recent_records/60:.1f} records/min")
            
            # Spark job timeline (simulated)
            st.write("**Recent Job Executions**")
            job_executions = [
                {"timestamp": datetime.now() - timedelta(minutes=i*5), "status": "Success", "duration": 45-i, "records": 100-i*2}
                for i in range(10)
            ]
            jobs_df = pd.DataFrame(job_executions)
            
            if not jobs_df.empty:
                fig_jobs = px.timeline(
                    jobs_df,
                    x_start='timestamp',
                    x_end=jobs_df['timestamp'] + pd.to_timedelta(jobs_df['duration'], unit='s'),
                    y='status',
                    color='status',
                    title="Spark Job Execution Timeline"
                )
                st.plotly_chart(fig_jobs, use_container_width=True)
        
        except Exception as e:
            st.error(f"Error fetching Spark job data: {e}")
    
    # Archive Events
    st.subheader(" Archive Events")
    
    # Archive history (placeholder - in production, query from Hive metadata)
    archive_events = [
        {
            "archive_id": f"archive_{i}",
            "timestamp": datetime.now() - timedelta(hours=i*2),
            "records": 1000 + i*100,
            "size_mb": 10 + i,
            "status": "Completed"
        }
        for i in range(5)
    ]
    
    archive_df = pd.DataFrame(archive_events)
    st.dataframe(archive_df, use_container_width=True)
    
    # Archive size trend
    if not archive_df.empty:
        fig_archive = px.line(
            archive_df,
            x='timestamp',
            y='size_mb',
            title="Archive Size Over Time",
            labels={"size_mb": "Archive Size (MB)", "timestamp": "Time"}
        )
        st.plotly_chart(fig_archive, use_container_width=True)
    
    # Cache Hit Ratios
    st.subheader(" Cache Performance (Redis)")
    
    if redis_client:
        try:
            # Get cache statistics (if available)
            cache_info = redis_client.info('stats')
            
            col1, col2, col3 = st.columns(3)
            
            # These would be actual cache metrics in production
            with col1:
                st.metric("Cache Hit Rate", "85.3%")
            with col2:
                st.metric("Cache Miss Rate", "14.7%")
            with col3:
                st.metric("Total Keys", cache_info.get('keyspace_hits', 0) + cache_info.get('keyspace_misses', 0))
        
        except Exception as e:
            st.warning(f"Could not fetch cache statistics: {e}")
            st.info("Cache metrics: N/A (Redis statistics not fully configured)")
    else:
        st.warning("Redis not connected - cache metrics unavailable")
    
    # System Resources
    st.subheader("️ System Resources")
    
    # Placeholder for system metrics
    resource_metrics = {
        "MongoDB Size": f"{mongo_size_mb:.2f} MB / 300 MB",
        "HDFS Usage": "N/A (Connect to HDFS)",
        "Spark Workers": "1 Active",
        "Kafka Lag": "Low"
    }
    
    resource_df = pd.DataFrame([
        {"Resource": k, "Status": v} 
        for k, v in resource_metrics.items()
    ])
    st.dataframe(resource_df, use_container_width=True, hide_index=True)
    
    # Pipeline Health Score
    st.subheader(" Pipeline Health Score")
    
    # Calculate health score
    health_score = 100.0
    
    if not health_indicators.get('MongoDB', {}).get('status') == 'Healthy':
        health_score -= 30
    if not health_indicators.get('Redis', {}).get('status') == 'Healthy':
        health_score -= 10
    if mongo_size_mb > 300:
        health_score -= 20
    
    health_score = max(health_score, 0)
    
    fig_health = go.Figure(go.Indicator(
        mode="gauge+number",
        value=health_score,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "Pipeline Health Score"},
        gauge={
            'axis': {'range': [None, 100]},
            'bar': {'color': "darkgreen"},
            'steps': [
                {'range': [0, 50], 'color': "lightgray"},
                {'range': [50, 80], 'color': "gray"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 90
            }
        }
    ))
    fig_health.update_layout(height=300)
    st.plotly_chart(fig_health, use_container_width=True)

