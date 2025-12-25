"""
Real-Time Overview Page: Live KPIs, incoming data rate, MongoDB size, archive size, system latency
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pymongo

def render(mongodb_client, redis_client):
    st.title(" Real-Time Overview")
    st.markdown("Live system metrics and KPIs")
    
    if not mongodb_client:
        st.error("MongoDB connection not available")
        return
    
    try:
        db = mongodb_client.flight_tracking
        collection = db.flight_events
        
        # Calculate metrics
        total_records = collection.count_documents({})
        
        # Last 1 minute records
        one_min_ago = datetime.now() - timedelta(minutes=1)
        recent_records = collection.count_documents({"ingestion_timestamp": {"$gte": one_min_ago}})
        
        # MongoDB size estimation
        stats = db.command("dbStats")
        mongo_size_mb = stats.get("dataSize", 0) / (1024 * 1024)
        
        # Get latest record timestamp
        latest_record = collection.find_one(sort=[("timestamp", -1)])
        latest_timestamp = latest_record.get("timestamp", datetime.now()) if latest_record else datetime.now()
        latency_seconds = (datetime.now() - latest_timestamp).total_seconds() if isinstance(latest_timestamp, datetime) else 0
        
        # KPI Cards
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label=" Incoming Data Rate",
                value=f"{recent_records} records/min",
                delta=f"{recent_records} last minute"
            )
        
        with col2:
            st.metric(
                label=" Total Records",
                value=f"{total_records:,}",
                delta="MongoDB"
            )
        
        with col3:
            st.metric(
                label=" MongoDB Size",
                value=f"{mongo_size_mb:.2f} MB",
                delta=f"{300 - mongo_size_mb:.2f} MB to threshold" if mongo_size_mb < 300 else "️ Over threshold"
            )
        
        with col4:
            st.metric(
                label=" System Latency",
                value=f"{latency_seconds:.1f}s",
                delta="Last record"
            )
        
        # Time series of incoming data
        st.subheader(" Data Ingestion Rate (Last 10 Minutes)")
        time_windows = []
        record_counts = []
        
        for i in range(10, 0, -1):
            window_start = datetime.now() - timedelta(minutes=i)
            window_end = datetime.now() - timedelta(minutes=i-1)
            count = collection.count_documents({
                "ingestion_timestamp": {
                    "$gte": window_start,
                    "$lt": window_end
                }
            })
            time_windows.append(window_start.strftime("%H:%M:%S"))
            record_counts.append(count)
        
        fig = px.line(
            x=time_windows,
            y=record_counts,
            labels={"x": "Time", "y": "Records per minute"},
            title="Records Per Minute"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Latest records table
        st.subheader(" Latest Flight Events")
        latest_records = list(collection.find().sort("timestamp", -1).limit(10))
        
        if latest_records:
            df_latest = pd.DataFrame(latest_records)
            # Select relevant columns
            display_cols = ["timestamp", "airline_code", "origin", "dest", "dep_delay", "predicted_delay"]
            available_cols = [col for col in display_cols if col in df_latest.columns]
            st.dataframe(df_latest[available_cols], use_container_width=True)
        else:
            st.info("No records available")
            
    except Exception as e:
        st.error(f"Error loading data: {e}")

