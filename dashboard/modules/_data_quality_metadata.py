"""
Data Quality & Metadata Page: Schema evolution, missing values, record counts, archive metadata, data freshness
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pymongo
import json
import numpy as np
import os

def get_hive_metadata():
    """Connect to Hive metastore PostgreSQL backend and get archive metadata"""
    try:
        import psycopg2
        
        hive_host = os.getenv("HIVE_HOST", "postgres-hive")
        conn = psycopg2.connect(
            host=hive_host,
            port=5432,
            database="metastore",
            user="hive",
            password="hive",
            connect_timeout=5
        )
        cursor = conn.cursor()
        
        # Get total number of tables (archives)
        cursor.execute("SELECT COUNT(*) FROM \"TBLS\"")
        total_archives = cursor.fetchone()[0]
        
        # Get table parameters for size info
        cursor.execute("""
            SELECT COALESCE(SUM(CAST(p.\"PARAM_VALUE\" AS BIGINT)), 0) 
            FROM \"TABLE_PARAMS\" p 
            WHERE p.\"PARAM_KEY\" IN ('totalSize', 'rawDataSize')
        """)
        total_size_bytes = cursor.fetchone()[0] or 0
        total_size_mb = total_size_bytes / (1024 * 1024)
        
        # Get total record count from table params
        cursor.execute("""
            SELECT COALESCE(SUM(CAST(p.\"PARAM_VALUE\" AS BIGINT)), 0)
            FROM \"TABLE_PARAMS\" p 
            WHERE p.\"PARAM_KEY\" = 'numRows'
        """)
        total_records = cursor.fetchone()[0] or 0
        
        # Get latest partition/table creation time
        cursor.execute("""
            SELECT MAX(t.\"CREATE_TIME\") 
            FROM \"TBLS\" t
        """)
        latest_create_time = cursor.fetchone()[0]
        if latest_create_time:
            latest_date = datetime.fromtimestamp(latest_create_time).strftime("%Y-%m-%d %H:%M:%S")
        else:
            latest_date = "No tables found"
        
        cursor.close()
        conn.close()
        
        return {
            "total_archives": str(total_archives),
            "total_archived_records": f"{total_records:,}",
            "total_archive_size_mb": f"{total_size_mb:.2f} MB",
            "latest_archive_date": latest_date
        }
    except ImportError:
        return None
    except Exception as e:
        return {"error": str(e)}

def render(mongodb_client, redis_client):
    st.title(" Data Quality & Metadata")
    st.markdown("Data quality metrics, schema information, and metadata management")
    
    if not mongodb_client:
        st.error("MongoDB connection not available")
        return
    
    try:
        db = mongodb_client.flight_tracking
        collection = db.flight_events
        
        # Schema Information
        st.subheader(" Schema Information")
        
        # Get a sample document to infer schema
        sample_doc = collection.find_one()
        if sample_doc:
            schema_info = {}
            for key, value in sample_doc.items():
                if key != '_id':
                    schema_info[key] = type(value).__name__
            
            schema_df = pd.DataFrame([
                {"Field": k, "Type": v, "Description": ""} 
                for k, v in schema_info.items()
            ])
            st.dataframe(schema_df, use_container_width=True)
        
        # Data Quality Metrics
        st.subheader(" Data Quality Metrics")
        
        # Fetch sample data for analysis
        cursor = collection.find().limit(10000)
        df = pd.DataFrame(list(cursor))
        
        if not df.empty:
            # Missing values analysis
            missing_data = df.isnull().sum()
            missing_percent = (missing_data / len(df)) * 100
            quality_df = pd.DataFrame({
                'Field': missing_data.index,
                'Missing Count': missing_data.values,
                'Missing Percentage': missing_percent.values
            })
            quality_df = quality_df[quality_df['Missing Count'] > 0].sort_values('Missing Count', ascending=False)
            
            if not quality_df.empty:
                st.write("**Missing Values Analysis**")
                st.dataframe(quality_df, use_container_width=True)
                
                # Visualize missing values
                fig_missing = px.bar(
                    quality_df,
                    x='Field',
                    y='Missing Percentage',
                    title="Missing Values by Field (%)"
                )
                st.plotly_chart(fig_missing, use_container_width=True)
            else:
                st.success(" No missing values detected")
            
            # Data freshness
            st.subheader(" Data Freshness")
            
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                latest_timestamp = df['timestamp'].max()
                oldest_timestamp = df['timestamp'].min()
                age_hours = (datetime.now() - latest_timestamp).total_seconds() / 3600
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Latest Record", latest_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
                with col2:
                    st.metric("Oldest Record", oldest_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
                with col3:
                    st.metric("Data Age", f"{age_hours:.2f} hours")
            
            # Record counts per batch
            st.subheader(" Record Counts")
            
            if 'ingestion_timestamp' in df.columns:
                df['ingestion_timestamp'] = pd.to_datetime(df['ingestion_timestamp'])
                df['ingestion_date'] = df['ingestion_timestamp'].dt.date
                
                batch_counts = df.groupby('ingestion_date').size().reset_index()
                batch_counts.columns = ['date', 'record_count']
                
                fig_counts = px.bar(
                    batch_counts,
                    x='date',
                    y='record_count',
                    title="Record Counts per Batch/Date"
                )
                st.plotly_chart(fig_counts, use_container_width=True)
        
        # Archive Metadata
        st.subheader(" Archive Metadata")
        
        # Try to get archive information from Hive
        hive_metadata = get_hive_metadata()
        
        if hive_metadata is None:
            st.warning("⚠️ psycopg2 library not available. Install it to connect to Hive metastore.")
            archive_metadata = {
                "total_archives": "N/A (psycopg2 required)",
                "total_archived_records": "N/A",
                "total_archive_size_mb": "N/A",
                "latest_archive_date": "N/A"
            }
        elif "error" in hive_metadata:
            st.warning(f"⚠️ Could not connect to Hive metastore: {hive_metadata['error']}")
            archive_metadata = {
                "total_archives": "N/A (Connection error)",
                "total_archived_records": "N/A",
                "total_archive_size_mb": "N/A",
                "latest_archive_date": "N/A"
            }
        else:
            st.success("✅ Connected to Hive metastore successfully")
            archive_metadata = hive_metadata
        
        archive_df = pd.DataFrame([
            {"Metric": k.replace("_", " ").title(), "Value": v} 
            for k, v in archive_metadata.items()
        ])
        st.dataframe(archive_df, use_container_width=True, hide_index=True)
        
        # Data Quality Score
        st.subheader(" Overall Data Quality Score")
        
        if not df.empty:
            # Calculate quality score based on multiple factors
            quality_score = 100.0
            
            # Deduct for missing values
            if not quality_df.empty:
                total_missing_pct = quality_df['Missing Percentage'].sum()
                quality_score -= min(total_missing_pct * 2, 50)  # Max 50 points deduction
            
            # Deduct for data age
            if 'timestamp' in df.columns:
                if age_hours > 24:
                    quality_score -= min((age_hours - 24) / 24 * 10, 30)  # Max 30 points deduction
            
            # Deduct for duplicate records
            if 'flight_id' in df.columns:
                duplicates = df['flight_id'].duplicated().sum()
                dup_pct = (duplicates / len(df)) * 100
                quality_score -= min(dup_pct, 20)  # Max 20 points deduction
            
            quality_score = max(quality_score, 0)  # Ensure non-negative
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Quality score gauge
                fig_gauge = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=quality_score,
                    domain={'x': [0, 1], 'y': [0, 1]},
                    title={'text': "Data Quality Score"},
                    gauge={
                        'axis': {'range': [None, 100]},
                        'bar': {'color': "darkblue"},
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
                fig_gauge.update_layout(height=300)
                st.plotly_chart(fig_gauge, use_container_width=True)
            
            with col2:
                st.metric("Quality Score", f"{quality_score:.1f}/100")
                
                if quality_score >= 90:
                    st.success(" Excellent data quality")
                elif quality_score >= 70:
                    st.info("️ Good data quality with room for improvement")
                else:
                    st.error(" Data quality needs attention")
        
        # Field Statistics
        st.subheader(" Field Statistics")
        
        if not df.empty:
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            if numeric_cols:
                stats_df = df[numeric_cols].describe().T
                st.dataframe(stats_df, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error loading data quality information: {e}")

