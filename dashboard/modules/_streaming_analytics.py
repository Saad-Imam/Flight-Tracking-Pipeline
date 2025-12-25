"""
Streaming Analytics Page: Time-series charts, rolling aggregates, window-based metrics, trends
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pymongo

def render(mongodb_client, redis_client):
    st.title(" Streaming Analytics")
    st.markdown("Real-time analytics and time-series visualizations")
    
    if not mongodb_client:
        st.error("MongoDB connection not available")
        return
    
    try:
        db = mongodb_client.flight_tracking
        collection = db.flight_events
        
        # Time range filter
        col1, col2 = st.columns(2)
        with col1:
            time_range = st.selectbox("Time Range", ["Last 1 hour", "Last 6 hours", "Last 24 hours", "All time"])
        with col2:
            group_by = st.selectbox("Group By", ["Minute", "5 Minutes", "15 Minutes", "Hour"])
        
        # Calculate time window
        if time_range == "Last 1 hour":
            start_time = datetime.now() - timedelta(hours=1)
        elif time_range == "Last 6 hours":
            start_time = datetime.now() - timedelta(hours=6)
        elif time_range == "Last 24 hours":
            start_time = datetime.now() - timedelta(days=1)
        else:
            start_time = datetime(2020, 1, 1)
        
        # Fetch data
        query = {"timestamp": {"$gte": start_time}}
        cursor = collection.find(query)
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            st.warning("No data available for the selected time range")
            return
        
        # Convert timestamp
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
        
        # Time-series: Average Delay Over Time
        st.subheader("️ Average Departure Delay Over Time")
        
        if group_by == "Minute":
            df_grouped = df.groupby(df['timestamp'].dt.floor('1T')).agg({
                'dep_delay': 'mean',
                'flight_id': 'count'
            }).reset_index()
            df_grouped.columns = ['timestamp', 'avg_delay', 'count']
        elif group_by == "5 Minutes":
            df_grouped = df.groupby(df['timestamp'].dt.floor('5T')).agg({
                'dep_delay': 'mean',
                'flight_id': 'count'
            }).reset_index()
            df_grouped.columns = ['timestamp', 'avg_delay', 'count']
        elif group_by == "15 Minutes":
            df_grouped = df.groupby(df['timestamp'].dt.floor('15T')).agg({
                'dep_delay': 'mean',
                'flight_id': 'count'
            }).reset_index()
            df_grouped.columns = ['timestamp', 'avg_delay', 'count']
        else:  # Hour
            df_grouped = df.groupby(df['timestamp'].dt.floor('H')).agg({
                'dep_delay': 'mean',
                'flight_id': 'count'
            }).reset_index()
            df_grouped.columns = ['timestamp', 'avg_delay', 'count']
        
        fig = px.line(
            df_grouped,
            x='timestamp',
            y='avg_delay',
            title="Average Departure Delay",
            labels={"avg_delay": "Delay (minutes)", "timestamp": "Time"}
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Rolling aggregates
        st.subheader(" Rolling Aggregates")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Rolling average
            window_size = st.slider("Rolling Window Size", 5, 50, 10)
            if 'dep_delay' in df.columns:
                df['rolling_avg'] = df['dep_delay'].rolling(window=window_size, min_periods=1).mean()
                fig_rolling = px.line(
                    df.tail(100),
                    x='timestamp' if 'timestamp' in df.columns else df.index,
                    y='rolling_avg',
                    title=f"Rolling Average (Window: {window_size})"
                )
                st.plotly_chart(fig_rolling, use_container_width=True)
        
        with col2:
            # Delay distribution
            if 'dep_delay' in df.columns:
                fig_dist = px.histogram(
                    df,
                    x='dep_delay',
                    nbins=30,
                    title="Delay Distribution",
                    labels={"dep_delay": "Delay (minutes)", "count": "Frequency"}
                )
                st.plotly_chart(fig_dist, use_container_width=True)
        
        # Window-based metrics
        st.subheader(" Window-Based Metrics")
        
        col1, col2, col3 = st.columns(3)
        
        if 'dep_delay' in df.columns:
            with col1:
                st.metric("Current Window Avg", f"{df['dep_delay'].tail(100).mean():.2f} min")
            with col2:
                st.metric("Current Window Max", f"{df['dep_delay'].tail(100).max():.2f} min")
            with col3:
                st.metric("Current Window Min", f"{df['dep_delay'].tail(100).min():.2f} min")
        
        # Trend analysis
        st.subheader(" Trend Analysis")
        
        if 'timestamp' in df.columns and 'dep_delay' in df.columns:
            # Linear trend
            df_trend = df.copy()
            df_trend['time_numeric'] = (df_trend['timestamp'] - df_trend['timestamp'].min()).dt.total_seconds()
            
            fig_trend = px.scatter(
                df_trend,
                x='timestamp',
                y='dep_delay',
                trendline="ols",
                title="Delay Trend with Regression Line"
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error loading streaming analytics: {e}")

