"""
Historical Analytics Page: Long-term trends, daily/hourly aggregations, comparative analytics, OLAP drill-downs
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pymongo

def render(mongodb_client, redis_client):
    st.title(" Historical Analytics")
    st.markdown("Long-term trends and comparative analysis")
    
    if not mongodb_client:
        st.error("MongoDB connection not available")
        return
    
    try:
        db = mongodb_client.flight_tracking
        collection = db.flight_events
        
        # Date range filter
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=datetime.now().date() - timedelta(days=7))
        with col2:
            end_date = st.date_input("End Date", value=datetime.now().date())
        
        # Fetch data
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        query = {
            "timestamp": {
                "$gte": start_datetime,
                "$lte": end_datetime
            }
        }
        cursor = collection.find(query)
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            st.warning("No data available for the selected date range")
            return
        
        # Convert timestamp
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['date'] = df['timestamp'].dt.date
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.day_name()
        
        # Daily aggregations
        st.subheader(" Daily Aggregations")
        
        if 'date' in df.columns and 'dep_delay' in df.columns:
            daily_stats = df.groupby('date').agg({
                'dep_delay': ['mean', 'max', 'min', 'count'],
                'arr_delay': 'mean' if 'arr_delay' in df.columns else 'count'
            }).reset_index()
            daily_stats.columns = ['date', 'avg_dep_delay', 'max_delay', 'min_delay', 'flight_count', 'avg_arr_delay']
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig_daily = px.line(
                    daily_stats,
                    x='date',
                    y='avg_dep_delay',
                    title="Average Daily Departure Delay",
                    labels={"avg_dep_delay": "Avg Delay (minutes)", "date": "Date"}
                )
                st.plotly_chart(fig_daily, use_container_width=True)
            
            with col2:
                fig_count = px.bar(
                    daily_stats,
                    x='date',
                    y='flight_count',
                    title="Daily Flight Count",
                    labels={"flight_count": "Number of Flights", "date": "Date"}
                )
                st.plotly_chart(fig_count, use_container_width=True)
        
        # Hourly aggregations
        st.subheader(" Hourly Aggregations")
        
        if 'hour' in df.columns and 'dep_delay' in df.columns:
            hourly_stats = df.groupby('hour').agg({
                'dep_delay': 'mean',
                'flight_id': 'count' if 'flight_id' in df.columns else 'dep_delay'
            }).reset_index()
            hourly_stats.columns = ['hour', 'avg_delay', 'flight_count']
            
            fig_hourly = px.bar(
                hourly_stats,
                x='hour',
                y='avg_delay',
                title="Average Delay by Hour of Day",
                labels={"avg_delay": "Avg Delay (minutes)", "hour": "Hour"}
            )
            st.plotly_chart(fig_hourly, use_container_width=True)
        
        # Day of week analysis
        st.subheader(" Day of Week Analysis")
        
        if 'day_of_week' in df.columns and 'dep_delay' in df.columns:
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            day_stats = df.groupby('day_of_week')['dep_delay'].mean().reindex(day_order, fill_value=0).reset_index()
            day_stats.columns = ['day_of_week', 'avg_delay']
            
            fig_day = px.bar(
                day_stats,
                x='day_of_week',
                y='avg_delay',
                title="Average Delay by Day of Week",
                labels={"avg_delay": "Avg Delay (minutes)", "day_of_week": "Day"}
            )
            st.plotly_chart(fig_day, use_container_width=True)
        
        # OLAP-style drill-down
        st.subheader(" OLAP Drill-Down Analysis")
        
        drill_level = st.selectbox("Drill Level", ["Airline", "Origin", "Destination", "Route"])
        
        if drill_level == "Airline" and 'airline_code' in df.columns:
            airline_stats = df.groupby('airline_code').agg({
                'dep_delay': ['mean', 'count'],
                'arr_delay': 'mean' if 'arr_delay' in df.columns else 'count'
            }).reset_index()
            airline_stats.columns = ['airline_code', 'avg_delay', 'flight_count', 'avg_arr_delay']
            airline_stats = airline_stats.sort_values('avg_delay', ascending=False)
            
            st.dataframe(airline_stats, use_container_width=True)
            
            fig_airline = px.bar(
                airline_stats.head(10),
                x='airline_code',
                y='avg_delay',
                title="Top 10 Airlines by Average Delay"
            )
            st.plotly_chart(fig_airline, use_container_width=True)
        
        elif drill_level == "Origin" and 'origin' in df.columns:
            origin_stats = df.groupby('origin').agg({
                'dep_delay': ['mean', 'count']
            }).reset_index()
            origin_stats.columns = ['origin', 'avg_delay', 'flight_count']
            origin_stats = origin_stats.sort_values('avg_delay', ascending=False)
            
            st.dataframe(origin_stats.head(20), use_container_width=True)
        
        elif drill_level == "Destination" and 'dest' in df.columns:
            dest_stats = df.groupby('dest').agg({
                'arr_delay': ['mean', 'count'] if 'arr_delay' in df.columns else ('dep_delay', ['mean', 'count'])
            }).reset_index()
            dest_stats.columns = ['destination', 'avg_delay', 'flight_count']
            dest_stats = dest_stats.sort_values('avg_delay', ascending=False)
            
            st.dataframe(dest_stats.head(20), use_container_width=True)
        
        elif drill_level == "Route" and 'origin' in df.columns and 'dest' in df.columns:
            df['route'] = df['origin'] + ' → ' + df['dest']
            route_stats = df.groupby('route').agg({
                'dep_delay': ['mean', 'count']
            }).reset_index()
            route_stats.columns = ['route', 'avg_delay', 'flight_count']
            route_stats = route_stats[route_stats['flight_count'] >= 5]  # Filter routes with at least 5 flights
            route_stats = route_stats.sort_values('avg_delay', ascending=False)
            
            st.dataframe(route_stats.head(20), use_container_width=True)
        
    except Exception as e:
        st.error(f"Error loading historical analytics: {e}")

