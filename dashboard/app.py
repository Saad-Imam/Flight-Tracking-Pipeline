"""
Comprehensive Streamlit Dashboard for Flight Tracking Pipeline
6 Pages: Real-Time Overview, Streaming Analytics, Historical Analytics, 
         Prediction & Insights, Data Quality & Metadata, Operations & Monitoring
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pymongo
from pymongo import MongoClient
import redis
import time
import os
import pages.real_time_overview as real_time_overview
import pages.streaming_analytics as streaming_analytics
import pages.historical_analytics as historical_analytics
import pages.prediction_insights as prediction_insights
import pages.data_quality_metadata as data_quality_metadata
import pages.operations_monitoring as operations_monitoring

# Page configuration
st.set_page_config(
    page_title="Flight Tracking Analytics Dashboard",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Auto-refresh every minute
st_autorefresh = st.empty()
st_autorefresh.success(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
st_autorefresh.info("🔄 Auto-refresh: Every 60 seconds")

# Sidebar navigation
st.sidebar.title("✈️ Flight Tracking Dashboard")
st.sidebar.markdown("---")

page = st.sidebar.selectbox(
    "Navigate to:",
    [
        "Real-Time Overview",
        "Streaming Analytics",
        "Historical Analytics",
        "Prediction & Insights",
        "Data Quality & Metadata",
        "Operations & Monitoring"
    ]
)

# Connection utilities
@st.cache_resource
def get_mongodb_client():
    """Get MongoDB client connection"""
    try:
        uri = os.getenv("MONGODB_URI", "mongodb://mongodb:27017/")
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')  # Test connection
        return client
    except Exception as e:
        st.error(f"MongoDB connection error: {e}")
        return None

@st.cache_resource
def get_redis_client():
    """Get Redis client connection"""
    try:
        host = os.getenv("REDIS_HOST", "redis")
        port = int(os.getenv("REDIS_PORT", 6379))
        r = redis.Redis(host=host, port=port, decode_responses=True)
        r.ping()  # Test connection
        return r
    except Exception as e:
        st.error(f"Redis connection error: {e}")
        return None

# Initialize connections
mongodb_client = get_mongodb_client()
redis_client = get_redis_client()

# Route to selected page
if page == "Real-Time Overview":
    real_time_overview.render(mongodb_client, redis_client)
elif page == "Streaming Analytics":
    streaming_analytics.render(mongodb_client, redis_client)
elif page == "Historical Analytics":
    historical_analytics.render(mongodb_client, redis_client)
elif page == "Prediction & Insights":
    prediction_insights.render(mongodb_client, redis_client)
elif page == "Data Quality & Metadata":
    data_quality_metadata.render(mongodb_client, redis_client)
elif page == "Operations & Monitoring":
    operations_monitoring.render(mongodb_client, redis_client)

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("**System Status**")
if mongodb_client:
    st.sidebar.success("✅ MongoDB Connected")
else:
    st.sidebar.error("❌ MongoDB Disconnected")

if redis_client:
    st.sidebar.success("✅ Redis Connected")
else:
    st.sidebar.error("❌ Redis Disconnected")

# Auto-refresh script
st.markdown("""
<script>
    setTimeout(function(){
        window.location.reload(1);
    }, 60000);
</script>
""", unsafe_allow_html=True)

