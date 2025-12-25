"""
Comprehensive Streamlit Dashboard for Flight Tracking Pipeline
7 Pages: Executive Overview, Real-Time Overview, Streaming Analytics, Historical Analytics, 
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

# Import page modules from modules folder (not pages folder to avoid Streamlit auto-detection)
import modules._executive_overview as executive_overview
import modules._real_time_overview as real_time_overview
import modules._streaming_analytics as streaming_analytics
import modules._historical_analytics as historical_analytics
import modules._prediction_insights as prediction_insights
import modules._data_quality_metadata as data_quality_metadata
import modules._operations_monitoring as operations_monitoring

# Page configuration
st.set_page_config(
    page_title="Flight Tracking Analytics Dashboard",
    page_icon="️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Premium Dark Theme CSS
st.markdown("""
<style>
/* Main background and theme */
.stApp {
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
}

/* Sidebar styling */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
    border-right: 1px solid rgba(59, 130, 246, 0.3);
}

[data-testid="stSidebar"] .stMarkdown h1 {
    background: linear-gradient(90deg, #3b82f6, #8b5cf6);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-weight: 700;
}

/* Headers */
h1 {
    color: #f1f5f9 !important;
    font-weight: 700 !important;
    letter-spacing: -0.5px;
}

h2, h3, h4 {
    color: #e2e8f0 !important;
    font-weight: 600 !important;
}

/* Selectbox styling */
[data-testid="stSelectbox"] > div > div {
    background: linear-gradient(135deg, rgba(30, 58, 138, 0.6) 0%, rgba(59, 130, 246, 0.4) 100%);
    border: 1px solid rgba(59, 130, 246, 0.5);
    border-radius: 12px;
    color: white;
}

[data-testid="stSelectbox"] label {
    color: #94a3b8 !important;
    font-weight: 500 !important;
}

/* Metric cards styling */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.9) 0%, rgba(51, 65, 85, 0.7) 100%);
    border: 1px solid rgba(59, 130, 246, 0.3);
    border-radius: 16px;
    padding: 20px;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    backdrop-filter: blur(10px);
}

[data-testid="stMetric"] label {
    color: #94a3b8 !important;
    font-size: 0.9rem !important;
    font-weight: 500 !important;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #f1f5f9 !important;
    font-size: 1.8rem !important;
    font-weight: 700 !important;
}

[data-testid="stMetric"] [data-testid="stMetricDelta"] {
    font-size: 0.85rem !important;
}

/* DataFrame styling */
[data-testid="stDataFrame"] {
    background: rgba(30, 41, 59, 0.8);
    border-radius: 12px;
    border: 1px solid rgba(59, 130, 246, 0.2);
}

/* Expander styling */
.streamlit-expanderHeader {
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.9) 0%, rgba(51, 65, 85, 0.7) 100%);
    border-radius: 12px;
    border: 1px solid rgba(59, 130, 246, 0.3);
    color: #e2e8f0 !important;
}

/* Success/Error/Info/Warning boxes */
.stSuccess {
    background: linear-gradient(135deg, rgba(34, 197, 94, 0.2) 0%, rgba(22, 163, 74, 0.1) 100%);
    border: 1px solid rgba(34, 197, 94, 0.4);
    border-radius: 12px;
}

.stError {
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.2) 0%, rgba(220, 38, 38, 0.1) 100%);
    border: 1px solid rgba(239, 68, 68, 0.4);
    border-radius: 12px;
}

.stInfo {
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.2) 0%, rgba(37, 99, 235, 0.1) 100%);
    border: 1px solid rgba(59, 130, 246, 0.4);
    border-radius: 12px;
}

.stWarning {
    background: linear-gradient(135deg, rgba(245, 158, 11, 0.2) 0%, rgba(217, 119, 6, 0.1) 100%);
    border: 1px solid rgba(245, 158, 11, 0.4);
    border-radius: 12px;
}

/* Slider styling */
[data-testid="stSlider"] > div > div {
    background: rgba(59, 130, 246, 0.3);
}

/* Text */
p, span, li {
    color: #cbd5e1;
}

/* Divider */
hr {
    border-color: rgba(59, 130, 246, 0.3) !important;
}

/* Button styling */
.stButton > button {
    background: linear-gradient(135deg, #3b82f6 0%, #8b5cf6 100%);
    color: white;
    border: none;
    border-radius: 12px;
    padding: 12px 24px;
    font-weight: 600;
    transition: all 0.3s ease;
    box-shadow: 0 4px 15px rgba(59, 130, 246, 0.3);
}

.stButton > button:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 20px rgba(59, 130, 246, 0.4);
}

/* Date input styling */
[data-testid="stDateInput"] > div > div {
    background: rgba(30, 41, 59, 0.8);
    border: 1px solid rgba(59, 130, 246, 0.3);
    border-radius: 12px;
}

/* Charts container */
[data-testid="stPlotlyChart"] {
    background: rgba(30, 41, 59, 0.5);
    border-radius: 16px;
    padding: 10px;
    border: 1px solid rgba(59, 130, 246, 0.15);
}

/* Hide Streamlit branding */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}

/* Scrollbar styling */
::-webkit-scrollbar {
    width: 8px;
    height: 8px;
}

::-webkit-scrollbar-track {
    background: #1e293b;
}

::-webkit-scrollbar-thumb {
    background: linear-gradient(180deg, #3b82f6, #8b5cf6);
    border-radius: 4px;
}

::-webkit-scrollbar-thumb:hover {
    background: linear-gradient(180deg, #60a5fa, #a78bfa);
}

/* Animation for page load */
@keyframes fadeIn {
    from { opacity: 0; transform: translateY(10px); }
    to { opacity: 1; transform: translateY(0); }
}

.main .block-container {
    animation: fadeIn 0.5s ease-out;
}
</style>
""", unsafe_allow_html=True)

# LOGO FIRST - At the very top of sidebar
st.sidebar.markdown("""
<div style="text-align: center; padding: 20px 0 10px 0;">
    <h1 style="font-size: 1.8rem; margin: 0;">️ ✈️ Flight Tracker</h1>
    <p style="color: #64748b; font-size: 0.85rem; margin-top: 5px;">Analytics Dashboard</p>
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown("---")

# Navigation dropdown
page = st.sidebar.selectbox(
    " Navigate to:",
    [
        "Executive Overview",
        "Real-Time Overview",
        "Streaming Analytics",
        "Historical Analytics",
        "Prediction & Insights",
        "Data Quality & Metadata",
        "Operations & Monitoring"
    ],
    index=0
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
if page == "Executive Overview":
    executive_overview.render(mongodb_client, redis_client)
elif page == "Real-Time Overview":
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


# Footer with connection status removed as per user request


# Last updated timestamp
st.sidebar.markdown("---")
st.sidebar.markdown(f"""
<div style="text-align: center; padding: 10px; background: rgba(30, 41, 59, 0.5); border-radius: 8px;">
    <p style="color: #64748b; font-size: 0.75rem; margin: 0;">Last Updated (PKT)</p>
    <p style="color: #94a3b8; font-size: 0.85rem; margin: 5px 0 0 0; font-weight: 500;">{(datetime.utcnow() + timedelta(hours=5)).strftime('%H:%M:%S')}</p>
</div>
""", unsafe_allow_html=True)

# Auto-refresh script (every 60 seconds)
st.markdown("""
<script>
    setTimeout(function(){
        window.location.reload(1);
    }, 60000);
</script>
""", unsafe_allow_html=True)
