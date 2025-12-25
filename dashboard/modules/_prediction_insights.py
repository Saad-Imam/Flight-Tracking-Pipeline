"""
Prediction & Insights Page: Model outputs, prediction distributions, confidence indicators, anomaly scores, actual vs predicted
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pymongo
import numpy as np

def render(mongodb_client, redis_client):
    st.title(" Prediction & Insights")
    st.markdown("Machine learning predictions and model performance")
    
    if not mongodb_client:
        st.error("MongoDB connection not available")
        return
    
    try:
        db = mongodb_client.flight_tracking
        collection = db.flight_events
        
        # Fetch data with predictions
        query = {"predicted_delay": {"$exists": True}}
        cursor = collection.find(query).sort("timestamp", -1).limit(1000)
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            st.warning("No prediction data available")
            return
        
        # Convert timestamps
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Prediction Distribution
        st.subheader(" Prediction Distribution")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if 'predicted_delay' in df.columns:
                fig_dist = px.histogram(
                    df,
                    x='predicted_delay',
                    nbins=30,
                    title="Distribution of Predicted Delays",
                    labels={"predicted_delay": "Predicted Delay (minutes)", "count": "Frequency"}
                )
                st.plotly_chart(fig_dist, use_container_width=True)
        
        with col2:
            if 'prediction_confidence' in df.columns:
                fig_conf = px.histogram(
                    df,
                    x='prediction_confidence',
                    nbins=20,
                    title="Prediction Confidence Distribution",
                    labels={"prediction_confidence": "Confidence Score", "count": "Frequency"}
                )
                st.plotly_chart(fig_conf, use_container_width=True)
        
        # Actual vs Predicted
        st.subheader("️ Actual vs Predicted Delays")
        
        if 'predicted_delay' in df.columns and 'dep_delay' in df.columns:
            # Scatter plot
            fig_scatter = px.scatter(
                df,
                x='predicted_delay',
                y='dep_delay',
                title="Predicted vs Actual Delays",
                labels={"predicted_delay": "Predicted Delay (minutes)", "dep_delay": "Actual Delay (minutes)"},
                trendline="ols"
            )
            
            # Add diagonal line for perfect predictions
            max_val = max(df[['predicted_delay', 'dep_delay']].max())
            fig_scatter.add_shape(
                type="line",
                x0=0, y0=0, x1=max_val, y1=max_val,
                line=dict(color="red", dash="dash"),
                name="Perfect Prediction"
            )
            
            st.plotly_chart(fig_scatter, use_container_width=True)
            
            # Calculate accuracy metrics
            df['error'] = abs(df['predicted_delay'] - df['dep_delay'])
            df['squared_error'] = (df['predicted_delay'] - df['dep_delay'])**2
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                mae = df['error'].mean()
                st.metric("Mean Absolute Error", f"{mae:.2f} min")
            
            with col2:
                rmse = np.sqrt(df['squared_error'].mean())
                st.metric("RMSE", f"{rmse:.2f} min")
            
            with col3:
                mape = (df['error'] / (df['dep_delay'].abs() + 1)).mean() * 100
                st.metric("MAPE", f"{mape:.2f}%")
            
            with col4:
                # Accuracy within 10 minutes
                accuracy_10min = (df['error'] <= 10).sum() / len(df) * 100
                st.metric("Accuracy (±10min)", f"{accuracy_10min:.1f}%")
        
        # Prediction Accuracy Over Time
        st.subheader(" Prediction Accuracy Over Time")
        
        if 'timestamp' in df.columns and 'error' in df.columns:
            df['date'] = df['timestamp'].dt.date
            accuracy_over_time = df.groupby('date').agg({
                'error': 'mean'
            }).reset_index()
            accuracy_over_time.columns = ['date', 'avg_error']
            
            fig_acc = px.line(
                accuracy_over_time,
                x='date',
                y='avg_error',
                title="Average Prediction Error Over Time",
                labels={"avg_error": "Average Error (minutes)", "date": "Date"}
            )
            st.plotly_chart(fig_acc, use_container_width=True)
        
        # Anomaly Detection
        st.subheader(" Anomaly Detection")
        
        if 'predicted_delay' in df.columns and 'dep_delay' in df.columns:
            # Calculate anomaly score (deviation from prediction)
            df['anomaly_score'] = abs(df['predicted_delay'] - df['dep_delay']) / (df['predicted_delay'].abs() + 1)
            
            # Threshold for anomalies
            threshold = st.slider("Anomaly Threshold", 0.0, 2.0, 1.0)
            anomalies = df[df['anomaly_score'] > threshold]
            
            st.metric("Anomalies Detected", len(anomalies), delta=f"Threshold: {threshold}")
            
            if len(anomalies) > 0:
                fig_anomaly = px.scatter(
                    anomalies.head(100),
                    x='timestamp' if 'timestamp' in anomalies.columns else anomalies.index,
                    y='anomaly_score',
                    color='dep_delay',
                    title="Anomaly Scores Over Time",
                    labels={"anomaly_score": "Anomaly Score", "dep_delay": "Actual Delay"}
                )
                st.plotly_chart(fig_anomaly, use_container_width=True)
                
                # Show anomaly details
                st.subheader("Anomaly Details")
                display_cols = ['timestamp', 'airline_code', 'origin', 'dest', 'predicted_delay', 'dep_delay', 'anomaly_score']
                available_cols = [col for col in display_cols if col in anomalies.columns]
                st.dataframe(anomalies[available_cols].head(50), use_container_width=True)
        
        # Confidence Analysis
        st.subheader(" Confidence-Based Analysis")
        
        if 'prediction_confidence' in df.columns:
            confidence_threshold = st.slider("Confidence Threshold", 0.0, 1.0, 0.7)
            high_confidence = df[df['prediction_confidence'] >= confidence_threshold]
            low_confidence = df[df['prediction_confidence'] < confidence_threshold]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("High Confidence Predictions", len(high_confidence))
                if len(high_confidence) > 0 and 'error' in df.columns:
                    high_conf_error = abs(high_confidence['predicted_delay'] - high_confidence['dep_delay']).mean()
                    st.metric("High Conf Avg Error", f"{high_conf_error:.2f} min")
            
            with col2:
                st.metric("Low Confidence Predictions", len(low_confidence))
                if len(low_confidence) > 0 and 'error' in df.columns:
                    low_conf_error = abs(low_confidence['predicted_delay'] - low_confidence['dep_delay']).mean()
                    st.metric("Low Conf Avg Error", f"{low_conf_error:.2f} min")
        
    except Exception as e:
        st.error(f"Error loading prediction insights: {e}")

