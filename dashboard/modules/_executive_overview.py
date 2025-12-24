"""
Executive Overview Page: PowerBI-style dashboard with KPIs, charts, and insights
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import numpy as np

def render(mongodb_client, redis_client):
    # Custom CSS for PowerBI-style cards
    st.markdown("""
    <style>
    .kpi-card {
        background: linear-gradient(135deg, rgba(30, 58, 138, 0.9) 0%, rgba(59, 130, 246, 0.8) 100%);
        border-radius: 16px;
        padding: 24px;
        margin: 8px 0;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        border: 1px solid rgba(255, 255, 255, 0.1);
        backdrop-filter: blur(10px);
    }
    .kpi-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: #ffffff;
        margin: 0;
        text-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }
    .kpi-label {
        font-size: 0.9rem;
        color: rgba(255, 255, 255, 0.8);
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 8px;
    }
    .kpi-trend {
        font-size: 0.85rem;
        margin-top: 12px;
        padding: 4px 12px;
        border-radius: 20px;
        display: inline-block;
    }
    .trend-up { background: rgba(34, 197, 94, 0.3); color: #22c55e; }
    .trend-down { background: rgba(239, 68, 68, 0.3); color: #ef4444; }
    .section-header {
        font-size: 1.3rem;
        font-weight: 600;
        color: #e2e8f0;
        margin: 32px 0 16px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid rgba(59, 130, 246, 0.5);
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.title("📊 Executive Overview")
    st.markdown("Real-time flight operations intelligence dashboard")
    
    if not mongodb_client:
        st.error("MongoDB connection not available")
        return
    
    try:
        db = mongodb_client.flight_tracking
        collection = db.flight_events
        
        # Fetch data
        total_records = collection.count_documents({})
        
        # Today's data
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        today_count = collection.count_documents({"timestamp": {"$gte": today_start}})
        
        # Fetch recent data for analysis
        cursor = collection.find().sort("timestamp", -1).limit(5000)
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            st.warning("No flight data available")
            return
        
        # Convert timestamps
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Calculate KPIs
        avg_delay = df['dep_delay'].mean() if 'dep_delay' in df.columns else 0
        on_time_pct = (df['dep_delay'] <= 15).sum() / len(df) * 100 if 'dep_delay' in df.columns else 0
        
        # Prediction accuracy
        if 'predicted_delay' in df.columns and 'dep_delay' in df.columns:
            df_pred = df.dropna(subset=['predicted_delay', 'dep_delay'])
            if len(df_pred) > 0:
                accuracy = ((df_pred['predicted_delay'] - df_pred['dep_delay']).abs() <= 10).sum() / len(df_pred) * 100
            else:
                accuracy = 0
        else:
            accuracy = 0
        
        # ============== KPI CARDS ==============
        st.markdown('<div class="section-header">📈 Key Performance Indicators</div>', unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="kpi-card">
                <p class="kpi-value">{total_records:,}</p>
                <p class="kpi-label">Total Flights Tracked</p>
                <p class="kpi-trend trend-up">↑ {today_count} today</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            trend_class = "trend-down" if avg_delay > 15 else "trend-up"
            st.markdown(f"""
            <div class="kpi-card" style="background: linear-gradient(135deg, rgba(124, 58, 237, 0.9) 0%, rgba(167, 139, 250, 0.8) 100%);">
                <p class="kpi-value">{avg_delay:.1f} min</p>
                <p class="kpi-label">Average Delay</p>
                <p class="kpi-trend {trend_class}">{'↑ Above target' if avg_delay > 15 else '↓ On target'}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            trend_class = "trend-up" if on_time_pct >= 80 else "trend-down"
            st.markdown(f"""
            <div class="kpi-card" style="background: linear-gradient(135deg, rgba(16, 185, 129, 0.9) 0%, rgba(52, 211, 153, 0.8) 100%);">
                <p class="kpi-value">{on_time_pct:.1f}%</p>
                <p class="kpi-label">On-Time Performance</p>
                <p class="kpi-trend {trend_class}">{'↑ Meeting SLA' if on_time_pct >= 80 else '↓ Below SLA'}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            trend_class = "trend-up" if accuracy >= 70 else "trend-down"
            st.markdown(f"""
            <div class="kpi-card" style="background: linear-gradient(135deg, rgba(245, 158, 11, 0.9) 0%, rgba(251, 191, 36, 0.8) 100%);">
                <p class="kpi-value">{accuracy:.1f}%</p>
                <p class="kpi-label">Prediction Accuracy</p>
                <p class="kpi-trend {trend_class}">±10 min threshold</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # ============== CHARTS ROW 1 ==============
        st.markdown('<div class="section-header">🎯 Delay Analysis</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Delay Categories Donut Chart
            if 'dep_delay' in df.columns:
                df['delay_category'] = pd.cut(
                    df['dep_delay'],
                    bins=[-float('inf'), 0, 15, 30, 60, float('inf')],
                    labels=['Early/On-time', 'Minor (1-15min)', 'Moderate (16-30min)', 'Significant (31-60min)', 'Severe (60+min)']
                )
                category_counts = df['delay_category'].value_counts()
                
                colors = ['#10b981', '#3b82f6', '#f59e0b', '#f97316', '#ef4444']
                
                fig_donut = go.Figure(data=[go.Pie(
                    labels=category_counts.index,
                    values=category_counts.values,
                    hole=0.6,
                    marker=dict(colors=colors),
                    textinfo='percent+label',
                    textposition='outside',
                    textfont=dict(size=11, color='white')
                )])
                
                fig_donut.update_layout(
                    title=dict(text='Delay Distribution', font=dict(size=16, color='white')),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    showlegend=True,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.3,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10)
                    ),
                    height=400,
                    annotations=[dict(
                        text=f'{len(df):,}<br>Flights',
                        x=0.5, y=0.5,
                        font_size=18,
                        font_color='white',
                        showarrow=False
                    )]
                )
                st.plotly_chart(fig_donut, use_container_width=True)
        
        with col2:
            # Top Airlines by Average Delay
            if 'airline_code' in df.columns and 'dep_delay' in df.columns:
                airline_delays = df.groupby('airline_code')['dep_delay'].agg(['mean', 'count']).reset_index()
                airline_delays.columns = ['airline_code', 'avg_delay', 'flight_count']
                airline_delays = airline_delays[airline_delays['flight_count'] >= 5]  # Min 5 flights
                airline_delays = airline_delays.nlargest(10, 'avg_delay')
                
                fig_bar = go.Figure(data=[go.Bar(
                    x=airline_delays['avg_delay'],
                    y=airline_delays['airline_code'],
                    orientation='h',
                    marker=dict(
                        color=airline_delays['avg_delay'],
                        colorscale=[[0, '#3b82f6'], [0.5, '#f59e0b'], [1, '#ef4444']],
                        line=dict(color='rgba(255,255,255,0.3)', width=1)
                    ),
                    text=[f"{x:.1f} min" for x in airline_delays['avg_delay']],
                    textposition='outside',
                    textfont=dict(color='white', size=11)
                )])
                
                fig_bar.update_layout(
                    title=dict(text='Top 10 Airlines by Avg Delay', font=dict(size=16, color='white')),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    xaxis=dict(title='Average Delay (min)', gridcolor='rgba(255,255,255,0.1)', showgrid=True),
                    yaxis=dict(title='', gridcolor='rgba(255,255,255,0.1)', categoryorder='total ascending'),
                    height=400,
                    margin=dict(l=80, r=80)
                )
                st.plotly_chart(fig_bar, use_container_width=True)
        
        # ============== CHARTS ROW 2 ==============
        st.markdown('<div class="section-header">🔮 Prediction Insights</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Predicted vs Actual Scatter
            if 'predicted_delay' in df.columns and 'dep_delay' in df.columns:
                df_sample = df.dropna(subset=['predicted_delay', 'dep_delay']).sample(min(500, len(df)))
                
                fig_scatter = go.Figure()
                
                fig_scatter.add_trace(go.Scatter(
                    x=df_sample['predicted_delay'],
                    y=df_sample['dep_delay'],
                    mode='markers',
                    marker=dict(
                        size=8,
                        color=df_sample['dep_delay'],
                        colorscale='Viridis',
                        opacity=0.7,
                        line=dict(width=1, color='rgba(255,255,255,0.3)')
                    ),
                    name='Flights'
                ))
                
                # Perfect prediction line
                max_val = max(df_sample['predicted_delay'].max(), df_sample['dep_delay'].max())
                min_val = min(df_sample['predicted_delay'].min(), df_sample['dep_delay'].min())
                fig_scatter.add_trace(go.Scatter(
                    x=[min_val, max_val],
                    y=[min_val, max_val],
                    mode='lines',
                    line=dict(color='#ef4444', dash='dash', width=2),
                    name='Perfect Prediction'
                ))
                
                fig_scatter.update_layout(
                    title=dict(text='Predicted vs Actual Delays', font=dict(size=16, color='white')),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    xaxis=dict(title='Predicted Delay (min)', gridcolor='rgba(255,255,255,0.1)', zeroline=False),
                    yaxis=dict(title='Actual Delay (min)', gridcolor='rgba(255,255,255,0.1)', zeroline=False),
                    height=400,
                    showlegend=True,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig_scatter, use_container_width=True)
        
        with col2:
            # Hourly Delay Heatmap
            if 'timestamp' in df.columns and 'dep_delay' in df.columns:
                df['hour'] = df['timestamp'].dt.hour
                df['day_of_week'] = df['timestamp'].dt.day_name()
                
                heatmap_data = df.pivot_table(
                    values='dep_delay',
                    index='day_of_week',
                    columns='hour',
                    aggfunc='mean'
                )
                
                day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                heatmap_data = heatmap_data.reindex([d for d in day_order if d in heatmap_data.index])
                
                fig_heatmap = go.Figure(data=go.Heatmap(
                    z=heatmap_data.values,
                    x=heatmap_data.columns,
                    y=heatmap_data.index,
                    colorscale=[[0, '#1e3a8a'], [0.25, '#3b82f6'], [0.5, '#22c55e'], [0.75, '#f59e0b'], [1, '#ef4444']],
                    colorbar=dict(
                        title=dict(text='Avg Delay', font=dict(color='white')),
                        tickfont=dict(color='white')
                    ),
                    hoverongaps=False
                ))
                
                fig_heatmap.update_layout(
                    title=dict(text='Delay Patterns by Hour & Day', font=dict(size=16, color='white')),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    xaxis=dict(title='Hour of Day', tickmode='linear', dtick=2),
                    yaxis=dict(title=''),
                    height=400
                )
                st.plotly_chart(fig_heatmap, use_container_width=True)
        
        # ============== CHARTS ROW 3 ==============
        st.markdown('<div class="section-header">📍 Geographic & Route Analysis</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top Origins by Delay
            if 'origin' in df.columns and 'dep_delay' in df.columns:
                origin_delays = df.groupby('origin')['dep_delay'].agg(['mean', 'count']).reset_index()
                origin_delays.columns = ['origin', 'avg_delay', 'flight_count']
                origin_delays = origin_delays[origin_delays['flight_count'] >= 3]
                origin_delays = origin_delays.nlargest(15, 'avg_delay')
                
                fig_origin = go.Figure(data=[go.Bar(
                    x=origin_delays['origin'],
                    y=origin_delays['avg_delay'],
                    marker=dict(
                        color=origin_delays['avg_delay'],
                        colorscale='Blues',
                        line=dict(color='rgba(255,255,255,0.3)', width=1)
                    ),
                    text=[f"{x:.0f}" for x in origin_delays['avg_delay']],
                    textposition='outside',
                    textfont=dict(color='white', size=10)
                )])
                
                fig_origin.update_layout(
                    title=dict(text='Airports with Highest Departure Delays', font=dict(size=16, color='white')),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    xaxis=dict(title='Origin Airport', gridcolor='rgba(255,255,255,0.1)', tickangle=45),
                    yaxis=dict(title='Avg Delay (min)', gridcolor='rgba(255,255,255,0.1)'),
                    height=400,
                    margin=dict(b=100)
                )
                st.plotly_chart(fig_origin, use_container_width=True)
        
        with col2:
            # Time Series Trend
            if 'timestamp' in df.columns and 'dep_delay' in df.columns:
                df_hourly = df.groupby(df['timestamp'].dt.floor('H')).agg({
                    'dep_delay': 'mean',
                    'airline_code': 'count'
                }).reset_index()
                df_hourly.columns = ['timestamp', 'avg_delay', 'flight_count']
                df_hourly = df_hourly.tail(48)  # Last 48 hours
                
                fig_trend = make_subplots(specs=[[{"secondary_y": True}]])
                
                fig_trend.add_trace(
                    go.Scatter(
                        x=df_hourly['timestamp'],
                        y=df_hourly['avg_delay'],
                        name='Avg Delay',
                        line=dict(color='#3b82f6', width=3),
                        fill='tozeroy',
                        fillcolor='rgba(59, 130, 246, 0.2)'
                    ),
                    secondary_y=False
                )
                
                fig_trend.add_trace(
                    go.Bar(
                        x=df_hourly['timestamp'],
                        y=df_hourly['flight_count'],
                        name='Flight Count',
                        marker=dict(color='rgba(34, 197, 94, 0.5)'),
                        opacity=0.5
                    ),
                    secondary_y=True
                )
                
                fig_trend.update_layout(
                    title=dict(text='Delay Trend (Last 48 Hours)', font=dict(size=16, color='white')),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    xaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
                    height=400,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                fig_trend.update_yaxes(title_text="Avg Delay (min)", secondary_y=False, gridcolor='rgba(255,255,255,0.1)')
                fig_trend.update_yaxes(title_text="Flight Count", secondary_y=True, showgrid=False)
                
                st.plotly_chart(fig_trend, use_container_width=True)
        
        # ============== AIR TRAFFIC MAP ==============
        st.markdown('<div class="section-header">🗺️ Air Traffic Map - Delays by Airport</div>', unsafe_allow_html=True)
        
        # US Airport coordinates database
        airport_coords = {
            # Major Hubs
            'ATL': (33.6407, -84.4277, 'Atlanta'),
            'LAX': (33.9416, -118.4085, 'Los Angeles'),
            'ORD': (41.9742, -87.9073, 'Chicago O\'Hare'),
            'DFW': (32.8998, -97.0403, 'Dallas/Fort Worth'),
            'DEN': (39.8561, -104.6737, 'Denver'),
            'JFK': (40.6413, -73.7781, 'New York JFK'),
            'SFO': (37.6213, -122.379, 'San Francisco'),
            'SEA': (47.4502, -122.3088, 'Seattle'),
            'LAS': (36.0840, -115.1537, 'Las Vegas'),
            'MCO': (28.4312, -81.3081, 'Orlando'),
            'EWR': (40.6895, -74.1745, 'Newark'),
            'BOS': (42.3656, -71.0096, 'Boston'),
            'PHX': (33.4373, -112.0078, 'Phoenix'),
            'IAH': (29.9902, -95.3368, 'Houston'),
            'MIA': (25.7959, -80.2870, 'Miami'),
            'MSP': (44.8848, -93.2223, 'Minneapolis'),
            'DTW': (42.2124, -83.3534, 'Detroit'),
            'FLL': (26.0742, -80.1506, 'Fort Lauderdale'),
            'PHL': (39.8729, -75.2437, 'Philadelphia'),
            'LGA': (40.7769, -73.8740, 'LaGuardia'),
            'BWI': (39.1774, -76.6684, 'Baltimore'),
            'SLC': (40.7899, -111.9791, 'Salt Lake City'),
            'DCA': (38.8512, -77.0402, 'Washington Reagan'),
            'IAD': (38.9531, -77.4565, 'Washington Dulles'),
            'SAN': (32.7338, -117.1933, 'San Diego'),
            'TPA': (27.9756, -82.5333, 'Tampa'),
            'PDX': (45.5898, -122.5951, 'Portland'),
            'STL': (38.7487, -90.3700, 'St. Louis'),
            'HNL': (21.3187, -157.9225, 'Honolulu'),
            'AUS': (30.1975, -97.6664, 'Austin'),
            'BNA': (36.1263, -86.6774, 'Nashville'),
            'MDW': (41.7868, -87.7522, 'Chicago Midway'),
            'DAL': (32.8481, -96.8512, 'Dallas Love'),
            'HOU': (29.6454, -95.2789, 'Houston Hobby'),
            'RDU': (35.8776, -78.7875, 'Raleigh-Durham'),
            'CLE': (41.4117, -81.8498, 'Cleveland'),
            'SMF': (38.6954, -121.5908, 'Sacramento'),
            'MCI': (39.2976, -94.7139, 'Kansas City'),
            'SJC': (37.3639, -121.9289, 'San Jose'),
            'OAK': (37.7213, -122.2208, 'Oakland'),
            'MSY': (29.9934, -90.2580, 'New Orleans'),
            'SAT': (29.5337, -98.4698, 'San Antonio'),
            'PIT': (40.4915, -80.2329, 'Pittsburgh'),
            'RSW': (26.5362, -81.7552, 'Fort Myers'),
            'IND': (39.7173, -86.2944, 'Indianapolis'),
            'CVG': (39.0489, -84.6678, 'Cincinnati'),
            'CMH': (39.9980, -82.8919, 'Columbus'),
            'JAX': (30.4941, -81.6879, 'Jacksonville'),
        }
        
        if 'origin' in df.columns and 'dep_delay' in df.columns:
            # Aggregate data by origin airport
            airport_stats = df.groupby('origin').agg({
                'dep_delay': ['mean', 'sum', 'count'],
                'airline_code': 'nunique'
            }).reset_index()
            airport_stats.columns = ['airport', 'avg_delay', 'total_delay', 'flight_count', 'airlines']
            
            # Add coordinates
            airport_stats['lat'] = airport_stats['airport'].map(lambda x: airport_coords.get(x, (None, None, None))[0])
            airport_stats['lon'] = airport_stats['airport'].map(lambda x: airport_coords.get(x, (None, None, None))[1])
            airport_stats['city'] = airport_stats['airport'].map(lambda x: airport_coords.get(x, (None, None, x))[2])
            
            # Filter to airports with coordinates
            airport_stats = airport_stats.dropna(subset=['lat', 'lon'])
            
            if len(airport_stats) > 0:
                # Create hover text
                airport_stats['hover_text'] = airport_stats.apply(
                    lambda row: f"<b>{row['airport']} - {row['city']}</b><br>" +
                               f"<b>Total Delay:</b> {row['total_delay']:.0f} min<br>" +
                               f"<b>Avg Delay:</b> {row['avg_delay']:.1f} min<br>" +
                               f"<b>Flights:</b> {row['flight_count']:,}<br>" +
                               f"<b>Airlines:</b> {row['airlines']}",
                    axis=1
                )
                
                # Scale bubble sizes (min 10, max 60)
                max_delay = airport_stats['total_delay'].max()
                min_delay = airport_stats['total_delay'].min()
                airport_stats['bubble_size'] = 10 + (airport_stats['total_delay'] - min_delay) / (max_delay - min_delay + 1) * 50
                
                # Create the map
                fig_map = go.Figure()
                
                fig_map.add_trace(go.Scattergeo(
                    lon=airport_stats['lon'],
                    lat=airport_stats['lat'],
                    text=airport_stats['hover_text'],
                    hoverinfo='text',
                    mode='markers',
                    marker=dict(
                        size=airport_stats['bubble_size'],
                        color=airport_stats['avg_delay'],
                        colorscale=[
                            [0, '#22c55e'],      # Green - low delay
                            [0.25, '#84cc16'],   # Lime
                            [0.5, '#eab308'],    # Yellow
                            [0.75, '#f97316'],   # Orange
                            [1, '#ef4444']       # Red - high delay
                        ],
                        colorbar=dict(
                            title=dict(text='Avg Delay<br>(min)', font=dict(color='white', size=12)),
                            tickfont=dict(color='white'),
                            bgcolor='rgba(0,0,0,0.5)',
                            bordercolor='rgba(255,255,255,0.3)',
                            len=0.6
                        ),
                        opacity=0.85,
                        line=dict(width=2, color='rgba(255,255,255,0.8)'),
                        sizemode='diameter'
                    ),
                    name='Airports'
                ))
                
                fig_map.update_layout(
                    title=dict(
                        text='✈️ US Airport Delays - Click & Drag to Explore | Scroll to Zoom',
                        font=dict(size=16, color='white'),
                        x=0.5,
                        xanchor='center'
                    ),
                    geo=dict(
                        scope='usa',
                        projection_type='albers usa',
                        showland=True,
                        landcolor='rgb(30, 41, 59)',
                        showlakes=True,
                        lakecolor='rgb(15, 23, 42)',
                        showocean=True,
                        oceancolor='rgb(15, 23, 42)',
                        showcoastlines=True,
                        coastlinecolor='rgba(59, 130, 246, 0.5)',
                        showframe=False,
                        bgcolor='rgba(0,0,0,0)',
                        countrycolor='rgba(59, 130, 246, 0.3)',
                        subunitcolor='rgba(59, 130, 246, 0.2)',
                        showsubunits=True
                    ),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    height=550,
                    margin=dict(l=0, r=0, t=50, b=0),
                    dragmode='pan'
                )
                
                # Enable scroll zoom
                config = {
                    'scrollZoom': True,
                    'displayModeBar': True,
                    'modeBarButtonsToAdd': ['pan2d', 'zoomIn2d', 'zoomOut2d', 'resetScale2d'],
                    'displaylogo': False
                }
                
                st.plotly_chart(fig_map, use_container_width=True, config=config)
                
                # Add legend/info
                st.markdown("""
                <div style="text-align: center; padding: 10px; background: rgba(30, 41, 59, 0.5); border-radius: 8px; margin-top: -10px;">
                    <span style="color: #94a3b8; font-size: 0.85rem;">
                        🟢 Low Delay &nbsp;|&nbsp; 🟡 Moderate &nbsp;|&nbsp; 🔴 High Delay &nbsp;|&nbsp; 
                        <b>Bubble Size</b> = Total Delay Minutes
                    </span>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.info("No airports with coordinate data found in the current dataset")

        
        # ============== METRICS ROW ==============
        st.markdown('<div class="section-header">📊 Quick Stats</div>', unsafe_allow_html=True)
        
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            severe_delays = (df['dep_delay'] > 60).sum() if 'dep_delay' in df.columns else 0
            st.metric("🚨 Severe Delays", severe_delays, delta=f"{severe_delays/len(df)*100:.1f}%")
        
        with col2:
            unique_airlines = df['airline_code'].nunique() if 'airline_code' in df.columns else 0
            st.metric("✈️ Airlines", unique_airlines)
        
        with col3:
            unique_origins = df['origin'].nunique() if 'origin' in df.columns else 0
            st.metric("🛫 Origins", unique_origins)
        
        with col4:
            unique_dests = df['dest'].nunique() if 'dest' in df.columns else 0
            st.metric("🛬 Destinations", unique_dests)
        
        with col5:
            max_delay = df['dep_delay'].max() if 'dep_delay' in df.columns else 0
            st.metric("⏰ Max Delay", f"{max_delay:.0f} min")
        
        with col6:
            median_delay = df['dep_delay'].median() if 'dep_delay' in df.columns else 0
            st.metric("📈 Median Delay", f"{median_delay:.1f} min")
            
    except Exception as e:
        st.error(f"Error loading executive overview: {e}")
        import traceback
        st.code(traceback.format_exc())
