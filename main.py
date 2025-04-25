from fastapi import FastAPI, HTTPException # type: ignore
from fastapi.responses import HTMLResponse # type: ignore
import plotly.express as px # type: ignore
import pandas as pd
import os
import psycopg2
from functools import lru_cache
from datetime import datetime, timedelta

app = FastAPI()
# connection to supabase postgres db
conn = psycopg2.connect(
    host=os.getenv('SESSION_POOLER_HOST'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASS'),
    sslmode = "require",
    port=os.getenv('DB_PORT')
)

COLOR_MAP = {
    'solar': '#FFD700',
    'grid': '#2ca02c',  
    'wind': '#1f77b4',
}

DEFAULT_TIME_RANGE = 7

#database connections cache
@lru_cache(maxsize=32)
def get_db_data(org_id, days = DEFAULT_TIME_RANGE):
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    
    query = """
        SELECT 
            ec.start_time, 
            ec.energy_kwh, 
            ec.source, 
            ec.cost,
            o.name as org_name
        FROM energy_consumption ec
        JOIN organizations o ON ec.org_id = o.id
        WHERE ec.org_id = %s
            AND ec.start_time BETWEEN %s AND %s
        ORDER BY ec.start_time
    """
    
    try:
        df = pd.read_sql(
            query, 
            conn,
            params=(org_id, start_time, end_time),
            parse_dates=['start_time']
        )
        
        if not df.empty:
            df = df.set_index('start_time')
            numeric_cols = ['energy_kwh', 'cost']
            categorical_cols = ['source', 'org_name']
            # resampling for missing data
            numeric_df = df[numeric_cols].resample('h').mean()
            categorical_df = df[categorical_cols].resample('h').ffill()
            
            df = pd.concat([numeric_df, categorical_df], axis=1).reset_index()
            df = df.ffill()
            
            # Ensure org_name consistency
            df['org_name'] = df['org_name'].mode()[0]
            
        return df
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

def create_figure_template():
    return {
        'layout': {
            'plot_bgcolor': '#f8f9fa',
            'paper_bgcolor': '#ffffff',
            'hovermode': 'x unified',
            'xaxis': {
                'title': 'Time',
                'gridcolor': '#e9ecef',
                'rangeselector': {
                    'buttons': [
                        {'count': 1, 'label': '1d', 'step': 'day'},
                        {'count': 7, 'label': '1w', 'step': 'day'},
                        {'step': 'all'}
                    ]
                },
                'rangeslider': {'visible': True}
            },
            'margin': {'t': 40}
        }
    }
@app.get('/viz/energy/{org_id}', response_class=HTMLResponse)
async def energy_consumption(org_id):
    try:
        df = get_db_data(org_id)
        print("Dataframe shape:", df.head())
        fig = px.line(
            df,
            x="start_time",
            y="energy_kwh",
            color="source",
            color_discrete_map=COLOR_MAP,
            title = f"Energy Consumption - {df['org_name'][0] if not df.empty else {org_id}}",
            labels={'energy_kwh': 'Energy (kWh)'},
            template=create_figure_template()
        )
        
        fig.update_traces(hovertemplate="%{x|%Y-%m-%d %H:%M}<br>%{y} kWh")
        return fig.to_html(include_plotlyjs='cdn', full_html=False)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
# cost visualization
@app.get('/viz/cost/{org_id}', response_class=HTMLResponse)
async def energy_cost(org_id):
    try:
        df = get_db_data(org_id)
        # aggregating daily costs
        daily = df.groupby(
            [pd.Grouper(key='start_time', freq='D'), 'source']
        )['cost'].sum().reset_index()
        fig = px.bar(
            daily,
            x="start_time",
            y="cost",
            color="source",
            color_discrete_map=COLOR_MAP,
            title = f"Energy Consumption - {df['org_name'][0] if not df.empty else {org_id}}",
            labels={'cost': 'Total Cost (USD)'},
            barmode='stack',
            template=create_figure_template()
        )
        fig.update_traces(hovertemplate="%{x|%Y-%m-%d}<br>$%{y:.2f}<extra></extra>")
        return fig.to_html(include_plotlyjs='cdn', full_html=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
# summary visualization
@app.get('/viz/summary/{org_id}', response_class=HTMLResponse)
async def energy_summary(org_id: int):
    try:
        df = get_db_data(org_id)
        fig = px.scatter(
            df,
            x="start_time",
            y="energy_kwh",
            color="source",
            size="cost",
            color_discrete_map=COLOR_MAP,
            title = f"Energy Consumption - {df['org_name'][0] if not df.empty else {org_id}}",
            labels={'energy_kwh': 'Energy (kWh)', 'cost': 'Cost'},
            template=create_figure_template()
        )
        fig.update_layout(
            hoverlabel=dict(bgcolor="white"),
            xaxis=dict(title="Time"),
            yaxis=dict(title="Energy Consumption (kWh)")
        )
        
        return fig.to_html(include_plotlyjs='cdn', full_html=False)     
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))