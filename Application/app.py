import os
import time
import sqlite3
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go

# Initialize the Dash app
app = dash.Dash(__name__)
server = app.server  # for WSGI deployment if needed

# Layout: Left sidebar with filters and panel selectors; right side with two vertically stacked graphs.
app.layout = html.Div([
    html.H1("Real-Time Sensor Data Comparison"),
    dcc.Interval(
        id="interval-component",
        interval=1000,  # Update every 1 second
        n_intervals=0
    ),
    html.Div([
        # Left sidebar: Filters and two panel selection dropdowns.
        html.Div([
            html.H2("Date Filter"),
            dcc.DatePickerRange(
                id='date-picker-range',
                start_date_placeholder_text="Start Date",
                end_date_placeholder_text="End Date",
                display_format='YYYY-MM-DD'
            ),
            html.Br(), html.Br(),
            html.H2("Relative Time Filter"),
            dcc.Dropdown(
                id="relative-filter",
                options=[
                    {"label": "None", "value": "None"},
                    {"label": "Last 5 minutes", "value": "5"},
                    {"label": "Last 10 minutes", "value": "10"},
                    {"label": "Last 30 minutes", "value": "30"},
                    {"label": "Last 1 hour", "value": "60"},
                    {"label": "Last 12 hours", "value": "720"},
                    {"label": "Last 1 day", "value": "1440"}
                ],
                value="None",
                clearable=False,
                style={'width': '100%'}
            ),
            html.Br(), html.Br(),
            html.H2("Select Panel (Top Plot)"),
            dcc.Dropdown(
                id="panel-selector-top",
                options=[
                    {"label": "Panel A", "value": "panelA"},
                    {"label": "Panel B", "value": "panelB"},
                    {"label": "Panel C", "value": "panelC"},
                    {"label": "Panel D", "value": "panelD"},
                    {"label": "Panel E", "value": "panelE"},
                    {"label": "Panel F", "value": "panelF"}
                ],
                value="panelA",
                clearable=False,
                style={'width': '100%'}
            ),
            html.Br(), html.Br(),
            html.H2("Select Panel (Bottom Plot)"),
            dcc.Dropdown(
                id="panel-selector-bottom",
                options=[
                    {"label": "Panel A", "value": "panelA"},
                    {"label": "Panel B", "value": "panelB"},
                    {"label": "Panel C", "value": "panelC"},
                    {"label": "Panel D", "value": "panelD"},
                    {"label": "Panel E", "value": "panelE"},
                    {"label": "Panel F", "value": "panelF"}
                ],
                value="panelB",
                clearable=False,
                style={'width': '100%'}
            )
        ], style={
            'width': '20%',
            'padding': '10px',
            'backgroundColor': '#f8f9fa',
            'boxShadow': '2px 2px 5px rgba(0,0,0,0.1)',
            'height': '900px',
            'overflowY': 'auto'
        }),
        
        # Right side: Pause/Resume control and vertical stack of two graphs.
        html.Div([
            html.Div([
                html.Button("Pause", id="toggle-button", n_clicks=0),
                dcc.Store(id="update-state", data=True)
            ], style={'margin-bottom': '20px'}),
            html.Div(id="graph-container")
        ], style={'width': '80%', 'padding': '10px'})
    ], style={'display': 'flex'})
])

# Construct the absolute path to the database file.
app_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(app_dir, '..', 'Logging', 'mqtt_data.db')
print("Database path:", db_path)

# (Optional) Test the connection at startup.
try:
    conn = sqlite3.connect(db_path)
    df_test = pd.read_sql_query("SELECT * FROM sensor_data ORDER BY received_at ASC", conn)
    if not df_test.empty:
        df_test['received_at'] = pd.to_datetime(df_test['received_at'])
        # Convert timestamps from UTC to UTC+7 (Asia/Bangkok)
        df_test['received_at'] = df_test['received_at'].dt.tz_localize('UTC').dt.tz_convert('Asia/Bangkok')
    print("Fetched rows at startup:", len(df_test))
except Exception as e:
    print("Error fetching data at startup:", e)
finally:
    conn.close()

def fetch_all_data():
    """
    Fetch the last 1000 rows from the sensor_data table.
    Convert the 'received_at' timestamps from UTC to UTC+7.
    """
    app_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(app_dir, '..', 'Logging', 'mqtt_data.db')
    max_retries = 3
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(db_path, timeout=10)
            query = "SELECT * FROM sensor_data ORDER BY received_at DESC LIMIT 1000"
            df = pd.read_sql_query(query, conn)
            conn.close()
            print("Fetched rows:", len(df))
            if not df.empty:
                df['received_at'] = pd.to_datetime(df['received_at'])
                # Convert database timestamps (assumed UTC) to UTC+7.
                df['received_at'] = df['received_at'].dt.tz_localize('UTC').dt.tz_convert('Asia/Bangkok')
                df.sort_values(by='received_at', inplace=True)
            return df
        except sqlite3.OperationalError as e:
            print(f"Attempt {attempt + 1}: Error fetching data: {e}")
            time.sleep(1)
    return pd.DataFrame()

# Callback to update both graphs based on the filters and selected panels.
@app.callback(
    Output("graph-container", "children"),
    [Input("interval-component", "n_intervals"),
     Input("date-picker-range", "start_date"),
     Input("date-picker-range", "end_date"),
     Input("relative-filter", "value"),
     Input("panel-selector-top", "value"),
     Input("panel-selector-bottom", "value")]
)
def update_graphs(n, start_date, end_date, relative_filter, panel_top, panel_bottom):
    # Retrieve the last 1000 rows.
    df = fetch_all_data()

    # Apply relative time filter if selected.
    if relative_filter != "None":
        now = pd.Timestamp.now(tz='Asia/Bangkok')
        delta_minutes = int(relative_filter)
        start_date_dt = now - pd.Timedelta(minutes=delta_minutes)
        df = df[(df['received_at'] >= start_date_dt) & (df['received_at'] <= now)]
    else:
        # Otherwise, use custom date picker values if provided.
        if start_date:
            start_date_dt = pd.to_datetime(start_date).tz_localize('Asia/Bangkok')
            df = df[df['received_at'] >= start_date_dt]
        if end_date:
            end_date_dt = pd.to_datetime(end_date).tz_localize('Asia/Bangkok')
            df = df[df['received_at'] <= end_date_dt]

    # Filter data for top and bottom panels.
    df_top = df[df['panel'] == panel_top]
    df_bottom = df[df['panel'] == panel_bottom]

    # Define numeric columns to plot.
    numeric_columns = ["flow1", "turbidity", "ph", "tds", "level1", "level2"]

    def create_fig(df_panel, panel_name):
        traces = []
        for col in numeric_columns:
            if col in df_panel.columns and df_panel[col].notna().any():
                traces.append(go.Scatter(
                    x=df_panel["received_at"],
                    y=df_panel[col],
                    mode='lines+markers',
                    name=col
                ))
        if traces:
            fig = go.Figure(data=traces)
            fig.update_layout(
                title=f"{panel_name} Data",
                xaxis_title="Received At",
                yaxis_title="Value",
                margin={'l': 50, 'r': 10, 't': 50, 'b': 50}
            )
        else:
            fig = go.Figure(layout=go.Layout(
                title=f"{panel_name} Data - No data available",
                xaxis_title="Received At",
                yaxis_title="Value"
            ))
        return fig

    # Create figures for the top and bottom panels.
    fig_top = create_fig(df_top, panel_top)
    fig_bottom = create_fig(df_bottom, panel_bottom)

    # Return two vertically stacked graphs.
    return html.Div([
        dcc.Graph(figure=fig_top),
        dcc.Graph(figure=fig_bottom)
    ])

# Callback to toggle live update state (pause/resume).
@app.callback(
    Output("update-state", "data"),
    [Input("toggle-button", "n_clicks")],
    [State("update-state", "data")]
)
def toggle_update(n_clicks, current_state):
    if n_clicks is None:
        return current_state
    return not current_state

# Callback to update the Interval component and button text based on update state.
@app.callback(
    [Output("interval-component", "disabled"),
     Output("toggle-button", "children")],
    [Input("update-state", "data")]
)
def update_interval_and_button(update_state):
    if update_state:
        return False, "Pause"
    else:
        return True, "Resume"

if __name__ == '__main__':
    app.run_server(debug=True)
