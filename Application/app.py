import os
import time
import sqlite3
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go

# Initialize the Dash app
app = dash.Dash(__name__)
server = app.server  # for WSGI deployment if needed

# Define the layout of the dashboard
app.layout = html.Div([
    html.H1("Real-Time Sensor Data by Panel"),
    dcc.Interval(
        id="interval-component",
        interval=1000,  # Update every 1 second
        n_intervals=0
    ),
    # This container will hold one graph per panel
    html.Div(id="graphs-container")
])

# Construct the absolute path to the database file.
app_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(app_dir, '..', 'Logging', 'mqtt_data.db')
print("Database path:", db_path)

# (Optional) Test the connection at startup:
try:
    conn = sqlite3.connect(db_path)
    df_test = pd.read_sql_query("SELECT * FROM sensor_data ORDER BY received_at ASC", conn)
    print("Fetched rows at startup:", len(df_test))
except Exception as e:
    print("Error fetching data at startup:", e)
finally:
    conn.close()


def fetch_all_data():
    """
    Fetch only the last 1000 rows from the sensor_data table.
    The query orders by received_at descending so that we get the most recent rows,
    then the DataFrame is re-sorted in ascending order by received_at.
    """
    app_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(app_dir, '..', 'Logging', 'mqtt_data.db')
    max_retries = 3
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(db_path, timeout=10)
            # Get the last 1000 rows (most recent) using descending order,
            # then later we will sort in ascending order.
            query = "SELECT * FROM sensor_data ORDER BY received_at DESC LIMIT 1000"
            df = pd.read_sql_query(query, conn)
            conn.close()
            print("Fetched rows:", len(df))
            if not df.empty:
                # Convert 'received_at' to datetime and then sort in ascending order.
                df['received_at'] = pd.to_datetime(df['received_at'])
                df.sort_values(by='received_at', inplace=True)
            return df
        except sqlite3.OperationalError as e:
            print(f"Attempt {attempt + 1}: Error fetching data: {e}")
            time.sleep(1)  # Wait a second before retrying
    return pd.DataFrame()


@app.callback(
    Output("graphs-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_graphs(n):
    # Retrieve the last 1000 data rows.
    df = fetch_all_data()

    # If no data is available, show a message.
    if df.empty:
        return html.Div("No data available")

    # Define the desired panel order.
    panels_order = ["panelA", "panelB", "panelC", "panelD", "panelE", "panelF"]
    graphs = []

    # List the numeric fields you want to plot.
    numeric_columns = ["flow1", "turbidity", "ph", "tds", "level1", "level2"]

    # Create one graph per panel in the specified order.
    for panel in panels_order:
        df_panel = df[df['panel'] == panel]
        traces = []
        # Add a trace for each numeric column if there is any data.
        for col in numeric_columns:
            if col in df_panel.columns and df_panel[col].notna().any():
                traces.append(go.Scatter(
                    x=df_panel["received_at"],
                    y=df_panel[col],
                    mode='lines+markers',
                    name=col
                ))
        # Create a figure.
        if traces:
            fig = go.Figure(data=traces)
            fig.update_layout(
                title=f"{panel} Data",
                xaxis_title="Received At",
                yaxis_title="Value",
                margin={'l': 50, 'r': 10, 't': 50, 'b': 50}
            )
        else:
            # If no data exists for this panel, display an empty figure with a message.
            fig = go.Figure(layout=go.Layout(
                title=f"{panel} Data - No data available",
                xaxis_title="Received At",
                yaxis_title="Value"
            ))
        graphs.append(
            html.Div([
                html.H2(panel),
                dcc.Graph(figure=fig)
            ], style={'margin-bottom': '50px'})
        )

    return graphs


if __name__ == '__main__':
    app.run_server(debug=True)
