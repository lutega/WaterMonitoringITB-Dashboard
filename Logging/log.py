import json
import sqlite3
import paho.mqtt.client as mqtt

# === Database Setup ===
# Open (or create) a SQLite database file.
# Note: check_same_thread=False allows access from multiple threads if needed.
conn = sqlite3.connect('mqtt_data.db', check_same_thread=False)
cursor = conn.cursor()

# Create a table with separate columns for each expected field.
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sensor_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT,
        panel TEXT,
        mqtt_timestamp INTEGER,
        flow1 REAL,
        turbidity REAL,
        ph REAL,
        tds REAL,
        level1 INTEGER,
        level2 INTEGER,
        received_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')
conn.commit()

# === MQTT Callback Functions ===
def on_connect(client, userdata, flags, rc):
    print("Connected with result code:", rc)
    # Subscribe to all topics under water_monitor/data/
    client.subscribe("water_monitor/data/#")

def on_message(client, userdata, msg):
    topic = msg.topic
    payload_str = msg.payload.decode('utf-8')
    print(f"Received message on topic {topic}: {payload_str}")
    
    # Parse the JSON payload
    try:
        data = json.loads(payload_str)
    except json.JSONDecodeError as e:
        print("Error parsing JSON:", e)
        return

    # Extract common and type-specific fields.
    mqtt_timestamp = data.get('timestamp')
    flow1       = data.get('flow1')      # e.g., for panelA, panelB, panelE
    turbidity   = data.get('turbidity')
    ph          = data.get('ph')
    tds         = data.get('tds')
    level1      = data.get('level1')      # e.g., for panelC, panelD, panelF
    level2      = data.get('level2')

    # Derive the panel from the topic string, e.g. "water_monitor/data/panelB"
    panel = topic.split('/')[-1]

    # Insert parsed data into the database
    try:
        cursor.execute('''
            INSERT INTO sensor_data (
                topic, panel, mqtt_timestamp, flow1, turbidity, ph, tds, level1, level2
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (topic, panel, mqtt_timestamp, flow1, turbidity, ph, tds, level1, level2))
        conn.commit()
        print("Data successfully inserted into database.")
    except Exception as e:
        print("Database insertion error:", e)

# === MQTT Client Setup ===
client = mqtt.Client()

# Set the username and password
client.username_pw_set("Gateway-WTP", "gateway-wtp")

# Configure TLS to securely connect on port 8883.
# This uses the default system CA certificates.
client.tls_set()

# Assign the callbacks
client.on_connect = on_connect
client.on_message = on_message

# Connect to the broker with the provided details.
mqtt_broker = "xaaa13e5.ala.asia-southeast1.emqxsl.com"
mqtt_port   = 8883
client.connect(mqtt_broker, mqtt_port, keepalive=60)

# Start the network loop (this call is blocking).
client.loop_forever()