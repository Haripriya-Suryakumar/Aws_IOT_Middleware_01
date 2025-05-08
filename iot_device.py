from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import random
import datetime

# Initialize the MQTT Client
client = AWSIoTMQTTClient("AssetTracker")
client.configureEndpoint("a3dmb30l73pi7o-ats.iot.us-east-1.amazonaws.com", 8883)
client.configureCredentials("AmazonRootCA1.pem", "private.pem.key", "certificate.pem.crt")
client.configureOfflinePublishQueueing(-1)
client.configureDrainingFrequency(2)
client.configureConnectDisconnectTimeout(10)
client.configureMQTTOperationTimeout(5)
client.connect()

# Function to convert ISO 8601 timestamp to Unix time in milliseconds
def iso_to_unix_ms(iso_timestamp):
    dt = datetime.datetime.strptime(iso_timestamp, '%Y-%m-%dT%H:%M:%SZ')
    unix_timestamp = int(dt.timestamp())  # Convert to Unix timestamp (seconds)
    unix_timestamp_ms = unix_timestamp * 1000  # Convert to milliseconds
    return unix_timestamp_ms

# Simulate asset data with timestamp conversion
def simulate_asset_data():
    data = {
        "deviceId": "asset-001",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latitude": round(random.uniform(12.90, 13.00), 6),
        "longitude": round(random.uniform(77.50, 77.60), 6),
        "speed": round(random.uniform(0, 80), 2),
        "status": random.choice(["moving", "stopped"])
    }
    
    # Convert the ISO 8601 timestamp to Unix timestamp in milliseconds
    data["timestamp"] = iso_to_unix_ms(data["timestamp"])
    
    return data

# Main loop to simulate and publish data
while True:
    data = simulate_asset_data()
    print("Publishing:", data)
    client.publish("asset/tracker", json.dumps(data), 1)
    time.sleep(5)
