import csv
import time
from datetime import datetime
from arduino_iot_cloud import ArduinoCloudClient

DEVICE_ID = "50510722-a1a5-427d-8f5f-b01a15f89147"  # Replace with your actual device ID
SECRET_KEY = "b2Zdl3Cf5gb0M3?QLxgARW@ta"  # Replace with your actual secret key

# CSV file name
csv_file = "temperature_humidity_power_data.csv"

# Buffer to store data
buffer = {
    'timestamp': None,
    'temperature': None,
    'humidity': None,
    'heater_state': None,
    'occupancy': None,
    'power_consumption': None
}

# Last update timestamp
last_update = None

# Callback functions for Arduino Cloud variables
def on_temperature_changed(client, value):
    buffer['temperature'] = round(value, 2)  # Round to 2 decimal places
    print(f"Temperature updated: {buffer['temperature']}")
    save_data_if_complete()

def on_humidity_changed(client, value):
    buffer['humidity'] = round(value, 2)  # Round to 2 decimal places
    print(f"Humidity updated: {buffer['humidity']}")
    save_data_if_complete()

def on_heater_state_changed(client, value):
    buffer['heater_state'] = value
    print(f"Heater State updated: {buffer['heater_state']}")
    save_data_if_complete()

def on_occupancy_changed(client, value):
    buffer['occupancy'] = value
    print(f"Occupancy updated: {buffer['occupancy']}")
    save_data_if_complete()

def on_power_consumption_changed(client, value):
    buffer['power_consumption'] = round(value, 2)  # Round to 2 decimal places
    print(f"Power Consumption updated: {buffer['power_consumption']}")
    save_data_if_complete()

# Save data to CSV if a specified time interval has passed
def save_data_if_complete():
    global last_update
    current_time = datetime.now()
    
    # If timestamp is not set or the interval has passed, update the CSV
    if last_update is None or (current_time - last_update).total_seconds() >= 1:
        last_update = current_time
        timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')
        save_data(timestamp, buffer['temperature'], buffer['humidity'], buffer['heater_state'], buffer['occupancy'], buffer['power_consumption'])
        # Reset the buffer
        buffer['temperature'] = None
        buffer['humidity'] = None
        buffer['heater_state'] = None
        buffer['occupancy'] = None
        buffer['power_consumption'] = None

# Function to save data to CSV file
def save_data(timestamp, temperature, humidity, heater_state, occupancy, power_consumption):
    try:
        with open(csv_file, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow([timestamp, temperature, humidity, heater_state, occupancy, power_consumption])
            file.flush()  # Ensure data is written to the file immediately
            print(f"Data saved: {timestamp}, Temperature={temperature}, Humidity={humidity}, Heater State={heater_state}, Occupancy={occupancy}, Power Consumption={power_consumption}")
    except Exception as e:
        print(f"Error writing to CSV: {e}")

def main():
    print("Starting the IoT client...")

    # Instantiate Arduino Cloud Client
    client = ArduinoCloudClient(
        device_id=DEVICE_ID, username=DEVICE_ID, password=SECRET_KEY
    )

    # Register cloud variables
    client.register("temperature", value=None, on_write=on_temperature_changed)
    client.register("humidity", value=None, on_write=on_humidity_changed)
    client.register("heater_state", value=None, on_write=on_heater_state_changed)
    client.register("occupancy", value=None, on_write=on_occupancy_changed)
    client.register("power_consumption", value=None, on_write=on_power_consumption_changed)

    # Initialize the CSV file and write headers if not already created
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["Timestamp", "Temperature", "Humidity", "Heater State", "Occupancy", "Power Consumption"])
            print("CSV file initialized with headers.")
    except Exception as e:
        print(f"Error initializing CSV file: {e}")

    # Start the Arduino IoT Cloud client
    client.start()

    # Keep the script running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Script terminated by user.")

if __name__ == "__main__":
    main()

