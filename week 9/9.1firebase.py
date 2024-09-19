import firebase_admin
from firebase_admin import credentials, db
import time
import csv

# Path to your Firebase service account key
cred = credentials.Certificate("research-195f0-firebase-adminsdk-y0iyy-fb4d6ef1b5.json")

# Initialize the Firebase app
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://research-195f0-default-rtdb.firebaseio.com/'  # Replace with your Firebase Realtime Database URL
})

# Reference to the Firebase Realtime Database
ref = db.reference('room_monitoring')

def store_data_to_firebase(temperature, humidity, heater_state, occupancy, power_consumption, timestamp):
    # Create a data dictionary
    data = {
        'temperature': temperature,
        'humidity': humidity,
        'heater_state': heater_state,
        'occupancy': occupancy,
        'power_consumption': power_consumption,
        'timestamp': timestamp
    }

    # Push data to the database
    ref.push(data)
    print("Data pushed to Firebase:", data)

def read_latest_data_from_csv(file_path):
    with open(file_path, 'r') as file:
        # Create a CSV reader object
        csv_reader = csv.DictReader(file)

        # Print headers for debugging
        print("CSV Headers:", csv_reader.fieldnames)

        # Get the last row (latest data)
        last_row = None
        for row in csv_reader:
            last_row = row

        if last_row:
            # Extract data from the last row
            try:
                timestamp = last_row['Timestamp']
                temperature = float(last_row['Temperature'])
                humidity = float(last_row['Humidity'])
                heater_state = last_row['Heater State'].strip().lower() == 'true'  # Convert string to boolean
                occupancy = last_row['Occupancy'].strip().lower() == 'true'  # Convert string to boolean
                power_consumption = float(last_row['Power Consumption'])

                return timestamp, temperature, humidity, heater_state, occupancy, power_consumption
            except KeyError as e:
                print(f"Missing column in CSV: {e}")
                return None, None, None, None, None, None
            except ValueError as e:
                print(f"Error converting value: {e}")
                return None, None, None, None, None, None
        else:
            print("CSV file is empty or not in correct format")
            return None, None, None, None, None, None

# CSV file path
csv_file_path = 'temperature_humidity_power_data.csv'  # Updated CSV file name

# Main loop to read from CSV and push data to Firebase
while True:
    # Read the latest data from the CSV file
    timestamp, temperature, humidity, heater_state, occupancy, power_consumption = read_latest_data_from_csv(csv_file_path)

    if timestamp and temperature is not None and humidity is not None:
        # Store the data to Firebase
        store_data_to_firebase(temperature, humidity, heater_state, occupancy, power_consumption, timestamp)
    else:
        print("No valid data found in CSV")

    # Wait before reading the next data (you can adjust the interval)
    time.sleep(60)  # Upload data every minute
