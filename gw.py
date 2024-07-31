from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
import time

# ThingsBoard client configuration
TB_SERVER = "thingsboard.cloud"
TB_PORT = 1883
DEVICE_TOKEN = "t19v9MCQILdTBjklr6er"

# Location parameters
LATITUDE = 14.683860
LONGITUDE = -17.462075

def read_file():
    with open("device_data.txt", "r") as fd:
        lines = [line.rstrip() for line in fd]
        return lines

def get_obstruction_status_from_file(line):
    if line == "#1":
        return 1
    elif line == "#0":
        return 0
    else:
        print('ERROR: getting Arduino sensor values from file')
        return None

def tb_connect(addr, port, device_token):
    return TBDeviceMqttClient(addr, port, device_token)

def send_location(client, latitude, longitude):
    gps_coord = {"latitude": latitude, "longitude": longitude}
    print(f"Sending location {gps_coord}")
    result = client.send_attributes(gps_coord)
    if result.get() == 0:
        print("OK")
    else:
        print(f"ERROR --> {result.get()}")

def send_obstruction_status(client, timestamp, obstruction_status):
    telemetry_with_ts = {"ts": timestamp, "values": {"obstruction_status": obstruction_status}}
    print(f"Sending telemetry {telemetry_with_ts}")
    result = client.send_telemetry(telemetry_with_ts)
    if result.get() == 0:
        print("OK")
    else:
        print(f"ERROR --> {result.get()}")

def main():
    # Read sensor data from file
    sensor_data_from_file = read_file()
    number = 0

    # Setup ThingsBoard Server
    print(f"Connecting to {TB_SERVER}...")
    tb_client = tb_connect(TB_SERVER, TB_PORT, DEVICE_TOKEN)
    tb_client.max_inflight_messages_set(100)
    tb_client.connect()
    time.sleep(5)

    # Set attributes
    send_location(tb_client, LATITUDE, LONGITUDE)

    # Read sensor data and send it to ThingsBoard
    while True:
        timestamp = int(round(time.time() * 1000))

        # Data from file (from simulated sensor)
        obstruction_status = get_obstruction_status_from_file(sensor_data_from_file[number])
        number += 1
        if number >= len(sensor_data_from_file):
            number = 0

        if obstruction_status is not None:
            send_obstruction_status(tb_client, timestamp, obstruction_status)

        # Pause for 1 second
        time.sleep(1)

if __name__ == "__main__":
    main()
