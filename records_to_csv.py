import geopandas as gpd
import pandas as pd
import requests
import datetime
import time
import csv
import yaml
import json

def main():
    config = load_config()
    url = "https://api.purpleair.com/v1/sensors/:sensor_index/history/csv"
    url2 = "https://api.purpleair.com/v1/sensors/:sensor_index"
    names = get_sensor_names(url2, config)
    pull_data(url, datetime.datetime(2022, 1, 1, 0, 0, 0), datetime.datetime(2022, 2, 28, 23, 59, 59), config, names)
    
    
def load_config():

    """

    The function reads configuration file.

    Outputs
    Configuration data.


    """

    with open("config.yaml", "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return config

def get_sensor_names(url, config):
    sensor_names = {}
    sensor_index_list = config['sensor_indexes']
    for sensor_index in sensor_index_list:

        # Try to replace ':sensor_index' in the url with the sensor_index variable
        try:
            index_url = url.replace(":sensor_index", str(sensor_index))
        except:
            pass

        # Define headers and parameters for GET request
        sensor_headers = {

                "X-API-Key": config['api_token']

            }

        sensor_data_params = { 

            "sensor_index": sensor_index,
            "fields": "name"
            }
        
        # Make GET request and load response text as JSON
        response = requests.get(url=index_url, headers=sensor_headers, params=sensor_data_params)
        data = response.text
        sensor_names[sensor_index] = json.loads(data)['sensor']['name']
    return sensor_names
    

def pull_data(url, start_date, end_date, config, names):

    """
    Retrieve data from a set of sensors and return it as a dictionary of DataFrames.

    Parameters
        url : str
            The base URL to make requests to.
        start_date:
            From which date records will be pulled.
        end_date:
            Until which date records will be pulled.
        config:
            Dictionary of variables from the configuration file.
    Returns
        dict[str, pd.DataFrame]
            A dictionary with sensor index keys and pandas DataFrames as values, containing the data retrieved from the sensors.
    Notes
        The function makes GET requests to the specified URL with the sensor index appended and specified headers and parameters, and loads
        the response text as JSON to create the DataFrames with the data and fields from the JSON object. The sensor indices are hardcoded in the function.
    """
    
    sensor_index_list = config['sensor_indexes']
    count = 0

    for sensor_index in sensor_index_list:

        # Try to replace ':sensor_index' in the url with the sensor_index variable
        try:
            index_url = url.replace(":sensor_index", str(sensor_index))
        except:
            pass

        # Define headers and parameters for GET request
        sensor_headers = {

                "X-API-Key": config['api_token']

            }

        sensor_data_params = { 

            "sensor_index": sensor_index,
            "start_timestamp": time.mktime(start_date.timetuple()),
            "end_timestamp": time.mktime(end_date.timetuple()),
            "average": 1440,
            "fields": "*"
            }
        
        # Make GET request and load response text as JSON
        response = requests.get(url=index_url, headers=sensor_headers, params=sensor_data_params)
        data = response.text
        data = add_names(data, names, sensor_index)
        if count == 0:
            export_results(data, "w")
        else:
            export_results(data, "a")
        count=count+1

def add_names(data, names, sensor_index):
    count = 0
    data = "name,"+data
    rows = data.strip().split('\n')
    new_rows = []
    for row in rows:
        if count != 0:
            row = names[sensor_index] + "," + row
        new_rows.append(row)
        count = count + 1
    new_data = '\n'.join(new_rows)
    return new_data

def export_results(data, mode):
    data = data[:-1]
    if mode == "a":
        rows = data.strip().split('\n')
        rows.pop(0)
        data = '\n'.join(rows)
        # Fix date before exporting
        rows = data.split('\n')
        rows = [r.split(',') for r in rows]

        # modify the first column value for each row
        for row in rows:
            try:
                new_date = datetime.datetime.fromtimestamp(int(row[1])) # convert milliseconds to seconds and then to date.
                row[1] = new_date  # replace the second column value with the new value
            except ValueError:
                continue
    else:
        # Fix date before exporting
        rows = data.split('\n')
        rows = [r.split(',') for r in rows]

        # modify the first column value for each row
        for row in rows[1:]:  # exclude the header row
            try:
                new_date = datetime.datetime.fromtimestamp(int(row[1])) # convert milliseconds to seconds and then to date.
                row[1] = new_date  # replace the second column value with the new value
            except ValueError:
                continue

    # Start exporting
    with open('output.csv', mode, encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)

        # write the data to the CSV file
        writer.writerows(rows)

if __name__ == "__main__":
    main()