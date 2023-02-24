import requests
import geopandas as gpd
import datetime
import json
import csv

def main():
    fromDate = int(datetime.datetime(2022, 1, 1).timestamp() * 1000)
    toDate = int(datetime.datetime(2022, 2, 28).timestamp() * 1000)
    link = "https://opencity.vplanas.lt/arcgis/rest/services/Aplinkosauga/Oro_tarsa/MapServer/3/query"
    data = get_records(link)
    sorted = sort_records(data, fromDate, toDate)
    export_results(sorted)

    print()


def get_records(url):
    query = {
        'where':'1=1',
        'returnIdsOnly':'false',
        'returnGeometry':'false',
        'outFields':'*',
        'f':'pjson'
        }
    response = requests.get(url, params=query)
    return json.loads(json.dumps(response.json()))

def sort_records(data_to_sort, fromDate, toDate):

    result = []

    for record in data_to_sort['features']:

        if(record['attributes']['last_seen'] >= fromDate and record['attributes']['last_seen'] <= toDate):
            result.append(record)
    
    return result

def export_results(results):
    keys = results[0]['attributes'].keys()
    data = [d['attributes'] for d in results]
    fix_date(data)

    # Open the CSV file in write mode
    with open('output.csv', 'w', newline='', encoding='utf-8-sig') as output_file:

        # Create a CSV writer object
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)

        # Write the header row
        dict_writer.writeheader()

        # Write the data rows
        dict_writer.writerows(data)

def fix_date(data):
    for record in data:
        new_date = datetime.datetime.fromtimestamp(record['last_seen'] / 1000) # convert milliseconds to seconds and then to date.
        record['last_seen'] = new_date

if __name__ == "__main__":
    main()