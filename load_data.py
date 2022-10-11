from urllib import request
import requests
import datetime
import json 
import fiona
import geopandas as gpd
import pandas
import yaml

def get_records(url, conf):

    """

    The function pulls records from the web services in accordance with configuration file.

    Arguments
    url: str
        URL address of a web service
    conf: dict
        configuration file

    Outputs
    Geodata frame from queried web service.

    """

    query = {}
    if(url in conf):
        for key in conf[url]:
            query[key] = conf[url][key]
    else:
        for key in conf['global']:
            query[key] = conf['global'][key]
    response = requests.get(url, params=query)
    data = gpd.read_file(response.text);
    print(type(data))
    return data



def load_config():

    """

    The function reads configuration file.

    Outputs
    Configuration data.


    """

    with open("config.yml", "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return config


# Preperation for work: loading config, loading link list, initializing required variable(-s).

conf = load_config()
file = open('links.txt', 'r')
Lines = file.readlines()
count = 1


# Parse through every record in list of links, pull data from each one and save all data to one .gpkg file, but on different layers for every link.

for line in Lines:
    data = get_records(line.strip(), conf)
    data.to_file('output.gpkg', layer=str(count),driver="GPKG")
    print('Finished link no. ' + str(count))
    count=count+1
print('Done.')


"""

For testing purposes

data = gpd.read_file('output.gpkg', layer="3")
print(data)

"""
