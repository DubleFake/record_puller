from urllib import request
import requests
import datetime
import json 
import fiona
import geopandas as gpd
import pandas
import yaml

def get_records(url, conf):
    query = {}
    if(url in conf):
        for key in conf[url]:
            query[key] = conf[url][key]
    else:
        for key in conf['global']:
            query[key] = conf['global'][key]
    response = requests.get(url, params=query)
    data = gpd.read_file(response.text);
    return data

def load_config():
    with open("config.yml", "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return config

conf = load_config()
file = open('links.txt', 'r')
Lines = file.readlines()
count = 1
for line in Lines:
    data = get_records(line.strip(), conf)
    data.to_file('output.gpkg', layer=str(count),driver="GPKG")
    print('Finished link no. ' + str(count))
    count=count+1
print('Done.')



#For testing purposes
#
data = gpd.read_file('output.gpkg', layer="3")
print(data)

