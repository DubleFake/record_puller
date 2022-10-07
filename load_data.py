from urllib import request
import requests
import datetime
import json 
import fiona
import geopandas as gpd
import pandas

def get_records(url):
    query = { 
    'where':'1=1',
    'outFields':'*', 
    'returnIdsOnly':'false',
    'returngeometry':'true',
    'f':'geojson'}
    response = requests.get(url, params=query)
    data = gpd.read_file(response.text);
    return data


file = open('links.txt', 'r')
Lines = file.readlines()
count = 1
for line in Lines:
    data = get_records(line.strip())
    data.to_file('output.gpkg', layer=str(count),driver="GPKG")
    print('Finished link no. ' + str(count))
    count=count+1
print('Done.')

#For testing purposes
#
#data = gpd.read_file('output0.gpkg', layer="10")
#print(data)

