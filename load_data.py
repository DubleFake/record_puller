from urllib import request
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import requests
import geopandas as gpd
import yaml
import os

def get_records(url, conf):

    """

    The function pulls records from the web services in accordance with configuration file.

    Arguments
    url: str
        URL address of a web service
    conf: dict
        configuration file

    Outputs
    Geodata frame from queried web service as JSON string.

    """

    query = {}
    if(url in conf):
        for key in conf[url]:
            query[key] = conf[url][key]
    else:
        for key in conf['global']:
            query[key] = conf['global'][key]
    response = requests.get(url, params=query)
    return response.text



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
data_from_links = []
count = 1

# Parse through every record in list of links, pull data from each one and save all data to list.

print('Starting to pull data...')

for line in Lines:
    print('Working on: ' + line)
    data_from_links.append(get_records(line.strip(), conf))
print('Done!')


print('Writing to files...')
# Go through each of the link's data from the list, convert it to GEOJSON and save it to 
# file (each on a different layer) and save each element as a separate .json file

for data in data_from_links:

    # Save to main .gpkg file
    gpd.read_file(data).to_file('output.gpkg', layer=str(count),driver="GPKG")

    # Create a directory to store JSONs if it does not exist
    try:
        os.makedir("JSONs")
    except:
        pass
        

    # Save every layer to their own json file
    with open("JSONs/layer"+str(count)+".json", "w", encoding="utf8") as json_file:
       json_file.write(data)
    count=count+1

print('Done!')


"""
#For testing purposes
data = gpd.read_file('output.gpkg', layer="1")
print(data)
"""



print('Starting to upload files...')

try:

    # Create the BlobServiceClient object with connection string from the config file
    blob_service_client = BlobServiceClient.from_connection_string(conf["connection_string"])

    # Create timestamp for file names
    dt_string = datetime.now().strftime("%d-%m-%Y-%H:%M:%S")

    
    # Upload every json file
    count = 1
    while count <= len(data_from_links):

        # Create a blob client using the preset file name with the current time as the name for the blob
        blob_client = blob_service_client.get_blob_client(container="variousgisdata", blob="layer"+str(count)+"_"+dt_string+".json")

        # Upload the created file
        with open(file="C:/Users/Bob/Desktop/Work/Python/record_puller/JSONs/layer"+str(count)+".json", mode="rb") as data:
            blob_client.upload_blob(data)
        count=count+1

    # Upload .gpkg file
    blob_client = blob_service_client.get_blob_client(container="variousgisdata", blob="data_"+dt_string+".gpkg")
    with open(file="C:/Users/Bob/Desktop/Work/Python/record_puller/output.gpkg", mode="rb") as data:
        blob_client.upload_blob(data)
    
    print('Done!')
except Exception as ex:
    print('Exception:')
    print(ex)

