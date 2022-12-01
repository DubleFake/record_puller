from base64 import encode
import shutil
from typing import Type
from urllib import request
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import requests
import geopandas as gpd
import yaml
import os

def get_records(url, conf, main_query):

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
        for key in conf[main_query]:
            query[key] = conf[main_query][key]
    response = requests.get(url, params=query)
    return response.text

def download_records(conf):

    # Create the BlobServiceClient object with connection string from the config file
    blob_service_client = BlobServiceClient.from_connection_string(conf["connection_string"])
    # Create a blob client using the preset file name
    blob_client = blob_service_client.get_blob_client(container="variousgisdata", blob="data.gpkg")

    print("Downloading the package")

    download_file_path = "downloaded_data.gpkg"
    container_client = blob_service_client.get_container_client(container= "variousgisdata")

    with open(file=download_file_path, mode="wb") as download_file:
        download_file.write(container_client.download_blob("data.gpkg").readall())

    print("Done.")

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

def largest_number(in_str):
    arr = in_str.split(",")
    return arr.pop()

def pull_data(data_from_links, Lines, conf):

    # Parse through every record in list of links, pull data from each one and save all data to list.

    print('Starting to pull data...')

    for line in Lines:
        print('Working on: ' + line)
        data_from_links.append(get_records(line.strip(), conf, 'global'))
    print('Done!')

def write_to_files(data_from_links):

    count = 1

    print('Writing to files...')


    # Go through each of the link's data from the list, convert it to GEOJSON and save it to 
    # file (each on a different layer) and save each element as a separate .json file
    
    try:
        os.mkdir("JSONs")
    except:
        pass

    for data in data_from_links:
        # Save to main .gpkg file
        gpd.read_file(data).to_file('output.gpkg', layer=str(count) ,driver="GPKG")
        with open("JSONs/layer" + str(count) + ".json", "w", encoding='utf-8') as outfile:
            outfile.write(data)
        outfile.close()
        count = count + 1
    
    print('Done!')

def upload_files(data_from_links, conf):

    print('Starting to upload files...')
    try:

        # Create the BlobServiceClient object with connection string from the config file
        blob_service_client = BlobServiceClient.from_connection_string(conf["connection_string"])

    
        # Upload every json file
        count = 1
        while count <= len(data_from_links):

            # Create a blob client using the preset file name
            blob_client = blob_service_client.get_blob_client(container="variousgisdata", blob="layer"+str(count)+".json")

            # Check if blob exists and if it does, delete it so the replacement could be uploaded
            if(blob_client.exists()):
                blob_client.delete_blob()

            # Upload the created file
            with open(file="JSONs/layer"+str(count)+".json", mode="rb") as data:
                blob_client.upload_blob(data)
            count=count+1

        # Create a blob client using the preset file name
        blob_client = blob_service_client.get_blob_client(container="variousgisdata", blob="data.gpkg")

        # Check if blob exists and if it does, delete it so the replacement could be uploaded
        if(blob_client.exists()):
            blob_client.delete_blob()


        # Upload .gpkg file
        with open(file="output.gpkg", mode="rb") as data:
            blob_client.upload_blob(data)

        # Delete local file after it was uploaded
        # os.remove("output.gpkg")

        # Delete local layer 
        # shutil.rmtree('JSONs')
    
        print('Done!')

    except Exception as ex:
        print('Exception:')
        print(ex)

def mark_links_to_download_from(Lines, linksToDownloadFrom, conf):
    x = 0
    while(x < len(Lines)):
        data_from_file = gpd.read_file('downloaded_data.gpkg', layer=str(x+1))
        records_from_file = data_from_file[data_from_file.columns[0]].count()
        records_from_cloud = get_records(Lines[x], conf, 'getCount')
        if(records_from_file == records_from_cloud):
            linksToDownloadFrom.append(Lines[x])
        x = x + 1
    

    print("Done.")

def main():

    # Preperation for work: loading config, loading link list, initializing required variable(-s).
    linksToDownloadFrom = []
    conf = load_config()
    file = open('links.txt', 'r')
    Lines = file.readlines()
    data_from_links = []
    
    #download_records(conf)
    #pullData(data_from_links, Lines, conf)
    #writeToFiles(data_from_links)
    #uploadFiles(data_from_links, conf)
    mark_links_to_download_from(Lines, linksToDownloadFrom, conf)

    print(len(linksToDownloadFrom))
    #For testing purposes
    #data = gpd.read_file('downloaded_data.gpkg', layer="10")
    #print(data)
    #print(data[data.columns[0]].count())
   
if __name__ == "__main__":
    main()


