from codecs import ignore_errors
from pickle import TRUE
from time import sleep
from tkinter.tix import COLUMN
from azure.storage.blob import BlobServiceClient
import requests
import pandas as pd
import geopandas as gpd
import yaml
import os
import json
import time

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
    count = 0
    result = []
    column_name = ""

    # Check if records exceed 2000
    # print(json.loads(data_from_links[0])['properties']['count'])
    for key in conf['getCount']:
            query[key] = conf['getCount'][key]
    response = requests.get(url, params=query)
    records = int(json.loads(response.text)['properties']['count'])

    query = {}
    for key in conf['getOneRecord']:
        query[key] = conf['getOneRecord'][key]
    response = requests.get(url, params=query)
    for element in conf['id_table_names']:
        if element.lower() in response.text.lower():
            column_name = element
            break;
    
 
    print("Total records: " + str(records))

    query = {}
    if (records > 2000 and main_query != 'getCount' and main_query != 'getOneRecord'):
        while (records >= count):
            query = {}
            if(url in conf):
                for key in conf[url]:
                    query[key] = conf[url][key]
            else:
                for key in conf[main_query]:
                    query[key] = conf[main_query][key]
            if(count+2000 <= records):
                query['where'] = column_name + ' <= ' + str(count+2000) + ' AND ' + column_name + ' > ' + str(count)
            else:
                query['where'] = column_name + ' <= ' + str(records) + ' AND ' + column_name + ' > ' + str(count)
            print("\t" + query['where'])
            response = requests.get(url, params=query)
            time.sleep(1)
            result.append(response.text)
            count = count + 2000
        return result
    else:
        if(url in conf):
            for key in conf[url]:
                query[key] = conf[url][key]
        else:
            for key in conf[main_query]:
                query[key] = conf[main_query][key]
        response = requests.get(url, params=query)
        result.append(response.text)
        return result

def download_records(conf):

    # Create the BlobServiceClient object with connection string from the config file
    blob_service_client = BlobServiceClient.from_connection_string(conf["connection_string"])
    # Create a blob client using the preset file name
    blob_client = blob_service_client.get_blob_client(container="variousgisdata", blob="data.gpkg")

    print("Downloading the package...")

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
        result = get_records(line.strip(), conf, 'global') 
        data_from_links.append(result)
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
    
    for record in data_from_links:
        loop = 0
        for data in record:
        # Save to main .gpkg file
            if (loop == 0):
                gpd.read_file(data).to_file('output.gpkg', layer="1" ,driver="GPKG")
                with open("JSONs/layer" + str(count) + ".json", "w", encoding='utf-8') as outfile:
                    outfile.write(data)
                outfile.close()
            else:
                gpd.read_file(data).to_file('output.gpkg', layer="1" ,driver="GPKG", mode="a")
                with open("JSONs/layer" + str(count) + ".json", "a", encoding='utf-8') as outfile:
                    outfile.write(data)
                outfile.close()
            loop = loop + 1
            print("Loop No." + str(loop) + " ended")
        count = count + 1
        
        
    time.sleep(1)
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

    
        print('Done!')

    except Exception as ex:
        print('Exception:')
        print(ex)

def mark_links_to_download_from(Lines, linksToDownloadFrom, conf):
    print("Marking links...")
    x = 0
    while(x < len(Lines)):
        data_from_file = gpd.read_file('downloaded_data.gpkg', layer=str(x+1))
        records_from_file = data_from_file[data_from_file.columns[0]].count()
        records_from_cloud = get_records(Lines[x].strip(), conf, 'getCount')

        print(str(x) + ": " + str(records_from_file) + "==" + str(json.loads(records_from_cloud[0])['properties']['count']))
        
        if(records_from_file != int(json.loads(records_from_cloud[0])['properties']['count'])):
            linksToDownloadFrom.append(Lines[x])
        x = x + 1
    

    print("Done.")

def main():

    # Preperation for work: loading config, loading link list, initializing required variable(-s).
    linksToDownloadFrom = []
    conf = load_config()
    file = open('links.txt', 'r')
    Lines = file.readlines()
    data_from_links = [[]]
    
    #download_records(conf)
    mark_links_to_download_from(Lines, linksToDownloadFrom, conf)
    if(len(linksToDownloadFrom) != 0):
        pull_data(data_from_links, linksToDownloadFrom, conf)
        write_to_files(data_from_links)
        #upload_files(data_from_links, conf)
    




    #For testing purposes


    #//////////////////////////////////////////////////////////
    #data = gpd.read_file('test.gpkg', layer="1")
    #print(data)
    #print(data[data.columns[0]].count())

    #temp = get_records(Lines[0].strip(), conf, 'global')
    #for record in temp:
    #   data_from_links.append(record)
    #write_to_files(data_from_links)
    #data_from_links.append(get_records(Lines[1].strip(), conf, 'test2'))
    
    #temp.append(gpd.read_file(data_from_links[1]))
    #//////////////////////////////////////////////////////////

    print(gpd.read_file("output.gpkg", layer="1"))

if __name__ == "__main__":
    main()


