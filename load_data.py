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

conf = {}

def get_records(url, main_query):

    """

    The function pulls records from the web services in accordance with configuration file.

    Arguments
    url: str
        URL address of a web service
    conf: dict
        configuration file
    main_query: str
        predefined query name in the configuration file

    Outputs
        Geodata frame from queried web service as JSON string list.

    """
    
    # Create a list to store query parameters 
    query = {}
    
    # Create a pointer to keep track of records
    count = 0
    
    # Create a result list
    result = []
    
    # Create a variable to store the correct column's name 
    column_name = ""

    # Load predefined parameters from the configuration file to get the count of records.
    for key in conf['getCount']:
            query[key] = conf['getCount'][key]
    
    # Send the request
    response = requests.get(url, params=query)
    
    # Save the record count
    records = int(json.loads(response.text)['properties']['count'])

    # Clear the 'query' variable
    query = {}
    
    # Load predefined parameters from the configuration file to get one record.
    for key in conf['getOneRecord']:
        query[key] = conf['getOneRecord'][key]
        
    # Send the request
    response = requests.get(url, params=query)
    
    # Parse thorugh column names, which are predefined in the configuration file.
    for element in conf['id_table_names']:
        # Check if the record has a column with the same name as the one from the list 
        if not (json.loads(response.text.lower())['features'][0]['properties'].get(element.lower()) is None):
            # Set the value of 'column_name' with the name of the column, that record has from a given list and break the loop 
            column_name = element
            break; 
    
    # Print total records to console
    printToConsole("Total records: " + str(records))
    
    # Check if record codun exceeds 2000 and the requested query name is not 'getCount' or 'getOneRecord', whitch are predefined query names.
    if (records > 2000 and main_query != 'getCount' and main_query != 'getOneRecord'):
        # If true, loop through all the records 2000 at a time 
        while (records >= count):
            
            # Clear the 'query' variable
            query = {}
            
            # Check if passed URL has user defined query parameters
            if(url in conf):
                # If true, add those parameters to the query
                for key in conf[url]:
                    query[key] = conf[url][key]
            else:
                # If false, add default query parameters
                for key in conf[main_query]:
                    query[key] = conf[main_query][key]
                    
            # Check if incrementing the pointer would exceed the total record count
            if(count+2000 <= records):
                # If it does not, create 'where' statement to get the next 2000 records
                query['where'] = column_name + ' <= ' + str(count+2000) + ' AND ' + column_name + ' > ' + str(count)
            else:
                # if it does, create 'where' statement to get the remaining records.
                query['where'] = column_name + ' <= ' + str(records) + ' AND ' + column_name + ' > ' + str(count)
                
            # Print the progress to console
            printToConsole("\t" + query['where'])
            
            # Send the request
            response = requests.get(url, params=query)
            
            # Wait for one second
            time.sleep(1)
            
            # Add the response as text to the result list
            result.append(response.text)
            
            # Increment count by 2000
            count = count + 2000
        return result
    else:
        # if record count is less than 2000 or the requested query name is not 'getCount' or 'getOneRecord', check if the requested URL has user defined query parameters
        if(url in conf):
            # If true, add those parameters to the query
            for key in conf[url]:
                query[key] = conf[url][key]
        else:
            # If false, add default query parameters
            for key in conf[main_query]:
                query[key] = conf[main_query][key]
                
        # Send the request
        response = requests.get(url, params=query)
        
        # Add the response as text to the result list
        result.append(response.text)
        
        # Return the result list
        return result

def download_records():

    """
    Downloads a GPKG file from Azure Blob Storage and saves it to the local file system.
    
    Parameters:
        None
                 
    Returns:
        None
    """

    # Create a BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(conf["connection_string"])
    
    # Get a BlobClient for the GPKG file in the "variousgisdata" container
    blob_client = blob_service_client.get_blob_client(container="variousgisdata", blob="data.gpkg")

    # Print a message to the console indicating that the download is starting
    printToConsole("Downloading the package...")

    # Set the local file path for the downloaded GPKG file
    download_file_path = "downloaded_data.gpkg"
    
    # Get a ContainerClient for the "variousgisdata" container
    container_client = blob_service_client.get_container_client(container= "variousgisdata")

    # Open a file in write binary mode and write the contents of the GPKG file to it
    with open(file=download_file_path, mode="wb") as download_file:
        download_file.write(container_client.download_blob("data.gpkg").readall())

    # Print a message to the console indicating that the download is finished
    printToConsole("Done.")

def load_config():
    
    """
    Loads the configuration from the 'config.yml' file.
    
    Returns:
        dict: A dictionary containing the configuration parameters.
    
    Raises:
        YAMLError: If there is a problem parsing the configuration file.
    """
    
    with open("config.yml", "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            printToConsole(exc)
    return config

def largest_number(in_str):
    
    """
    Returns the last element in a string separated by commas.
    
    Parameters:
        in_str (str): The input string to be processed.
    
    Returns:
        str: The last element in the input string after it has been split by commas.
    """
    
    arr = in_str.split(",")
    return arr.pop()

def pull_data(data_from_links, Lines):
    
    """
    Pull data from a list of links and append it to a list.

    Parameters:
        data_from_links (list): A list where the data will be appended.
        Lines (list): A list of links from which to pull data.

    Returns:
        None
    """

    # Print a message indicating that the data pull has started
    printToConsole('Starting to pull data...')

    # Loop through the list of links
    for line in Lines:
        
         # Print a message indicating which link is being processed
        printToConsole('Working on: ' + line)
        
        # Retrieve data from the link using the get_records function
        result = get_records(line.strip(), conf, 'global')
        
         # Append the retrieved data to the data_from_links list
        data_from_links.append(result)
        
    # Print a message indicating that the data pull is complete    
    printToConsole('Done!')

def write_to_files(data_from_links):

    """
    Write data to a series of files.
    
    Parameters:
    data_from_links (list): A list of records, where each record is a list of strings.
    
    Returns:
    None
    """

    count = 1

    printToConsole('Writing to files...')
    
    # Try to create a directory called "JSONs"
    try:
        os.mkdir("JSONs")
        
    # If it fails, assume the directory already exists    
    except:
        pass
    
    # Iterate over the records in data_from_links
    for record in data_from_links:
        loop = 0
        
        # Iterate over the data in the current record
        for data in record:
            # If this is the first iteration, write the data to "output.gpkg" and "JSONs/layer" + count + ".json"
            if (loop == 0):
                gpd.read_file(data).to_file('output.gpkg', layer=str(count) ,driver="GPKG")
                with open("JSONs/layer" + str(count) + ".json", "w", encoding='utf-8') as outfile:
                    outfile.write(data)
                outfile.close()
            else:
                # If this is not the first iteration, append the data to these same files
                gpd.read_file(data).to_file('output.gpkg', layer=str(count) ,driver="GPKG", mode="a")
                with open("JSONs/layer" + str(count) + ".json", "a", encoding='utf-8') as outfile:
                    outfile.write(data)
                outfile.close()
            loop = loop + 1
            printToConsole("Loop No." + str(loop) + " ended")
        count = count + 1
    
    # Print "Done!" to the console
    printToConsole('Done!')

def upload_files(data_from_links):

    """
    Uploads files to a specified Azure Blob storage container.

    Parameters:
    data_from_links (list): A list of data retrieved from links.

    Returns:
    None.
    """

    printToConsole('Starting to upload files...')
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

    
        printToConsole('Done!')

    except Exception as ex:
        printToConsole('Exception:')
        printToConsole(ex)

def mark_links_to_download_from(Lines, linksToDownloadFrom):
    
    
    
    printToConsole("Marking links...")
    x = 0
    while(x < len(Lines)):
        
        # Read data from a file named 'downloaded_data.gpkg'
        data_from_file = gpd.read_file('downloaded_data.gpkg', layer=str(x+1))
        
        # Get the number of records in the data from the file
        records_from_file = data_from_file[data_from_file.columns[0]].count()
        
        # Get the number of records in the data from the cloud using the 'get_records' function
        records_from_cloud = get_records(Lines[x].strip(), conf, 'getCount')

        printToConsole(str(x) + ": " + str(records_from_file) + "==" + str(json.loads(records_from_cloud[0])['properties']['count']))
        
        # If the number of records in the data from the file is not equal to the number of records in the data from the cloud
        if(records_from_file != int(json.loads(records_from_cloud[0])['properties']['count'])):
            
            # Add the link to the list of links to be downloaded
            linksToDownloadFrom.append(Lines[x])
        x = x + 1
    

    printToConsole("Done.")

def printToConsole(string):
    if(conf['provide_output'] == True):
        print(string)

def main():

    """
    This is the main function
    
    Parameters:
        None.
        
    Returns:
        None.
    
    
    
    """
    
    # Create dictionary to store links, from which data will be downloaded
    linksToDownloadFrom = []
    
    # Load configuration from file
    conf = load_config()
    
    # Read all links from file 'links.txt'.
    file = open('links.txt', 'r')
    Lines = file.readlines()
    
    # Create list of lists to store data from links
    data_from_links = [[]]
    
    #download_records()
    mark_links_to_download_from(Lines, linksToDownloadFrom)
    if(len(linksToDownloadFrom) != 0):
        pull_data(data_from_links, linksToDownloadFrom)
        write_to_files(data_from_links)
        #upload_files(data_from_links)
    




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

    #print(gpd.read_file("output.gpkg", layer="2"))

if __name__ == "__main__":
    main()


