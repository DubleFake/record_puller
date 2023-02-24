load_data.py:
  This script queries all web services listed in "links.txt" and pulls data from them in accordance with "config.yml". Configuration file ("config.yml")     can be edited to retrieve relevant data from all the web services or specific ones. Additional instruction for modifying "config.yml" is provided in the configuration   file itself as comments.
  
records_to_csv:
  This script queries PurpleAir API and returns all data from a daily average from a span of two months (2022-01-01 - 2022-02-28). It formats timestamp to be a date and conjoins all results into one .csv file. 
