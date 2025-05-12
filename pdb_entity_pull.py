import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import os
import csv
import time


output = '/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/pdb_json_entities'
pdb_ids = "/home/zhn1744/AlphaFold/data/pdb_ids_v3.csv"
failed_ids_path = "/home/zhn1744/AlphaFold/data/pdb_entity_pull_failed_ids_v3.csv"


os.makedirs(output, exist_ok=True)


# Grab all SwissProt IDs from Uniprot
pdb_ids = pd.read_csv(pdb_ids)
#pdb_ids = pdb_ids.iloc[:2]
pdb_ids.reset_index(drop=True, inplace=True)  # Reset index after slicing
cursor_index = 1
failed_index = 0
failed_ids = []


for i, row in pdb_ids.iterrows():
    entity_id = 0
    retry_count = 0
    working = True
    print(f"Pull {i} of {len(pdb_ids)}")
    pdb_id = row["pdb_id"]    
    # Wait time ensures compliance with API rate limit
    wait_time = 0.05
    last_request_time = datetime.now()

    # Start the request
    #start_time = datetime.now()
    #print(f"Request started at: {start_time}")
    while working == True:
        entity_id += 1
        URL = f"https://data.rcsb.org/rest/v1/core/polymer_entity/{pdb_id}/{entity_id}"
        
        while True:
            try:
                response = requests.get(URL)
                response.raise_for_status()  # Raise an error for bad status codes
                break  # Break out of the loop since we got a successful response
            except:
                retry_count +=1
                # If error code is 404, there are no more entries for this PDB ID
                if response.status_code == 404 & entity_id == 1:
                    print("HTTP Error 404: Resource not found")
                    working = False
                    failed_index += 1
                    print(f"Failed. Failed count is now {failed_index}")
                    failed_ids.append(pdb_id)
                    break  # Break out of the loop since the resource is not found
                else:
                    # If error code is not 404, there is likely a connection error
                    if retry_count == 0:
                        pass # Try the connection one msore time
                    else:
                        working = False
                        break


        if response.status_code != 404:
            print(URL)
            # Define the path to save the file, incorporating the output directory
            file_path = os.path.join(output, f'response_entry_{pdb_id}_{entity_id}.json')
            # Save the response as JSON
            with open(file_path, 'wb') as f:
                f.write(response.content)


    # Calculate the time to wait, ensuring at least `wait_time` seconds have passed since the last request
    time_elapsed = datetime.now() - last_request_time
    if time_elapsed < timedelta(seconds=wait_time):
        time_to_wait = (timedelta(seconds=wait_time) - time_elapsed).total_seconds()
        time.sleep(time_to_wait)

if failed_ids:
    failed_data = pd.DataFrame(data={'failed_ids': failed_ids})
    failed_data.to_csv(failed_ids_path, mode='a', index=False, header=not os.path.exists(failed_ids_path))




