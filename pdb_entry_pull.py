import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import os
import csv
import time


output = '/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/pdb_json_structures'
pdb_ids = "/home/zhn1744/AlphaFold/data/pdb_ids_v3.csv"
failed_ids_path = "/home/zhn1744/AlphaFold/data/pdb_pull_failed_ids_v3.csv"


os.makedirs(output, exist_ok=True)


# Grab all SwissProt IDs from Uniprot
pdb_ids = pd.read_csv(pdb_ids)
#pdb_ids = pdb_ids.iloc[:1]
pdb_ids.reset_index(drop=True, inplace=True)  # Reset index after slicing
cursor_index = 1
failed_index = 0
failed_ids = []


for i, row in pdb_ids.iterrows():
    print(f"Pull {i} of {len(pdb_ids)}")
    pdb_id = row["pdb_id"]
    URL = "https://data.rcsb.org/rest/v1/core/entry/" + pdb_id
    
    # Wait time ensures compliance with API rate limit
    wait_time = 0.05
    last_request_time = datetime.now()

    # Start the request
    print(URL)
    #start_time = datetime.now()
    #print(f"Request started at: {start_time}")
    try:
        response = requests.get(URL)
        #mid_time = datetime.now()
        #print(f"Request middle at: {mid_time}")
        # Define the path to save the file, incorporating the output directory
        file_path = os.path.join(output, f'response_entry_{pdb_id}.json')
        # Save the response as JSON
        with open(file_path, 'wb') as f:
            f.write(response.content)
        #end_time = datetime.now()
        #print(f"Request ended at: {end_time}")

    except:
        failed_index += 1
        print(f"Failed. Failed count is now {failed_index}")
        failed_ids.append(pdb_id)


    # Calculate the time to wait, ensuring at least `wait_time` seconds have passed since the last request
    time_elapsed = datetime.now() - last_request_time
    if time_elapsed < timedelta(seconds=wait_time):
        time_to_wait = (timedelta(seconds=wait_time) - time_elapsed).total_seconds()
        time.sleep(time_to_wait)

if failed_ids:
    failed_data = pd.DataFrame(data={'failed_ids': failed_ids})
    failed_data.to_csv(failed_ids_path, mode='a', index=False, header=not os.path.exists(failed_ids_path))




