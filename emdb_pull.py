import aiohttp
import asyncio
import json
import pandas as pd
from datetime import datetime, timedelta
import os
import csv
import time
from aiofiles import open as aio_open

# Define paths
output = '/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/emdb_json'
emdb_ids_path = "/home/zhn1744/AlphaFold/data/pdb_entries_emdb_ids.csv"
failed_ids_path = "/home/zhn1744/AlphaFold/data/emdb_pull_failed_ids.csv"

# Create output directory if it doesn't exist
os.makedirs(output, exist_ok=True)

# Get existing files
existing_files = set(os.listdir(output))

# Read and process EMDB IDs
emdb_ids = pd.read_csv(emdb_ids_path)
emdb_ids = emdb_ids[emdb_ids['emdb_ids'].notna() & (emdb_ids['emdb_ids'] != "")]
#emdb_ids = emdb_ids.iloc[:10]  # Keep original slice
emdb_ids.reset_index(drop=True, inplace=True)

print(f"Total EMDB IDs to process: {len(emdb_ids)}")

# Create list of IDs to process (excluding existing files)
to_process = []
for _, row in emdb_ids.iterrows():
    emdb_id = row["emdb_ids"]
    if emdb_id == "":
        continue
    filename = f'response_emdb_{emdb_id}.json'
    if filename not in existing_files:
        to_process.append(emdb_id)

print(f"EMDB IDs to download: {len(to_process)}")

failed_ids = []
wait_time = 0.05  # Original wait time

async def fetch_and_save(session, emdb_id):
    URL = f"https://www.ebi.ac.uk/emdb/api/entry/{emdb_id}"
    file_path = os.path.join(output, f'response_emdb_{emdb_id}.json')
    
    try:
        async with session.get(URL) as response:
            content = await response.read()
            async with aio_open(file_path, 'wb') as f:
                await f.write(content)
            #print(f"Successfully processed {emdb_id}")
    except Exception as e:
        #print(f"Failed to process {emdb_id}: {str(e)}")
        failed_ids.append(emdb_id)
        
    # Maintain original rate limiting
    await asyncio.sleep(wait_time)

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = []
        for emdb_id in to_process:
            task = asyncio.ensure_future(fetch_and_save(session, emdb_id))
            tasks.append(task)
        await asyncio.gather(*tasks)
    
    # Save failed IDs (keeping original functionality)
    if failed_ids:
        failed_data = pd.DataFrame(data={'failed_ids': failed_ids})
        failed_data.to_csv(
            failed_ids_path, 
            mode='a', 
            index=False, 
            header=not os.path.exists(failed_ids_path)
        )

if __name__ == "__main__":
    asyncio.run(main())
    print("Run 1 Complete")
    # Get existing files
    existing_files = set(os.listdir(output))

    # Read and process EMDB IDs
    emdb_ids = pd.read_csv(emdb_ids_path)
    emdb_ids = emdb_ids[emdb_ids['emdb_ids'].notna() & (emdb_ids['emdb_ids'] != "")]
    #emdb_ids = emdb_ids.iloc[:10]  # Keep original slice
    emdb_ids.reset_index(drop=True, inplace=True)

    print(f"Total EMDB IDs to process: {len(emdb_ids)}")
    to_process = []
    for _, row in emdb_ids.iterrows():
        emdb_id = row["emdb_ids"]
        if emdb_id == "":
            continue
        filename = f'response_emdb_{emdb_id}.json'
        if filename not in existing_files:
            to_process.append(emdb_id)

    print(f"EMDB IDs to download: {len(to_process)}")
    time.sleep(500)
    asyncio.run(main())
    print("Run 2 Complete")
    # Get existing files
    existing_files = set(os.listdir(output))

    # Read and process EMDB IDs
    emdb_ids = pd.read_csv(emdb_ids_path)
    emdb_ids = emdb_ids[emdb_ids['emdb_ids'].notna() & (emdb_ids['emdb_ids'] != "")]
    #emdb_ids = emdb_ids.iloc[:10]  # Keep original slice
    emdb_ids.reset_index(drop=True, inplace=True)

    print(f"Total EMDB IDs to process: {len(emdb_ids)}")
    to_process = []
    for _, row in emdb_ids.iterrows():
        emdb_id = row["emdb_ids"]
        if emdb_id == "":
            continue
        filename = f'response_emdb_{emdb_id}.json'
        if filename not in existing_files:
            to_process.append(emdb_id)

    print(f"EMDB IDs to download: {len(to_process)}")
    time.sleep(500)
    asyncio.run(main())
    print("Run 3 Complete")