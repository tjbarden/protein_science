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
output = '/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/empiar_json'
json_folder = '/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/emdb_json'
temp_ids = []

for filename in os.listdir(json_folder):
    print(filename)
    if ',' in filename:  # Handle files with multiple IDs
        id_list = filename.split(',')
        for id_str in id_list:
            print(id_str)
            id_str = id_str.replace('response_emdb_', '')
            id_str = id_str.replace('.json', '')
            num = id_str
            temp_ids.append(num)
    else:
        id_str = filename
        id_str = id_str.replace('response_emdb_', '')
        id_str = id_str.replace('.json', '')
        emdb_id = id_str
        temp_ids.append(emdb_id)

emdb_ids = pd.DataFrame(temp_ids, columns=['emdb_ids'])
failed_ids_path = "/home/zhn1744/AlphaFold/data/empiar_pull_failed_ids.csv"
os.makedirs(output, exist_ok=True)

emdb_ids = emdb_ids[emdb_ids['emdb_ids'].notna() & (emdb_ids['emdb_ids'] != "")]
emdb_ids.reset_index(drop=True, inplace=True)
print(f"Total EMDB IDs to process: {len(emdb_ids)}")

existing_files = set(os.listdir(output))
to_process = []
for _, row in emdb_ids.iterrows():
    emdb_id = row["emdb_ids"]
    if emdb_id == "":
        continue
    filename = f'response_empiar_{emdb_id}.json'
    if filename not in existing_files:
        to_process.append(emdb_id)

print(f"EMDB IDs to download: {len(to_process)}")

failed_ids = []
wait_time = 0.05  # Original wait time

async def fetch_and_save(session, emdb_id, semaphore):
    async with semaphore:
        URL = f"https://www.ebi.ac.uk/empiar/api/emdb_ref/{emdb_id}"
        file_path = os.path.join(output, f'response_empiar_{emdb_id}.json')
        try:
            async with session.get(URL) as response:
                content = await response.read()
                # Insert the emdb_id directly into the JSON string.
                # Assumes the JSON object starts with '{'
                content_str = content.decode("utf-8")
                insert_str = f'"emdb_id": "{emdb_id}",'
                content_str = content_str.replace("{", "{" + insert_str, 1)
                content = content_str.encode("utf-8")
                async with aio_open(file_path, 'wb') as f:
                    await f.write(content)
        except Exception as e:
            print(f"Error processing {emdb_id}: {e}")
            failed_ids.append(emdb_id)
        await asyncio.sleep(wait_time)

async def main():
    # Create the semaphore inside main so it's attached to the current event loop.
    semaphore = asyncio.Semaphore(5)
    timeout = aiohttp.ClientTimeout(total=30)  # Adjust the timeout as needed
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for emdb_id in to_process:
            task = asyncio.ensure_future(fetch_and_save(session, emdb_id, semaphore))
            tasks.append(task)
        await asyncio.gather(*tasks)
    
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
    
    # Run 2
    existing_files = set(os.listdir(output))
    to_process = []
    for _, row in emdb_ids.iterrows():
        emdb_id = row["emdb_ids"]
        if emdb_id == "":
            continue
        filename = f'response_empiar_{emdb_id}.json'
        if filename not in existing_files:
            to_process.append(emdb_id)
    print(f"EMDB IDs to download: {len(to_process)}")
    time.sleep(500)
    asyncio.run(main())
    print("Run 2 Complete")
    
    # Run 3
    existing_files = set(os.listdir(output))
    to_process = []
    for _, row in emdb_ids.iterrows():
        emdb_id = row["emdb_ids"]
        if emdb_id == "":
            continue
        filename = f'response_empiar_{emdb_id}.json'
        if filename not in existing_files:
            to_process.append(emdb_id)
    print(f"EMDB IDs to download: {len(to_process)}")
    time.sleep(500)
    asyncio.run(main())
    print("Run 3 Complete")
    
    # Run 4
    existing_files = set(os.listdir(output))
    to_process = []
    for _, row in emdb_ids.iterrows():
        emdb_id = row["emdb_ids"]
        if emdb_id == "":
            continue
        filename = f'response_empiar_{emdb_id}.json'
        if filename not in existing_files:
            to_process.append(emdb_id)
    print(f"EMDB IDs to download: {len(to_process)}")
    time.sleep(500)
    asyncio.run(main())
    print("Run 4 Complete")
    
    # Run 5
    existing_files = set(os.listdir(output))
    to_process = []
    for _, row in emdb_ids.iterrows():
        emdb_id = row["emdb_ids"]
        if emdb_id == "":
            continue
        filename = f'response_empiar_{emdb_id}.json'
        if filename not in existing_files:
            to_process.append(emdb_id)
    print(f"EMDB IDs to download: {len(to_process)}")
    time.sleep(500)
    asyncio.run(main())
    print("Run 5 Complete")
