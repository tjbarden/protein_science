import os
import json
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from functools import partial

# Initialize paths
json_folder = '/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/pdb_entry_json'
emdb_output = "/home/zhn1744/AlphaFold/data/pdb_entries_emdb_ids.csv"
failed_output = "/home/zhn1744/AlphaFold/data/failed/emdb_entries_failed.csv"

# Clean up previous output
try:
    os.remove(emdb_output)
except:
    pass

def process_batch(file_paths):
    records = []
    failed = []
    
    for file_path in file_paths:
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
            
            pdb_id = data['rcsb_entry_container_identifiers']['entry_id']
            deposition_date = data['pdbx_database_status']['recvd_initial_deposition_date']
            release_date = data['rcsb_accession_info']['initial_release_date']
            emdb_ids = data['rcsb_entry_container_identifiers'].get('emdb_ids', [])
            
            resolution = None
            num_particles = None
            if 'em3d_reconstruction' in data:
                em_recon = data['em3d_reconstruction'][0]
                resolution = em_recon.get('resolution')
                num_particles = em_recon.get('num_particles')
            
            records.append({
                'pdb_id': pdb_id,
                'emdb_ids': ','.join(emdb_ids) if emdb_ids else None,
                'deposition_date': deposition_date,
                'release_date': release_date,
                'resolution': resolution,
                'num_particles': num_particles
            })
            
        except Exception as e:
            failed.append(os.path.basename(file_path))
    
    return records, failed

# Get list of JSON files
file_paths = [os.path.join(json_folder, f) for f in os.listdir(json_folder)
              if f.startswith("response_entry_") and f.endswith(".json")]
total_files = len(file_paths)
print(f"Processing {total_files} files")

# Split files into batches
batch_size = 1000
batches = [file_paths[i:i + batch_size] for i in range(0, len(file_paths), batch_size)]

failed_filenames = []
with ProcessPoolExecutor() as executor:
    for i, (batch_records, batch_failed) in enumerate(executor.map(process_batch, batches), 1):
        if batch_records:
            df = pd.DataFrame(batch_records)
            df.to_csv(emdb_output, mode='a', index=False, header=not os.path.exists(emdb_output))
        
        failed_filenames.extend(batch_failed)
        print(f"Processed batch {i} of {len(batches)}")

# Write failed files
if failed_filenames:
    pd.DataFrame({'failed_filenames': failed_filenames}).to_csv(failed_output, index=False)
    print(f"Failed to process {len(failed_filenames)} files")