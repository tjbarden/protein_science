import os
import json
import pandas as pd
from collections import defaultdict
import csv
import multiprocessing as mp
from functools import partial
import time

# Set the folder path where the JSON files are located
json_folder = '/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/pdb_json_entities'
#json_folder = '/home/zhn1744/AlphaFold/data/pdb/entity'
json_folder = '/home/zhn1744/AlphaFold/data/pdb/entity/missing'

# Output path for combined data
combined_output = "/home/zhn1744/AlphaFold/data/pdb/entity/missing/pdb_entities_combined_missing.csv"
#combined_output = "/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/pdb_entities_combined_missing.csv"
failed_output = "/home/zhn1744/AlphaFold/data/failed/pdb_entities_failed.csv"
temp_dir = "/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/temp"

# Create temp directory if it doesn't exist
os.makedirs(temp_dir, exist_ok=True)

# Get list of JSON files in the folder
file_paths = [os.path.join(json_folder, f) for f in os.listdir(json_folder)
                if f.endswith(".json")]
#file_paths = file_paths[:25]
total_files = len(file_paths)
print(f"Total files to process: {total_files}")

# Helper function to sanitize and standardize values
def sanitize_value(value):
    """Clean and standardize values for CSV output"""
    # Convert lists and dicts to JSON strings to avoid CSV issues
    if isinstance(value, (list, dict)):
        try:
            return json.dumps(value)
        except:
            return str(value)
    # Convert None to empty string
    elif value is None:
        return ""
    # Convert everything else to string
    else:
        return str(value)

def process_special_array(data, key):
    """
    Process special array fields that need to be concatenated
    
    Args:
        data: The array data
        key: The key for the array field
        
    Returns:
        Concatenated string value
    """
    if key == 'rcsb_cluster_membership':
        # Format: cluster_id:identity#cluster_id:identity...
        cluster_strings = []
        for item in data:
            cluster_id = item.get('cluster_id', '')
            identity = item.get('identity', '')
            cluster_strings.append(f"{cluster_id}:{identity}")
        return "#".join(cluster_strings)

    elif key == 'taxonomy_lineage':
        # Format taxonomy lineage as a compact string
        taxonomy_strings = []
        for item in data:
            depth = item.get('depth', '')
            id_val = item.get('id', '')
            name = item.get('name', '')
            taxonomy_strings.append(f"{depth}:{id_val}:{name}")
        return "#".join(taxonomy_strings)
    
    elif key in ('rcsb_ec_lineage', 'annotation_lineage'):
        # Format lineage data
        lineage_strings = []
        for item in data:
            id_val = item.get('id', '')
            name = item.get('name', '')
            depth = item.get('depth', '')
            if depth:
                lineage_strings.append(f"{depth}:{id_val}:{name}")
            else:
                lineage_strings.append(f"{id_val}:{name}")
        return "#".join(lineage_strings)
    
    elif key == 'aligned_regions':
        # Format aligned regions
        region_strings = []
        for region in data:
            beg = region.get('entity_beg_seq_id', '')
            length = region.get('length', '')
            ref_beg = region.get('ref_beg_seq_id', '')
            region_strings.append(f"{beg}:{length}:{ref_beg}")
        return "#".join(region_strings)
    
    elif key == 'ncbi_common_names':
        # Join common names with #
        return "#".join(data)
    
    elif key == 'names':
        # Join names with #
        return "#".join(data)
    
    elif key == 'rcsb_macromolecular_names_combined':
        # Process macromolecular names
        name_strings = []
        for item in data:
            name = item.get('name', '')
            provenance = item.get('provenance_source', '')
            name_strings.append(f"{name}:{provenance}")
        return "#".join(name_strings)
    
    # Default: return JSON string
    return sanitize_value(data)

def extract_fields(data, prefix='', path=None):
    """
    Recursively extract fields from data, handling arrays
    
    Args:
        data: The dictionary/JSON data to extract from
        prefix: Prefix to add to column names (for nested structures)
        path: Current path in the JSON structure (for tracking)
        
    Returns:
        Dictionary with flattened field values
    """
    if path is None:
        path = []
        
    result = {}
    
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = path + [key]
            
            # Don't add prefix for the top-level fields (preserve original field name)
            new_prefix = key
            
            # If this is a simple value (not dict or list), add it
            if not isinstance(value, (dict, list)):
                result[new_prefix] = sanitize_value(value)
            
            # If it's a nested dict, recurse
            elif isinstance(value, dict):
                nested_result = extract_fields(value, key, new_path)
                result.update(nested_result)
            
            # If it's a list, handle as array
            elif isinstance(value, list):
                # Special handling for certain array fields
                if key in ('names', 'taxonomy_lineage', 'ncbi_common_names', 'rcsb_ec_lineage',
                        'aligned_regions', 'rcsb_macromolecular_names_combined',
                        'aligned_target', 'pubmed_ids', 'values', 'annotation_lineage', 'rcsb_cluster_membership'):
                    # Process these special arrays
                    result[new_prefix] = process_special_array(value, key)
                
                # For most arrays, only keep the first element
                elif len(value) > 0:
                    # Take just the first element
                    item = value[0]
                    if isinstance(item, dict):
                        for k, v in item.items():
                            item_prefix = f"{key}1_{k}"
                            result[item_prefix] = sanitize_value(v)
                    else:
                        result[f"{key}1"] = sanitize_value(item)
    
    return result

def process_file(file_path, batch_id):
    """Process a single file and return extracted data"""
    try:
        # Load the JSON data
        with open(file_path, "r") as f:
            data = json.load(f)
            
            # Extract entity_id and entry_id from container identifiers
            entity_id = data.get('rcsb_polymer_entity_container_identifiers', {}).get('entity_id', "")
            entry_id = data.get('rcsb_polymer_entity_container_identifiers', {}).get('entry_id', "")
            
            # For entity files that might not have these fields, extract from rcsb_id
            if not entry_id and 'rcsb_id' in data:
                parts = data['rcsb_id'].split('_')
                if len(parts) >= 2:
                    entry_id = parts[0]
                    entity_id = parts[1]
            
            # Create base entry with core identifiers
            entry_dict = {
                'entity_id': entity_id,
                'entry_id': entry_id
            }
            
            # Dynamically extract all fields
            for key, value in data.items():
                # Skip fields already in entry_dict
                if key in entry_dict:
                    continue
                    
                # Skip certain metadata fields we don't need to process separately
                if key in ('rcsb_polymer_entity_container_identifiers'):
                    continue
                
                # Handle different field types
                if isinstance(value, dict):
                    # Handle dictionary fields - extract nested structure
                    extracted = extract_fields(value, key)
                    entry_dict.update(extracted)
                
                elif isinstance(value, list):
                    # Special handling for special array fields
                    if key in ('taxonomy_lineage', 'rcsb_ec_lineage', 'ncbi_common_names', 
                        'aligned_regions', 'rcsb_macromolecular_names_combined', 
                        'names', 'aligned_target', 'pubmed_ids', 'values',
                        'annotation_lineage', 'rcsb_cluster_membership'):
                        entry_dict[key] = process_special_array(value, key)
                    
                    # For other arrays, take the first element only
                    elif len(value) > 0:
                        item = value[0]
                        if isinstance(item, dict):
                            for k, v in item.items():
                                item_key = f"{key}1_{k}"
                                entry_dict[item_key] = sanitize_value(v)
                        else:
                            entry_dict[f"{key}1"] = sanitize_value(item)
                
                else:
                    # Simple scalar field
                    entry_dict[key] = sanitize_value(value)
            
            return (entry_dict, None)
            
    except Exception as e:
        print(f"Failed to load or process {file_path}: {e}")
        return (None, os.path.basename(file_path))

def write_batch_results(batch_results, batch_id):
    """Write batch results to temporary files"""
    entries = []
    failures = []
    
    for result in batch_results:
        entry, failure = result
        if entry:
            entries.append(entry)
        if failure:
            failures.append(failure)
    
    # Save the batch results to a temp file
    entries_file = os.path.join(temp_dir, f"entries_batch_{batch_id}.json")
    failures_file = os.path.join(temp_dir, f"failures_batch_{batch_id}.json")
    
    with open(entries_file, 'w') as f:
        json.dump(entries, f)
    
    with open(failures_file, 'w') as f:
        json.dump(failures, f)
    
    print(f"Batch {batch_id} complete: {len(entries)} entries processed, {len(failures)} failures")

def main():
    
    # Remove existing output files if they exist
    try:
        os.remove(combined_output)
    except:
        pass
    
    try:
        os.remove(failed_output)
    except:
        pass
    
    # Clear any existing temp files
    for f in os.listdir(temp_dir):
        if f.startswith("entries_batch_") or f.startswith("failures_batch_"):
            os.remove(os.path.join(temp_dir, f))
    
    # Determine optimal batch size and number of processes
    num_cores = 18
    print(f"Using {num_cores} CPU cores")
    num_processes = max(1, min(num_cores - 1, 18))  # Leave one core free
    
    # We want at least 100 files per batch for efficiency
    batch_size = max(100, (total_files + num_processes - 1) // num_processes)
    num_batches = (total_files + batch_size - 1) // batch_size
    
    print(f"Processing {total_files} files in {num_batches} batches of ~{batch_size} files each")
    print(f"Using {num_processes} parallel processes")
    
    # Process batches
    start_time = time.time()
    
    with mp.Pool(processes=num_processes) as pool:
        for batch_id in range(num_batches):
            batch_start = batch_id * batch_size
            batch_end = min(batch_start + batch_size, total_files)
            
            print(f"Processing batch {batch_id+1}/{num_batches} (files {batch_start+1}-{batch_end})")
            
            # Process batch of files
            batch_files = file_paths[batch_start:batch_end]
            
            # Create a partial function with batch_id
            process_func = partial(process_file, batch_id=batch_id)
            
            # Process files in parallel
            batch_results = pool.map(process_func, batch_files)
            
            # Write batch results to temporary files
            write_batch_results(batch_results, batch_id)
    
    processing_time = time.time() - start_time
    print(f"File processing completed in {processing_time:.2f} seconds")
    
    # Now collect all the entries and failures
    print("Combining results from all batches...")
    all_entries = []
    all_failures = []
    all_columns = set(['entity_id', 'entry_id'])
    
    for batch_id in range(num_batches):
        entries_file = os.path.join(temp_dir, f"entries_batch_{batch_id}.json")
        failures_file = os.path.join(temp_dir, f"failures_batch_{batch_id}.json")
        
        try:
            with open(entries_file, 'r') as f:
                batch_entries = json.load(f)
                
                for entry in batch_entries:
                    all_columns.update(entry.keys())
                
                all_entries.extend(batch_entries)
                
            with open(failures_file, 'r') as f:
                batch_failures = json.load(f)
                all_failures.extend(batch_failures)
                
            # Clean up temp files as we go
            os.remove(entries_file)
            os.remove(failures_file)
        except Exception as e:
            print(f"Error processing batch {batch_id}: {e}")
    
    # Convert columns set to sorted list
    all_columns = sorted(list(all_columns))
    print(f"Found {len(all_columns)} unique columns across {len(all_entries)} entries")
    print(f"{len(all_failures)} files failed to process")
    
    # Write to final CSV
    print("Writing data to CSV...")
    start_time = time.time()
    
    with open(combined_output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        # Write header row
        writer.writerow(all_columns)
        
        # Write data rows in batches to avoid memory issues
        entry_batch_size = 10000
        for i in range(0, len(all_entries), entry_batch_size):
            end = min(i + entry_batch_size, len(all_entries))
            print(f"Writing entries {i+1}-{end} of {len(all_entries)}")
            
            for entry_dict in all_entries[i:end]:
                row = [entry_dict.get(col, "") for col in all_columns]
                writer.writerow(row)
    
    # Save any failed filenames
    if all_failures:
        pd.DataFrame({'failed_filenames': all_failures}).to_csv(failed_output, index=False)
    
    writing_time = time.time() - start_time
    print(f"CSV writing completed in {writing_time:.2f} seconds")
    print(f"Total processing complete. Processed {total_files} files with {len(all_failures)} failures.")

if __name__ == "__main__":
    main()