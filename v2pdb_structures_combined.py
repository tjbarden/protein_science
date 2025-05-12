import os
import json
import pandas as pd
from collections import defaultdict
import csv

# Set the folder path where the JSON files are located
json_folder = '/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/pdb_json_structures'
#json_folder = '/home/zhn1744/AlphaFold/data/pdb/entry_v2'

# Output path for combined data
combined_output = "/home/zhn1744/AlphaFold/data/pdb/pdb_structures_combined.csv"
failed_output = "/home/zhn1744/AlphaFold/data/failed/pdb_structures_failed.csv"

failed_filenames = []

# Remove existing output file if it exists
try:
    os.remove(combined_output)
except:
    pass

# Get list of JSON files in the folder
file_paths = [os.path.join(json_folder, f) for f in os.listdir(json_folder)
              if f.startswith("response_entry_") and f.endswith(".json")]
#file_paths = file_paths[:25]
total_files = len(file_paths)
print(f"Total files to process: {total_files}")

# Dictionary to track which array fields we've seen and their maximum indices
array_fields = defaultdict(int)

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
        data: The dictionary/JSON data
        key: The key for the array field
        
    Returns:
        Concatenated string value
    """
    if key == 'audit_author':
        # Format: name$identifier#name$identifier
        author_strings = []
        for author in data:
            name = author.get('name', '')
            ordinal = author.get('pdbx_ordinal', '')
            orcid = author.get('identifier_orcid', '')
            author_strings.append(f"{name}${ordinal}${orcid}")
        return "#".join(author_strings)
    
    elif key == 'pdbx_database_related':
        # Concatenate db_ids with #
        db_ids = [item.get('db_id', '') for item in data]
        return "#".join(db_ids)
    
    # For other special cases, add them here
    
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
            new_prefix = f"{prefix}_{key}" if prefix else key
            
            # If this is a simple value (not dict or list), add it
            if not isinstance(value, (dict, list)):
                result[new_prefix] = sanitize_value(value)
            
            # If it's a nested dict, recurse
            elif isinstance(value, dict):
                # Check if it's a special nested dict that should be kept as is
                if key == 'diffrn_resolution_high':
                    # Handle this special case directly
                    for k, v in value.items():
                        nested_key = f"{new_prefix}_{k}"
                        result[nested_key] = sanitize_value(v)
                else:
                    nested_result = extract_fields(value, new_prefix, new_path)
                    result.update(nested_result)
            
            # If it's a list, handle as array
            elif isinstance(value, list):
                # Special handling for certain array fields
                if key in ('resolution_combined', 'nonpolymer_bound_components', 
                           'ndb_struct_conf_na_feature_combined', 'rcsb_authors',
                           'pdbx_diffrn_id', 'software_programs_combined',
                           'rcsb_orcididentifiers'):
                    result[new_prefix] = sanitize_value(value)
                
                # Special handling for audit_author and pdbx_database_related
                elif key in ('audit_author', 'pdbx_database_related'):
                    result[new_prefix] = process_special_array(value, key)
                
                # For most array fields, only keep the first element but add '1' in the name
                else:
                    if len(value) > 0:
                        # For citations, we only want to keep citation1 and rcsb_primary_citation
                        if key == 'citation' and len(value) > 0:
                            item = value[0]  # Get the first citation only
                            for k, v in item.items():
                                item_prefix = f"{new_prefix}1_{k}"
                                result[item_prefix] = sanitize_value(v)
                        else:
                            # For other arrays, take the first element and add '1' in name
                            item = value[0]
                            if isinstance(item, dict):
                                for k, v in item.items():
                                    item_prefix = f"{new_prefix}1_{k}"
                                    result[item_prefix] = sanitize_value(v)
                            else:
                                result[f"{new_prefix}1"] = sanitize_value(item)
    
    return result

print("Processing PDB entries...")
# Process all files and create entries
all_entries = []
all_columns = set(['pdb_id', 'deposition_date', 'release_date'])

for index, file_path in enumerate(file_paths, start=1):
    print(f"{index} of {total_files}")
    
    try:
        # Load the JSON data
        with open(file_path, "r") as f:
            data = json.load(f)
            pdb_id = data['rcsb_entry_container_identifiers']['entry_id']
            
            # Handle potentially missing fields with safe access
            deposition_date = data.get('pdbx_database_status', {}).get('recvd_initial_deposition_date', "")
            release_date = data.get('rcsb_accession_info', {}).get('initial_release_date', "")
            
            # Create base entry with core identifiers
            entry_dict = {
                'pdb_id': pdb_id,
                'deposition_date': deposition_date,
                'release_date': release_date
            }
            
            # Special handling for audit_author - concatenate all with # separator
            if 'audit_author' in data:
                entry_dict['audit_author'] = process_special_array(data['audit_author'], 'audit_author')
            
            # Special handling for pdbx_database_related - concatenate all db_ids with # separator
            if 'pdbx_database_related' in data:
                entry_dict['pdbx_database_related_db_id'] = process_special_array(data['pdbx_database_related'], 'pdbx_database_related')
            
            # Dynamically extract all other fields
            for key, value in data.items():
                # Skip fields already handled
                if key in entry_dict or key in ('rcsb_entry_container_identifiers', 'pdbx_database_status', 'rcsb_accession_info', 'audit_author', 'pdbx_database_related'):
                    continue
                
                # Handle different field types
                if isinstance(value, dict):
                    # Handle dictionary fields - extract nested structure
                    extracted = extract_fields(value, key)
                    entry_dict.update(extracted)
                
                elif isinstance(value, list):
                    # Special handling for citation - only keep the first one
                    if key == 'citation' and len(value) > 0:
                        # Only keep citation1 fields
                        for k, v in value[0].items():
                            entry_dict[f"{key}1_{k}"] = sanitize_value(v)
                    
                    # For most arrays, only keep the first element
                    elif len(value) > 0:
                        if key in ('resolution_combined', 'nonpolymer_bound_components', 
                                  'ndb_struct_conf_na_feature_combined', 'software_programs_combined'):
                            # Keep these as full arrays
                            entry_dict[key] = sanitize_value(value)
                        else:
                            # For other arrays, take the first element but add '1' to the name
                            item = value[0]
                            if isinstance(item, dict):
                                for k, v in item.items():
                                    entry_dict[f"{key}1_{k}"] = sanitize_value(v)
                            else:
                                entry_dict[f"{key}1"] = sanitize_value(item)
                
                else:
                    # Simple scalar field
                    entry_dict[key] = sanitize_value(value)
            
            # Add all keys from this entry to the master set of columns
            all_columns.update(entry_dict.keys())
            
            # Store the entry
            all_entries.append(entry_dict)
            
    except Exception as e:
        print(f"Failed to load or process {file_path}: {e}")
        failed_filenames.append(os.path.basename(file_path))
        continue

# Convert set to list and sort for consistent ordering
all_columns = sorted(list(all_columns))
print(f"Found {len(all_columns)} unique columns across {len(all_entries)} entries.")

print("Writing data to CSV...")
# Now write the CSV with all discovered columns
with open(combined_output, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, quoting=csv.QUOTE_ALL)
    # Write header row
    writer.writerow(all_columns)
    
    # Write data rows
    for entry_index, entry_dict in enumerate(all_entries, 1):
        if entry_index % 1000 == 0:
            print(f"Writing entry {entry_index} of {len(all_entries)}")
        row = [entry_dict.get(col, "") for col in all_columns]
        writer.writerow(row)

# Save any failed filenames
if failed_filenames:
    pd.DataFrame({'failed_filenames': failed_filenames}).to_csv(failed_output, index=False)

print(f"Processing complete. Processed {total_files} files with {len(failed_filenames)} failures.")