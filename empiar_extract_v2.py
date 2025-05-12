import os
import json
import pandas as pd

# Set paths
json_folder = '/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/empiar_json_v2'
csv_output = "/home/zhn1744/AlphaFold/data/empiar_structures.csv"
emdb_file = '/home/zhn1744/AlphaFold/data/emdb_structures.csv'
merged_output = '/home/zhn1744/AlphaFold/data/merged_empiar_emdb.csv'

def extract_empiar_data(json_folder, csv_output):
    # Initialize empty lists to store data
    data_list = []

    # Get list of JSON files
    file_paths = [os.path.join(json_folder, f) for f in os.listdir(json_folder)
                  if f.endswith('.json')]

    # Process each file
    for file_path in file_paths:
        try:
            print(f"Processing {file_path}")
            
            # Load the JSON data
            with open(file_path, 'r') as f:
                data = json.load(f)
                
            # Find the EMPIAR entry key (should start with 'EMPIAR-')
            empiar_key = next((key for key in data.keys() if key.startswith('EMPIAR-')), None)
            
            if empiar_key is None:
                print(f"Warning: No EMPIAR key found in {file_path}")
                continue
                
            entry_data = data[empiar_key]
            
            # Extract the required fields using .get() to handle missing data
            release_date = entry_data.get('release_date', 'N/A')
            update_date = entry_data.get('update_date', 'N/A')
            title = entry_data.get('title', 'N/A')
            scale = entry_data.get('scale', 'N/A')
            
            # Handle cross references
            cross_refs = entry_data.get('cross_references', [])
            cross_refs_str = '#'.join(ref.get('name', '') for ref in cross_refs) if cross_refs else 'N/A'
            
            # Create dictionary for this entry
            entry_dict = {
                'empiar_id': empiar_key,
                'release_date': release_date,
                'update_date': update_date,
                'title': title,
                'scale': scale,
                'cross_references': cross_refs_str
            }
            
            data_list.append(entry_dict)
            print(f"Successfully processed {empiar_key}")
            
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")

    # Create DataFrame and save to CSV
    if data_list:
        df = pd.DataFrame(data_list)
        df.to_csv(csv_output, index=False)
        print(f"Data saved to {csv_output}")
        return df
    else:
        print("No data was processed successfully")
        return None

def merge_with_emdb(empiar_df, emdb_file_path, output_path):
    print("\nStarting merge process...")
    
    # Read the EMDB entries file
    emdb_df = pd.read_csv(emdb_file_path)
    print(f"Loaded EMDB data with {len(emdb_df)} entries")

    # Create a copy of the original EMPIAR dataframe for processing
    df = empiar_df.copy()
    
    # Split the cross_references column and explode to create separate rows for each EMDB ID
    df['emdb_id'] = df['cross_references'].str.split('#').apply(lambda x: [id for id in x if 'EMD-' in id])
    df = df.explode('emdb_id')
    
    # Clean up the EMDB IDs to match format in emdb_entries.csv
    df['emdb_id'] = df['emdb_id'].str.extract(r'(EMD-\d+)')
    
    # Before merging, keep a copy of original EMPIAR IDs
    empiar_ids = df['empiar_id'].unique()
    print(f"Found {len(empiar_ids)} unique EMPIAR IDs")

    # Merge the dataframes with left join to keep all EMDB entries
    merged_df = pd.merge(emdb_df, df, on='emdb_id', how='left')
    
    # Find EMPIAR entries that didn't merge
    merged_empiar_ids = merged_df['empiar_id'].unique()
    unmatched_empiar = set(empiar_ids) - set(merged_empiar_ids[~pd.isna(merged_empiar_ids)])

    print(f"\nEMPIAR entries that didn't find EMDB matches:")
    print(unmatched_empiar)
    print(f"Number of unmatched EMPIAR entries: {len(unmatched_empiar)}")

    # Save the merged dataframe
    merged_df.to_csv(output_path, index=False)
    print(f"Merged data saved to {output_path}")
    
    return merged_df

def main():
    
    # Extract EMPIAR data
    empiar_df = extract_empiar_data(json_folder, csv_output)
    
    if empiar_df is not None:
        # Merge with EMDB data
        merge_with_emdb(empiar_df, emdb_file, merged_output)

if __name__ == "__main__":
    main()