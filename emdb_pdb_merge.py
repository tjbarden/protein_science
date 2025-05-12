import pandas as pd
import os
import sys

emdb_output = "/home/zhn1744/AlphaFold/data/emdb_structures.csv"

# Load the newly created CSV and the pdb_entries_emdb_ids.csv
new_csv = pd.read_csv(emdb_output)
pdb_emdb_mapping = pd.read_csv('/home/zhn1744/AlphaFold/data/pdb_entries_emdb_ids.csv')

# Split the emdb_ids column into a list and explode it into separate rows
pdb_emdb_mapping['emdb_ids'] = pdb_emdb_mapping['emdb_ids'].str.split(',')
pdb_emdb_exploded = pdb_emdb_mapping.explode('emdb_ids').rename(columns={'emdb_ids': 'emdb_id'})

# Strip any whitespace from emdb_id in both DataFrames to ensure clean merging
pdb_emdb_exploded['emdb_id'] = pdb_emdb_exploded['emdb_id'].str.strip()
new_csv['emdb_id'] = new_csv['emdb_id'].str.strip()

# Perform a left merge to keep all rows from new_csv and add corresponding pdb_id where available
merged_df = pd.merge(new_csv, pdb_emdb_exploded[['emdb_id', 'pdb_id']], 
                    on='emdb_id', 
                    how='left')

# Save the merged DataFrame back to the output file or a new file
merged_output = "/home/zhn1744/AlphaFold/data/emdb_empiar_merged.csv"
merged_df.to_csv(merged_output, index=False)

print("Merging complete! Output saved to:", merged_output)