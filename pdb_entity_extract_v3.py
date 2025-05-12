import os
import json
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

# Initialize an empty list to temporarily store rows
buffer = []

# Set the folder path where the JSON files are located
json_folder = '/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/pdb_entity_json'

csv_output = "/home/zhn1744/AlphaFold/data/pdb_entities_v3.csv"
failed_output = "/home/zhn1744/AlphaFold/data/failed/pdb_entities_failed_v3.csv"
cursor_index = 0

# Remove existing output files, if any
try:
    os.remove(csv_output)
except:
    pass

# Get list of JSON file paths
file_paths = [os.path.join(json_folder, f) for f in os.listdir(json_folder)
              if f.startswith("response_entry_") and f.endswith(".json")]
total_files = len(file_paths)
print(total_files)

# Function to process a single file
def process_file(file_path):
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        
        rcsb_polymer_entity_align_object = data.get("rcsb_polymer_entity_align", [{}])[0] if len(data.get("rcsb_polymer_entity_align", [])) > 0 else {}
        uniprot_acc = rcsb_polymer_entity_align_object.get("reference_database_accession", "N/A")
        uniprot_ref = rcsb_polymer_entity_align_object.get("reference_database_name", "N/A")
        rcsb_polymer_entity = data.get("rcsb_polymer_entity", {})
        rcsb_macromolecular_names_combined = rcsb_polymer_entity.get("rcsb_macromolecular_names_combined", ["N/A"])[0]
        rcsb_id = data.get("rcsb_id", "N/A")

        rcsb_entity_source_organism = data.get("rcsb_entity_source_organism")
        rcsb_entity_host_organism = data.get("rcsb_entity_host_organism")
        organism_string = ""
        expression_string = ""
        gene_name = ""

        if rcsb_entity_source_organism is not None:
            for organism_object in rcsb_entity_source_organism:
                organism_string += organism_object.get("ncbi_scientific_name", "N/A") + "%" + str(organism_object.get("ncbi_taxonomy_id", "0")) + "#"
                gene_names = organism_object.get("rcsb_gene_name", [])
                gene_name += "#".join(g.get("value", "N/A") for g in gene_names) + "#"
        
        if rcsb_entity_host_organism is not None:
            for organism_object in rcsb_entity_host_organism:
                expression_string += organism_object.get("ncbi_scientific_name", "N/A") + "%" + str(organism_object.get("ncbi_taxonomy_id", "0")) + "#"

        entity_poly = data.get("entity_poly", {})

        # Return processed data as a dictionary
        return {
            'organism': organism_string,
            'expression_system': expression_string,
            'gene_name': gene_name,
            'mutation_count': entity_poly.get("rcsb_mutation_count"),
            'rcsb_id': rcsb_id,
            "rcsb_polymer_entity_name_pdb": rcsb_polymer_entity.get("pdbx_description", "N/A"),
            "pdbx_num_of_molecules": rcsb_polymer_entity.get("pdbx_number_of_molecules", "N/A"),
            'uniprot_acc': uniprot_acc,
            'uniprot_ref': uniprot_ref
        }
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return {"failed_file": os.path.basename(file_path)}

# Process files with concurrent.futures
failed_filenames = []
with ProcessPoolExecutor() as executor:
    for index, result in enumerate(executor.map(process_file, file_paths), start=1):
        if index % 1000 == 0:
            print(f"{index} of {total_files} processed")
        
        # Check if the result indicates a failure
        if "failed_file" in result:
            failed_filenames.append(result["failed_file"])
        else:
            buffer.append(result)

        # Write to CSV when buffer reaches size of 5000 or at the end
        if len(buffer) >= 5000 or cursor_index == total_files - 1:
            protein_data = pd.DataFrame(buffer)
            protein_data.to_csv(csv_output, mode='a', index=False, header=not os.path.exists(csv_output))
            buffer.clear()  # Clear the buffer

        cursor_index += 1

# Write remaining rows in buffer, if any
if buffer:
    protein_data = pd.DataFrame(buffer)
    protein_data.to_csv(csv_output, mode='a', index=False, header=not os.path.exists(csv_output))

# Write failed filenames to a CSV
if failed_filenames:
    pd.DataFrame({'failed_filenames': failed_filenames}).to_csv(failed_output, index=False)
