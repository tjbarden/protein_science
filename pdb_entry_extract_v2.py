import os
import json
import pandas as pd

# Read missing PDB IDs from CSV
missing_pdb_ids_file = "/home/zhn1744/AlphaFold/data/missing_pdb_ids.csv"  # Update this path to the correct location
missing_pdb_ids = pd.read_csv(missing_pdb_ids_file)['pdb_id'].tolist()

# Set the folder path where the JSON files are located
json_folder = '/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/pdb_entry_json'

# Get list of JSON files in the folder
all_file_paths = [os.path.join(json_folder, f) for f in os.listdir(json_folder)
                  if f.startswith("response_entry_") and f.endswith(".json")]

# Filter file_paths to include only those with PDB IDs in missing_pdb_ids
file_paths = [f for f in all_file_paths if os.path.basename(f)[len("response_entry_"):-len(".json")] in missing_pdb_ids]

# Print the number of files to be processed
total_files = len(file_paths)
print(f"Total files to process: {total_files}")

# Initialize empty DataFrames
citation_data = pd.DataFrame(columns=[
    'pdb_id', 'deposition_date', 'release_date', 'country', 'id', 'journal_abbrev', 'journal_id_astm',
    'journal_id_csd', 'journal_id_issn', 'journal_volume', 'page_first', 'page_last', 'pdbx_database_id_doi',
    'pdbx_database_id_pub_med', 'rcsb_authors', 'rcsb_journal_abbrev', 'title', 'year'
])
refine_data = pd.DataFrame(columns=[
    'pdb_id', 'deposition_date', 'release_date', 'ls_rfactor_rfree', 'ls_rfactor_rwork', 'ls_rfactor_obs',
    'ls_dres_high', 'ls_dres_low', 'ls_number_reflns_rfree', 'ls_number_reflns_obs', 'ls_percent_reflns_rfree',
    'ls_percent_reflns_obs', 'pdbx_rfree_selection_details', 'pdbx_data_cutoff_high_rms_abs_f',
    'pdbx_ls_cross_valid_method', 'pdbx_ls_sigma_f', 'pdbx_method_to_determine_struct', 'pdbx_refine_id',
    'solvent_model_details', 'solvent_model_param_bsol', 'solvent_model_param_ksol', 'biso_mean'
])
entry_data = pd.DataFrame(columns=[
    'pdb_id', 'deposition_date', 'release_date', 'assembly_count', 'branched_entity_count', 'cis_peptide_count',
    'deposited_atom_count', 'deposited_deuterated_water_count', 'deposited_hydrogen_atom_count',
    'deposited_model_count', 'deposited_modeled_polymer_monomer_count', 'deposited_nonpolymer_entity_instance_count',
    'deposited_polymer_entity_instance_count', 'deposited_polymer_monomer_count', 'deposited_solvent_atom_count',
    'deposited_unmodeled_polymer_monomer_count', 'diffrn_radiation_wavelength_maximum',
    'diffrn_radiation_wavelength_minimum', 'disulfide_bond_count', 'entity_count', 'experimental_method',
    'experimental_method_count', 'inter_mol_covalent_bond_count', 'inter_mol_metalic_bond_count', 'molecular_weight',
    'na_polymer_entity_types', 'nonpolymer_bound_components', 'nonpolymer_entity_count',
    'nonpolymer_molecular_weight_maximum', 'nonpolymer_molecular_weight_minimum', 'polymer_composition',
    'polymer_entity_count', 'polymer_entity_count_dna', 'polymer_entity_count_rna', 'polymer_entity_count_nucleic_acid',
    'polymer_entity_count_nucleic_acid_hybrid', 'polymer_entity_count_protein', 'polymer_entity_taxonomy_count',
    'polymer_molecular_weight_maximum', 'polymer_molecular_weight_minimum', 'polymer_monomer_count_maximum',
    'polymer_monomer_count_minimum', 'resolution_combined', 'selected_polymer_entity_types',
    'software_programs_combined', 'solvent_entity_count', 'structure_determination_methodology',
    'structure_determination_methodology_priority', 'diffrn_resolution_high_provenance_source',
    'diffrn_resolution_high_value'
])

citation_output = "/home/zhn1744/AlphaFold/data/pdb_structures_citation.csv"
refine_output = "/home/zhn1744/AlphaFold/data/pdb_structures_refine.csv"
entry_output = "/home/zhn1744/AlphaFold/data/pdb_structures_entry.csv"
failed_output = "/home/zhn1744/AlphaFold/data/failed/pdb_structures_failed.csv"

failed_filenames = []

cursor_index = 0

try:
    os.remove(citation_output)
    os.remove(refine_output)
    os.remove(entry_output)
except:
    pass

# Loop through files in the folder
for index, file_path in enumerate(file_paths, start=1):
    print(f"{index} of {total_files}")
    
    try:
        # Load the JSON data
        with open(file_path, "r") as f:
            data = json.load(f)
            print(file_path)
            
            pdb_id = data['rcsb_entry_container_identifiers']['entry_id']
            deposition_date = data.get('pdbx_database_status', {}).get('recvd_initial_deposition_date')
            release_date = data.get('rcsb_accession_info', {}).get('initial_release_date')
            
            print(f"Processing {pdb_id}")
    except Exception as e:
        print(f"Failed to load or process {file_path}: {e}")
        failed_filenames.append(os.path.basename(file_path))
        continue

    rcsb_entry_info = data.get("rcsb_entry_info", {})
    refine = data.get("refine", [{}])[0]
    rcsb_primary_citation = data.get("rcsb_primary_citation", {})

    citation_df = pd.DataFrame({
        'pdb_id': [pdb_id],
        'deposition_date': [deposition_date],
        'release_date': [release_date],
        "country": [rcsb_primary_citation.get("country")],
        "id": [rcsb_primary_citation.get("id")],
        "journal_abbrev": [rcsb_primary_citation.get("journal_abbrev")],
        "journal_id_astm": [rcsb_primary_citation.get("journal_id_astm")],
        "journal_id_csd": [rcsb_primary_citation.get("journal_id_csd")],
        "journal_id_issn": [rcsb_primary_citation.get("journal_id_issn")],
        "journal_volume": [rcsb_primary_citation.get("journal_volume")],
        "page_first": [rcsb_primary_citation.get("page_first")],
        "page_last": [rcsb_primary_citation.get("page_last")],
        "pdbx_database_id_doi": [rcsb_primary_citation.get("pdbx_database_id_doi")],
        "pdbx_database_id_pub_med": [rcsb_primary_citation.get("pdbx_database_id_pub_med")],
        "rcsb_authors": [rcsb_primary_citation.get("rcsb_authors")],
        "rcsb_journal_abbrev": [rcsb_primary_citation.get("rcsb_journal_abbrev")],
        "title": [rcsb_primary_citation.get("title")],
        "year": [rcsb_primary_citation.get("year")]
    })

    print(f"Citation Data for {pdb_id}:")
    print(citation_df)

    if not citation_df.empty:
        citation_data = pd.concat([citation_data, citation_df], ignore_index=True, sort=False)

    rcsb_entry_df = pd.DataFrame({
        'pdb_id': [pdb_id],
        'deposition_date': [deposition_date],
        'release_date': [release_date],
        "assembly_count": [rcsb_entry_info.get("assembly_count")],
        "branched_entity_count": [rcsb_entry_info.get("branched_entity_count")],
        "cis_peptide_count": [rcsb_entry_info.get("cis_peptide_count")],
        "deposited_atom_count": [rcsb_entry_info.get("deposited_atom_count")],
        "deposited_deuterated_water_count": [rcsb_entry_info.get("deposited_deuterated_water_count")],
        "deposited_hydrogen_atom_count": [rcsb_entry_info.get("deposited_hydrogen_atom_count")],
        "deposited_model_count": [rcsb_entry_info.get("deposited_model_count")],
        "deposited_modeled_polymer_monomer_count": [rcsb_entry_info.get("deposited_modeled_polymer_monomer_count")],
        "deposited_nonpolymer_entity_instance_count": [rcsb_entry_info.get("deposited_nonpolymer_entity_instance_count")],
        "deposited_polymer_entity_instance_count": [rcsb_entry_info.get("deposited_polymer_entity_instance_count")],
        "deposited_polymer_monomer_count": [rcsb_entry_info.get("deposited_polymer_monomer_count")],
        "deposited_solvent_atom_count": [rcsb_entry_info.get("deposited_solvent_atom_count")],
        "deposited_unmodeled_polymer_monomer_count": [rcsb_entry_info.get("deposited_unmodeled_polymer_monomer_count")],
        "diffrn_radiation_wavelength_maximum": [rcsb_entry_info.get("diffrn_radiation_wavelength_maximum")],
        "diffrn_radiation_wavelength_minimum": [rcsb_entry_info.get("diffrn_radiation_wavelength_minimum")],
        "disulfide_bond_count": [rcsb_entry_info.get("disulfide_bond_count")],
        "entity_count": [rcsb_entry_info.get("entity_count")],
        "experimental_method": [rcsb_entry_info.get("experimental_method")],
        "experimental_method_count": [rcsb_entry_info.get("experimental_method_count")],
        "inter_mol_covalent_bond_count": [rcsb_entry_info.get("inter_mol_covalent_bond_count")],
        "inter_mol_metalic_bond_count": [rcsb_entry_info.get("inter_mol_metalic_bond_count")],
        "molecular_weight": [rcsb_entry_info.get("molecular_weight")],
        "na_polymer_entity_types": [rcsb_entry_info.get("na_polymer_entity_types")],
        "nonpolymer_bound_components": [rcsb_entry_info.get("nonpolymer_bound_components")],
        "nonpolymer_entity_count": [rcsb_entry_info.get("nonpolymer_entity_count")],
        "nonpolymer_molecular_weight_maximum": [rcsb_entry_info.get("nonpolymer_molecular_weight_maximum")],
        "nonpolymer_molecular_weight_minimum": [rcsb_entry_info.get("nonpolymer_molecular_weight_minimum")],
        "polymer_composition": [rcsb_entry_info.get("polymer_composition")],
        "polymer_entity_count": [rcsb_entry_info.get("polymer_entity_count")],
        "polymer_entity_count_dna": [rcsb_entry_info.get("polymer_entity_count_dna")],
        "polymer_entity_count_rna": [rcsb_entry_info.get("polymer_entity_count_rna")],
        "polymer_entity_count_nucleic_acid": [rcsb_entry_info.get("polymer_entity_count_nucleic_acid")],
        "polymer_entity_count_nucleic_acid_hybrid": [rcsb_entry_info.get("polymer_entity_count_nucleic_acid_hybrid")],
        "polymer_entity_count_protein": [rcsb_entry_info.get("polymer_entity_count_protein")],
        "polymer_entity_taxonomy_count": [rcsb_entry_info.get("polymer_entity_taxonomy_count")],
        "polymer_molecular_weight_maximum": [rcsb_entry_info.get("polymer_molecular_weight_maximum")],
        "polymer_molecular_weight_minimum": [rcsb_entry_info.get("polymer_molecular_weight_minimum")],
        "polymer_monomer_count_maximum": [rcsb_entry_info.get("polymer_monomer_count_maximum")],
        "polymer_monomer_count_minimum": [rcsb_entry_info.get("polymer_monomer_count_minimum")],
        "resolution_combined": [rcsb_entry_info.get("resolution_combined")],
        "selected_polymer_entity_types": [rcsb_entry_info.get("selected_polymer_entity_types")],
        "software_programs_combined": [rcsb_entry_info.get("software_programs_combined")],
        "solvent_entity_count": [rcsb_entry_info.get("solvent_entity_count")],
        "structure_determination_methodology": [rcsb_entry_info.get("structure_determination_methodology")],
        "structure_determination_methodology_priority": [rcsb_entry_info.get("structure_determination_methodology_priority")],
        "diffrn_resolution_high_provenance_source": [rcsb_entry_info.get("diffrn_resolution_high_provenance_source")],
        "diffrn_resolution_high_value": [rcsb_entry_info.get("diffrn_resolution_high_value")]
    })

    print(f"Entry Data for {pdb_id}:")
    print(rcsb_entry_df)

    if not rcsb_entry_df.empty:
        entry_data = pd.concat([entry_data, rcsb_entry_df], ignore_index=True, sort=False)

    refine_df = pd.DataFrame({
        'pdb_id': [pdb_id],
        'deposition_date': [deposition_date],
        'release_date': [release_date],
        "ls_rfactor_rfree": [refine.get("ls_rfactor_rfree")],
        "ls_rfactor_rwork": [refine.get("ls_rfactor_rwork")],
        "ls_rfactor_obs": [refine.get("ls_rfactor_obs")],
        "ls_dres_high": [refine.get("ls_dres_high")],
        "ls_dres_low": [refine.get("ls_dres_low")],
        "ls_number_reflns_rfree": [refine.get("ls_number_reflns_rfree")],
        "ls_number_reflns_obs": [refine.get("ls_number_reflns_obs")],
        "ls_percent_reflns_rfree": [refine.get("ls_percent_reflns_rfree")],
        "ls_percent_reflns_obs": [refine.get("ls_percent_reflns_obs")],
        "pdbx_rfree_selection_details": [refine.get("pdbx_rfree_selection_details")],
        "pdbx_data_cutoff_high_rms_abs_f": [refine.get("pdbx_data_cutoff_high_rms_abs_f")],
        "pdbx_ls_cross_valid_method": [refine.get("pdbx_ls_cross_valid_method")],
        "pdbx_ls_sigma_f": [refine.get("pdbx_ls_sigma_f")],
        "pdbx_method_to_determine_struct": [refine.get("pdbx_method_to_determine_struct")],
        "pdbx_refine_id": [refine.get("pdbx_refine_id")],
        "solvent_model_details": [refine.get("solvent_model_details")],
        "solvent_model_param_bsol": [refine.get("solvent_model_param_bsol")],
        "solvent_model_param_ksol": [refine.get("solvent_model_param_ksol")],
        "biso_mean": [refine.get("biso_mean")]
    })

    print(f"Refine Data for {pdb_id}:")
    print(refine_df)

    if not refine_df.empty:
        refine_data = pd.concat([refine_data, refine_df], ignore_index=True, sort=False)

    if cursor_index % 5000 == 0 or cursor_index == total_files - 1:
        print("This should be working")
        citation_data.to_csv(citation_output, mode='a', index=False, header=not os.path.exists(citation_output))
        citation_data = pd.DataFrame(columns=citation_data.columns)
        entry_data.to_csv(entry_output, mode='a', index=False, header=not os.path.exists(entry_output))
        entry_data = pd.DataFrame(columns=entry_data.columns)
        refine_data.to_csv(refine_output, mode='a', index=False, header=not os.path.exists(refine_output))
        refine_data = pd.DataFrame(columns=refine_data.columns)

    cursor_index += 1

if failed_filenames:
    pd.DataFrame({'failed_filenames': failed_filenames}).to_csv(failed_output, index=False)
