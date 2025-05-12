import os
import json
import pandas as pd

# Initialize an empty DataFrame
emdb_data = pd.DataFrame(columns=[])

# Set the folder path where the JSON files are located
json_folder = '/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/emdb_json'
emdb_output = "/home/zhn1744/AlphaFold/data/emdb/emdb_structures.csv"
failed_output = "/home/zhn1744/AlphaFold/data/failed/emdb_entries_failed.csv"
failed_filenames = []
cursor_index = 0

try:
    os.remove(emdb_output)
except:
    pass

file_paths = [os.path.join(json_folder, f) for f in os.listdir(json_folder)
              if f.startswith("response_emdb_EMD") and f.endswith(".json")]

#file_paths = [os.path.join(json_folder, "response_emdb_EMD-17212,EMD-17254,EMD-17255.json")]

total_files = len(file_paths)
print(f"Total files to process: {total_files}")

def get_buffer_components(buffer_data):
    if not buffer_data or 'component' not in buffer_data:
        return '', '', ''
    
    components = buffer_data['component']
    
    names = [comp.get('name', '') for comp in components]
    formulas = [comp.get('formula', '') for comp in components]
    concentrations = [f"{comp.get('concentration', {}).get('valueOf_', '')} {comp.get('concentration', {}).get('units', '')}" 
                     for comp in components]
    
    return (';'.join(filter(None, names)), 
            ';'.join(filter(None, formulas)), 
            ';'.join(filter(None, concentrations)))

def get_value_with_units(obj):
    if not obj:
        return ''
    value = obj.get('valueOf_', '')
    units = obj.get('units', '')
    if value:
        return f"{value} {units}".strip()
    return ''

def format_pretreatment(pretreat):
    if not pretreat:
        return ''
    parts = []
    if pretreat.get('type_'):
        parts.append(f"Type: {pretreat['type_']}")
    if pretreat.get('atmosphere'):
        parts.append(f"Atmosphere: {pretreat['atmosphere']}")
    if pretreat.get('time'):
        time = get_value_with_units(pretreat['time'])
        if time:
            parts.append(f"Time: {time}")
    if pretreat.get('pressure'):
        pressure = get_value_with_units(pretreat['pressure'])
        if pressure:
            parts.append(f"Pressure: {pressure}")
    return '; '.join(parts)

for index, file_path in enumerate(file_paths, start=1):
    print(f"Processing {index} of {total_files}")
    
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        # Get structure determination details
        struct_det = data.get('structure_determination_list', {}).get('structure_determination', [{}])[0]
        
        # Get specimen preparation details
        specimen_prep = struct_det.get('specimen_preparation_list', {}).get('specimen_preparation', [{}])[0]
        
        # Get microscopy details
        microscopy = struct_det.get('microscopy_list', {}).get('microscopy', [{}])[0]
        
        # Get image recording details
        image_recording = microscopy.get('image_recording_list', {}).get('image_recording', [{}])[0]
        
        # Get image processing details
        image_processing = struct_det.get('image_processing', [{}])[0]
        reconstruction = image_processing.get('final_reconstruction', {})

        # Get buffer data safely
        buffer = specimen_prep.get('buffer', {})
        
        # Get grid data safely
        grid = specimen_prep.get('grid', {})
        
        # Get vitrification data safely
        vitrification = specimen_prep.get('vitrification', {})

        emdb_ids = file_path.replace("/kellogg/proj/rrh3749/Working_Projects/AlphaFold/data/emdb_json/response_emdb_", "")
        emdb_ids = file_path.replace(".json", "")
        emdb_list = emdb_ids.split(",")

        for id in emdb_list:
            emdb_df = pd.DataFrame({
                'emdb_id': [id],
                'pdb_id': ['#'.join([
                    ref.get('pdb_id', '') 
                    for ref in data.get('crossreferences', {}).get('pdb_list', {}).get('pdb_reference', [])
                    if ref.get('pdb_id', '')
                ])],
                'title': [data.get('admin', {}).get('title', '')],
                'method': [struct_det.get('method', '')],
                'aggregation_state': [struct_det.get('aggregation_state', '')],
                
                # Specimen preparation
                'concentration': [get_value_with_units(specimen_prep.get('concentration'))],
                'specimen_details': [specimen_prep.get('details', '')],
                'buffer_ph': [buffer.get('ph', '')],
                'buffer_details': [buffer.get('details', '')],
                'buffer_component_names': [get_buffer_components(buffer)[0]],
                'buffer_component_formulas': [get_buffer_components(buffer)[1]],
                'buffer_component_concentrations': [get_buffer_components(buffer)[2]],
                
                # Grid
                'grid_mesh': [grid.get('mesh', '')],
                'grid_model': [grid.get('model', '')],
                'grid_material': [grid.get('material', '')],
                'grid_details': [grid.get('details', '')],
                'grid_pretreatment': [format_pretreatment(grid.get('pretreatment', {}))],
                
                # Vitrification
                'vitrification_cryogen': [vitrification.get('cryogen_name', '')],
                'chamber_humidity': [get_value_with_units(vitrification.get('chamber_humidity'))],
                'chamber_temperature': [get_value_with_units(vitrification.get('chamber_temperature'))],
                'vitrification_instrument': [vitrification.get('instrument', '')],
                
                # Microscopy
                'microscope': [microscopy.get('microscope', '')],
                'illumination_mode': [microscopy.get('illumination_mode', '')],
                'imaging_mode': [microscopy.get('imaging_mode', '')],
                'electron_source': [microscopy.get('electron_source', '')],
                'acceleration_voltage': [get_value_with_units(microscopy.get('acceleration_voltage'))],
                'c2_aperture_diameter': [get_value_with_units(microscopy.get('c2_aperture_diameter'))],
                'nominal_cs': [get_value_with_units(microscopy.get('nominal_cs'))],
                'nominal_defocus_min': [get_value_with_units(microscopy.get('nominal_defocus_min'))],
                'nominal_defocus_max': [get_value_with_units(microscopy.get('nominal_defocus_max'))],
                'calibrated_magnification': [microscopy.get('calibrated_magnification', '')],
                'specimen_holder_model': [microscopy.get('specimen_holder_model', '')],
                'cooling_holder_cryogen': [microscopy.get('cooling_holder_cryogen', '')],
                'alignment_procedure': [','.join(microscopy.get('alignment_procedure', {}).keys())],
                
                # Image recording
                'detector_model': [image_recording.get('film_or_detector_model', {}).get('valueOf_', '')],
                'number_real_images': [image_recording.get('number_real_images', '')],
                'average_electron_dose': [get_value_with_units(image_recording.get('average_electron_dose_per_image'))],
                
                # Image processing
                'resolution': [get_value_with_units(reconstruction.get('resolution'))],
                'resolution_method': [reconstruction.get('resolution_method', '')],
                'number_images_used': [reconstruction.get('number_images_used', '')],
                'applied_symmetry_point_group': [reconstruction.get('applied_symmetry', {}).get('point_group', '')],
                
                # Map details from main map data
                'map_format': [data.get('map', {}).get('format', '')],
                'map_data_type': [data.get('map', {}).get('data_type', '')],
                'map_dimensions_x': [data.get('map', {}).get('dimensions', {}).get('col', '')],
                'map_dimensions_y': [data.get('map', {}).get('dimensions', {}).get('row', '')],
                'map_dimensions_z': [data.get('map', {}).get('dimensions', {}).get('sec', '')],
                'map_pixel_spacing_x': [get_value_with_units(data.get('map', {}).get('pixel_spacing', {}).get('x'))],
                'map_pixel_spacing_y': [get_value_with_units(data.get('map', {}).get('pixel_spacing', {}).get('y'))],
                'map_pixel_spacing_z': [get_value_with_units(data.get('map', {}).get('pixel_spacing', {}).get('z'))]
            })

            emdb_data = pd.concat([emdb_data, emdb_df], ignore_index=True, sort=False)

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        failed_filenames.append(os.path.basename(file_path))
        continue

    if cursor_index % 100 == 0 or cursor_index == total_files - 1:
        print("Writing batch to CSV...")
        emdb_data.to_csv(emdb_output, mode='a', index=False, header=not os.path.exists(emdb_output))
        emdb_data = pd.DataFrame(columns=emdb_data.columns)
    
    cursor_index += 1

if failed_filenames:
    pd.DataFrame({'failed_filenames': failed_filenames}).to_csv(failed_output, index=False)

print("Processing complete!")