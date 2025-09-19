import asyncio
import json
import os
import sys
from ezkl import ezkl
import torch
import pandas as pd

from Org2.hash_utils import verify_query_allowed
from Org1.main import op_perform_query

def load_contract_address(contract_name):
    # Load the contract address from the configuration file
    project_root = os.path.dirname(os.path.dirname(__file__))
    config_path = os.path.join(project_root, 'Blockchain', 'contract_addresses.json')
    with open(config_path, 'r') as f:
        contract_addresses = json.load(f)
    return contract_addresses.get(contract_name)

# Load the DataFactModel contract address dynamically
data_fact_model_address = load_contract_address("DataFactModel")
if not data_fact_model_address:
    print("DataFactModel address not found in configuration.")
    sys.exit(1)

# This function makes the user select a file to query with CLI
async def CLI_query():
    published_hash_path = os.path.join('Shared', 'published_hash.json')

    if not os.path.exists(published_hash_path):
        print("\nNo published hashes file found.")
        return
    
    with open(published_hash_path, 'r') as f:
        published_hashes = json.load(f)
    if not published_hashes:
        print("\nNo hashes available in the published file.")
        return
    
    print("\nAvailable published hashes:")
    for idx, (file_name, hash_value) in enumerate(published_hashes.items()):
        print(f"[{idx + 1}] File: {file_name} - Hash: {hash_value}")
    file_index = int(input("Select a file to query by index: ")) - 1
    if file_index < 0 or file_index >= len(published_hashes):
        print("Invalid index selected.")
        return
    
    print("\n")
    
    selected_file = list(published_hashes.keys())[file_index]

    await op_prepare_query(selected_file) # MAIN2.py

# This function:
# - verifies if the hash of the dataset do perform the query is the same as the one published on the blockchain
# - prepares the query by defining the OLAP operations to apply and checking if the query is allowed
async def op_prepare_query(selected_file): 
    # TO BE DONE
    #                   verify_dataset_hash(file_path) # HASH_UTILS.py

    # Define the OLAP operations to apply
    operations = {
        "Rollup": [["Clothes Type"], # rollup hierarchy
                   ["Date", "Month"]], # rollup dimension
        "Dicing": [{2: [0, 3]}]
    }

    query_dimensions, columns_to_remove_idx = get_query_dimensions(operations) # MAIN.py

    is_query_allowed = verify_query_allowed(query_dimensions, data_fact_model_address) # HASH_UTILS.py

    if not is_query_allowed:
        print("Query contains disallowed dimensions.")
        return
    print("Query is allowed. Proceeding with query execution...\n")

    try:
        final_tensor, columns_to_remove_idx = await op_perform_query(selected_file, operations, columns_to_remove_idx) # MAIN ORG1.py
        show_result(selected_file, final_tensor, columns_to_remove_idx)
    except Exception as e:
        print(f"Failed to perform query: {e}")
        return    

def get_query_dimensions(operations):
    columns_to_rollup_idx = []

    for i in operations["Rollup"]:
        columns_to_rollup_idx.extend(get_idx_rollup(i[0], i[1] if len(i) > 1 else None)) # get the indices of the dimensions to roll up

    # Remove duplicates and sort the indices
    columns_to_rollup_idx = sorted(set(columns_to_rollup_idx))

    with open("Shared/DFM_GHGe1.json", "r") as f:
        DFM_representation = json.load(f)
    columns = list(DFM_representation["dim_index"].keys())
    
    # Get the column names corresponding to columns_to_remove_idx
    columns_to_rollup = [columns[i] for i in columns_to_rollup_idx]


    # query_dimensions = ["Category", "Production Cost", "City", "Product Name"]
    query_dimensions = [col for col in columns if col not in columns_to_rollup]

    return query_dimensions, columns_to_rollup_idx

# This function gives the indices of the dimensions to be rolled up
# Example: if hierarchy_to_roll_up = "Date" and dimensions_to_rollup = "Year" -> it will return the indices of "Month", "Day" and "Product Name"
#          if hierarchy_to_rollup = "Clothes Type" and dimensions_to_rollup = None -> it will return the indices of all dimensions in the hierarchy
def get_idx_rollup(hierarchy_to_rollup, dimensions_to_rollup=None):
    with open("Shared/DFM_GHGe1.json", "r") as f:
        DFM_representation = json.load(f)

    dim_hierarchy = DFM_representation["dim_hierarchy"]
    dim_index = DFM_representation["dim_index"]

    dim_to_remove = []

    if dimensions_to_rollup is None: # Roll-up all dimensions of a hierarchy
        dim_to_remove.extend(dim_hierarchy[hierarchy_to_rollup])

    else:
        dim_of_hierarchy = dim_hierarchy[hierarchy_to_rollup] # ["Year", "Month", "Day"] or ["Category", "Product Name"]
        if dimensions_to_rollup in dim_of_hierarchy:
            idx = dim_of_hierarchy.index(dimensions_to_rollup)
        else:
            print(f"Dimension {dimensions_to_rollup} not found in hierarchy {hierarchy_to_rollup}.")
        
        for i in range(idx + 1, len(dim_of_hierarchy)):
            dim_to_remove.append(dim_of_hierarchy[i])

    # Convert the dimension names to their corresponding indices
    indices_to_remove = [dim_index[dim] for dim in dim_to_remove]       

    #print(f"Indices to remove for roll-up dim: {indices_to_remove}")
    return indices_to_remove

def op_verify_proof():
    proof_folder = 'Shared/proof'
    if not os.path.exists(proof_folder):
        print("Proof folder does not exist.")
        return
    
    proof_path = os.path.join(proof_folder, 'test.pf')
    vk_path = os.path.join(proof_folder, 'test.vk')
    settings_filename = os.path.join(proof_folder, 'settings.json')

    if not os.path.exists(proof_path):
        print("Proof file not found.")
        return

    if not os.path.exists(vk_path):
        print("Verification key file not found.")
        return
    
    if not os.path.exists(settings_filename):
        print("Settings file not found.")
        return
    
    print("\nStarting proof verification...")

    try:
        res = ezkl.verify(proof_path, settings_filename, vk_path)
        if res:
            print("EZKL Proof Verification successful")
    except Exception as e:
        print(f"Proof verification failed: {e}")

def show_result(selected_file, final_tensor, columns_to_remove_idx):
    # Print and save the final tensor after applying the OLAP operations in human-readable format
    # Remove rows from final_tensor that are all zeros
    non_zero_rows = ~torch.all(final_tensor == 0, dim=1)
    final_tensor = final_tensor[non_zero_rows] # after filtering

    # Save the final tensor as a CSV file after the query
    final_df = pd.DataFrame(final_tensor.detach().numpy())

    # Assign the original column names (from your input DataFrame) to the filtered DataFrame
    # filtered_columns = cube.df.columns[:final_df.shape[1]]
    
    # Remove columns of the slice
    with open("Shared/DFM_Sale.json", "r") as f:
        DFM_representation = json.load(f)
    dim_index = DFM_representation["dim_index"]
    kept_columns = [col for col, idx in dim_index.items() if idx not in columns_to_remove_idx]

    # final_df.columns = [i for i in filtered_columns if i in kept_columns]
    final_df.columns = kept_columns 
    #print(f"Final DataFrame:\n{final_df}")

    # Sum the emissions for roll-up and slice
    final_df = group_rows(final_df)

    with open("Shared/cat_map.json", "r") as f:
        cat_map =  json.load(f)
    
        #print("Category mappings loaded")
    filtered_cat_map = {col: mapping for col, mapping in cat_map.items() if col in final_df.columns}
    #final_cube = OLAPCube(final_df, category_mappings=filtered_cat_map)
        #print("Final cube created")
    final_decoded_cube = decode_categorical_columns(final_df, filtered_cat_map)
    # "Year", "Month", "Day" convert to int
    for col in ["Year", "Month", "Day"]:
        if col in final_decoded_cube.columns:
            final_decoded_cube[col] = final_decoded_cube[col].astype(int)
    # "Total Emissions (kgCO2e)" round to 1 decimal place
    # change CSV output format to 1 decimal place
    if "Total Emissions (kgCO2e)" in final_decoded_cube.columns:
        final_decoded_cube["Total Emissions (kgCO2e)"] = final_decoded_cube["Total Emissions (kgCO2e)"].round(1)
    # print the final decoded cube with 1 decimal place for floats
    pd.set_option('display.float_format', '{:.1f}'.format)

    print(f"Final Decoded Cube:\n{final_decoded_cube}")

    print("Query executed successfully.")

    # Save the final DataFrame as a CSV in Org2/Public
    output_dir = os.path.join('Org2', 'PublicDB')
    os.makedirs(output_dir, exist_ok=True)

    # Change the file name from "Private_Converted.csv" to "Public.csv" if present
    if selected_file.endswith("Private_Converted.csv"):
        selected_file = selected_file.replace("Private_Converted.csv", "Public.csv")

    output_path = os.path.join(output_dir, selected_file)
    final_decoded_cube.to_csv(output_path, index=False)
    print(f"Query result saved to {output_path}")
    


    """
    mod_selected_file = "mod_" + selected_file # mod = modified
    csv_output_path = os.path.join('Org', 'modified', mod_selected_file)
    os.makedirs(os.path.dirname(csv_output_path), exist_ok=True)
    final_df.to_csv(csv_output_path, index=False)
    print(f"Query result saved to {csv_output_path}")
    """

# This function groups the rows after the roll-up operation (sum the emissions of rows with same dimensions)
def group_rows (df):
    with open("Shared/DFM_GHGe1.json", "r") as f:
        DFM_representation = json.load(f)
    
    # attritubes to sum ("Total Emissions (kgCO2e)")
    attributes = DFM_representation["attributes"] 

    # Group by all columns except those in attributes
    group_cols = [col for col in df.columns if col not in attributes]
    sum_cols = [col for col in attributes if col in df.columns]

    #print(f"Columns to group by: {group_cols}")
    #print(f"Columns to sum: {sum_cols}")

    if group_cols and sum_cols:
        grouped_df = df.groupby(group_cols, as_index=False)[sum_cols].sum()
        return grouped_df
    else:
        print("No columns to group by or sum.")
        return df
    
def decode_categorical_columns(final_df, filtered_cat_map):
    decoded_df = final_df
    for col, mapping in filtered_cat_map.items():
        inv_mapping = {v: k for k, v in mapping.items()}
        decoded_df[col] = decoded_df[col].map(inv_mapping)
    return decoded_df
    
async def main():

    while True:
        print("\n\nORG 2 (data receiver) select an option:")
        print("[1] Perform Query")
        print("[2] Verify Proof")
        print("[0] Exit")
        sub_choice = input("Enter your choice (1, 2, or 0): ")

        if sub_choice == "1":  # PERFORM QUERY
            try:
                await CLI_query()
            except Exception as e:
                print(f"Failed to perform query: {e}")

        elif sub_choice == "2":  # VERIFY PROOF
            try:
                op_verify_proof()
            except Exception as e:
                print(f"Failed to verify proof: {e}")

        elif sub_choice == "0":
            print("Exiting ORG 2 menu.")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    asyncio.run(main())