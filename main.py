import os
import json
import pandas as pd
import torch
import onnx
from ezkl import ezkl
from web3 import Web3

import sys
import asyncio

from models.olap_cube import OLAPCube
from operations.slice_model import SliceModel
from operations.dicing_model import DicingModel
from operations.rollup_model import RollUpModel

from ezkl_workflow.generate_proof import generate_proof
from hash_utils import verify_dataset_hash, verify_query_allowed, publish_hash, calculate_file_hash
from data_generators.CSV_Generator1 import generate_CSV_1
from data_generators.CSV_Generator2 import generate_CSV_2


output_dir = './output'
os.makedirs(output_dir, exist_ok=True)

def load_contract_address(contract_name):
    # Load the contract address from the configuration file
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'contract_addresses.json')
    with open(config_path, 'r') as f:
        contract_addresses = json.load(f)
    return contract_addresses.get(contract_name)

# Load the DataFactModel contract address dynamically
data_fact_model_address = load_contract_address("DataFactModel")
if not data_fact_model_address:
    print("DataFactModel address not found in configuration.")
    sys.exit(1)

def update_metadata(file_path, update_date=None):

    metadata_path = os.path.join('data', 'metadata.json')
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

    # Load or create metadata.json
    if os.path.exists(metadata_path):
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
    else:
        metadata = {}

    file_name = os.path.basename(file_path)

    # Get the number of rows in the CSV file
    try:
        n_rows = pd.read_csv(file_path).shape[0]
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return

    # If the file is not in metadata (new)
    if file_name not in metadata:
        metadata[file_name] = {"Initial": [0, n_rows]}

    # If the file is in metadata (existing)
    else:
        # Update existing entry
        updates = metadata[file_name]

        # Find last interval's end
        last_key = list(updates.keys())[-1]
        last_end = updates[last_key][1]
        # 
        updates[update_date] = [last_end, n_rows]
        metadata[file_name] = updates

    # Save back to metadata.json
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=4)

    print(f"Metadata updated for file {file_name}.")


def op_generate_file():
    print("\nSelect a generator to use:")
    print("[1] Generator 1")
    print("[2] Generator 2")
    generator_choice = input("Enter your choice (1 or 2): ")

    if generator_choice == "1":
        output_file = "GHGe1.csv"
        generate_CSV_1(2000, 1, output_file) # CSV_Generator1.py
        print("File generated with Generator 1.")
    elif generator_choice == "2":
        output_file="GHGe2.csv"
        generate_CSV_2(2000, 1, output_file) # CSV_Generator2.py
        print("File generated with Generator 2.")
    else:
        print("Invalid choice. Returning to previous menu.")

    file_path = os.path.join('data', 'uploaded', output_file)
    
    update_metadata(file_path)

def CLI_update_file():
    # Make the user select a file to update 
    uploaded_files_dir = os.path.join('data', 'uploaded')
    files = os.listdir(uploaded_files_dir)
    if not files:
        print("No files available in the uploaded directory.")
        return
    print("Available files:")
    for idx, file in enumerate(files):
        print(f"[{idx + 1}] {file}")
    file_index = int(input("Select a file to publish by index: ")) - 1
    if file_index < 0 or file_index >= len(files):
        print("Invalid index selected.")
        return
    file_path = os.path.join(uploaded_files_dir, files[file_index])
    file_name = os.path.basename(file_path) # includes the extension

    # Ask the user for the string to append to the filename (date of the update)
    date = input("Please write the date of the update: ").strip()
    if not date:
        print("No date entered. Operation cancelled.")
        return
    # Ask the user how many new rows to add
    try:
        rows_to_add = int(input("How many new rows do you want to add? "))
    except ValueError:
        print("Invalid number entered. Operation cancelled.")
        return
    
    #base, ext = os.path.splitext(file_name)
    # new_filename = base + "_" + date + ext

    # Get the number of rows in the selected file
    num_rows = pd.read_csv(file_path).shape[0] + rows_to_add

    # Generate new rows using the appropriate generator based on file type
    if files[file_index].startswith("GHGe1"):
        generate_CSV_1(num_rows, 1, file_name)
    elif files[file_index].startswith("GHGe2"):
        generate_CSV_2(num_rows, 1, file_name)
    else:
        print("Unknown file type. Cannot generate new rows.")
        return

    # Update metadata for the new file
    update_metadata(file_path, date)

    """"
    # Split the filename and extension
    base, ext = os.path.splitext(files[file_index])
    new_filename = f"{base}_{append_str}{ext}"
    new_file_path = os.path.join(uploaded_files_dir, new_filename)

    # Copy the file for now
    with open(file_path, 'rb') as src, open(new_file_path, 'wb') as dst:
        dst.write(src.read())
    print(f"File copied to {new_file_path}")
    """
    

def CLI_publish_hash():
    # Make the user select a file to publish with CLI in data/uploaded directory
    uploaded_files_dir = os.path.join('data', 'uploaded')
    files = os.listdir(uploaded_files_dir)
    if not files:
        print("No files available in the uploaded directory.")
        return
    print("Available files:")
    for idx, file in enumerate(files):
        print(f"[{idx + 1}] {file}")
    file_index = int(input("Select a file to publish by index: ")) - 1
    if file_index < 0 or file_index >= len(files):
        print("Invalid index selected.")
        return
    file_path = os.path.join(uploaded_files_dir, files[file_index])

    hash = op_publish_hash(file_path) # MAIN.py

    # Save the hash in "published_hash.json" to share it with the customer
    published_hash_path = os.path.join('data', 'published_hash.json')
    os.makedirs(os.path.dirname(published_hash_path), exist_ok=True)  # Ensure the directory exists

    # Create the file if it doesn't exist
    if not os.path.exists(published_hash_path):
        with open(published_hash_path, 'w') as f:
            json.dump({}, f, indent=4)

    # Read the existing published hashes
    with open(published_hash_path, 'r') as f:
        published_hashes = json.load(f)
    # Add or update the hash for the selected file
    published_hashes[files[file_index]] = hash
    # Write the updated hashes back to the file
    with open(published_hash_path, 'w') as f:
        json.dump(published_hashes, f, indent=4)

    print(f"Hash for {files[file_index]} published successfully.")

def op_publish_hash(file_path):
    return publish_hash(file_path) # HASH_UTILS.py


# This function applies a sequence of OLAP operations to a data tensor using an OLAP cube object:
# - cube instance of OLAPCube from olap_cube.py -> tell how to apply OLAP operations to the data
# - tensor_data: the data tensor to be processed
# - operations: a list of OLAP operations to be applied
def apply_olap_operations(cube, tensor_data, operations):
    result_tensor = tensor_data
    for operation in operations:
        result_tensor = cube.execute_model(operation, result_tensor)
    return result_tensor


# This function gives the indices of the columns to roll-up based on the hierarchies names
def get_hier_indices_rollup(hierarchies_to_rollup):
    with open("DFM/DFM_GHGe1.json", "r") as f:
        DFM_representation = json.load(f)
    
    dimension_hierarchy = DFM_representation["dim_hierarchy"]
    dimension_index = DFM_representation["dim_index"]

    columns_to_remove = []
    for dim in hierarchies_to_rollup:
        columns_to_remove.extend(dimension_hierarchy[dim])

    indices_to_remove = [dimension_index[col] for col in columns_to_remove]
    #print(f"Indices to remove: {indices_to_remove}")
    return indices_to_remove

# This function gives the indices of the dimensions to be rolled up based on the dimensions name
# Example: if hierarchies_to_roll_up = [["Date", "Year"], ["Clothes Type", "Category"]], it will return the indices of "Month", "Day" and "Product Name"
def get_dim_indices_rollup(dimensions_to_rollup):
    with open("DFM/DFM_GHGe1.json", "r") as f:
        DFM_representation = json.load(f)
    
    dimension_hierarchy = DFM_representation["dim_hierarchy"]
    dimension_index = DFM_representation["dim_index"]

    dim_to_remove = []

    for dim in dimensions_to_rollup:
        hierarchy_name = dim[0] # "Date" or "Clothes Type"
        dim_of_hierarchy = dimension_hierarchy[hierarchy_name] # ["Year", "Month", "Day"] or ["Category", "Product Name"]
        # Get the index of dim[1] (dimension we want to do the rollup) in the hierarchy list
        if dim[1] in dim_of_hierarchy:
            idx = dim_of_hierarchy.index(dim[1])
        else:
            print(f"Dimension {dim[1]} not found in hierarchy {hierarchy_name}.")
        for i in range(idx + 1, len(dim_of_hierarchy)):
            dim_to_remove.append(dim_of_hierarchy[i]) 

    #print(f"Dimensions to remove for roll-up: {dim_to_remove}")

    # Convert the dimension names to their corresponding indices
    indices_to_remove = [dimension_index[dim] for dim in dim_to_remove]       

    #print(f"Indices to remove for roll-up dim: {indices_to_remove}")
    return indices_to_remove

# This function groups the rows after the roll-up operation (sum the emissions of rows with same dimensions)
def group_rows (df):
    with open("DFM/DFM_GHGe1.json", "r") as f:
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

# This function makes the user select a file to query with CLI
async def CLI_query():
    published_hash_path = os.path.join('data', 'published_hash.json')

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
    
    selected_file = list(published_hashes.keys())[file_index]
    file_path = os.path.join('data', 'uploaded', selected_file)

    await op_prepare_query(file_path) # MAIN.py

# This function:
# - verifies if the hash of the dataset do perform the query is the same as the one published on the blockchain
# - prepares the query by defining the OLAP operations to apply and checking if the query is allowed
async def op_prepare_query(file_path): 
    verify_dataset_hash(file_path) # HASH_UTILS.py

    # columns in the original file
    df = pd.read_csv(file_path)
    columns = df.columns.tolist()

    """
    # Define the list of OLAP operations to apply
    operations = [
        # SlicingModel and DicingModel are subclasses of OLAPOperation
        # {14: 1, 21: 12, 27: 0} is a dictionary where the keys are column indices and the values are the values to filter by
        SlicingModel({14: 1, 21: 12, 27: 0}),  # Slicing operation: select rows where column 14 is ==1, ...
        DicingModel({2: 2, 21: [3, 4], 27: 4})  # Dicing operation
    ]
    """

    # define the OLAP operations to apply
    hierarchies_to_rollup = get_hier_indices_rollup(["Clothes Type"]) # using dimensions hierarchy from "DFM/dimensions_hierarchy_GHGe1.json"
    columns_to_rollup = get_dim_indices_rollup([["Date", "Month"]]) # using dimensions hierarchy from "DFM/dimensions_hierarchy_GHGe1.json"

    columns_to_remove_idx = list(dict.fromkeys(hierarchies_to_rollup + columns_to_rollup)) # columns to remove from the tensor (no duplicates)
    
    # Get the column names corresponding to columns_to_remove_idx
    columns_to_remove = [columns[i] for i in columns_to_remove_idx]

    operations = [
        #SliceModel({2:0}), # filter column 2 with value ==0 ->  Material = "Canvas"
        DicingModel({2: [0, 3]}),
        RollUpModel(columns_to_remove_idx)
    ]

    # query_dimensions = ["Category", "Production Cost", "City", "Product Name"]
    query_dimensions = [col for col in columns if col not in columns_to_remove]

    is_query_allowed = verify_query_allowed(query_dimensions, data_fact_model_address) # HASH_UTILS.py

    if not is_query_allowed:
        print("Query contains disallowed dimensions.")
        return
    print("Query is allowed. Proceeding with query execution...")
    
    try:
        await op_perform_query(file_path, operations, columns_to_remove_idx) # MAIN.py
    except Exception as e:
        print(f"Failed to perform query: {e}")
        return    
    
async def op_perform_query(file_path, operations, columns_to_remove_idx):
    selected_file = os.path.basename(file_path)

    # dataframe is a bidimensional data structure with rows and columns
    df = pd.read_csv(file_path)

    # subset = df.iloc[0:2000] # select the first 2000 rows of the DataFrame

    print(f"Initial DataFrame: \n {df}")
    df.columns = df.columns.str.strip() # Remove leading and trailing whitespace from column names
    df = df.dropna() # Drop rows with NaN values

    #calculated_hash = calculate_file_hash(file_path) # hash_utils.py
    #bytes32_hash = Web3.to_bytes(hexstr=calculated_hash)

    # Initialize the OLAP cube and transform the data into a tensor
    #cube = OLAPCube(df, bytes32_hash)
    cube = OLAPCube(df)
    cube.save_category_mappings("cat_map.json") # save the mappings (categorical values - indexes) to a JSON file
    tensor_data = cube.to_tensor()

    #print(f"DataFrame after dropping NaN values: \n {df}") # categorical columns are already encoded as integers
    #print(f"OLAP cube: {cube}")

    # Apply the operations to the tensor data 
    final_tensor = apply_olap_operations(cube, tensor_data, operations)

    #print(f"Inital tensor:\n{tensor_data}")
    #print(f"Final tensor:\n{final_tensor}")

    # Export the model in ONNX format
    # Selects the last operation, which represents the final transformation of the tensor data
    final_operation = operations[-1]  
    # ONNX export requires the model and the input tensor to be on the same device (usually CPU for interoperability), we move the model to CPU
    # "device" specifies where (on which hardware) the model will be run, in this case on CPU
    final_operation.to(torch.device("cpu"))
    # Sets the model to evaluation mode (alternative to train mode) to disable dropout and batch normalization layers
    final_operation.eval()

    model_onnx_path = os.path.join(output_dir, 'model.onnx')
    # to export the model torch.nn.Module to ONNX format
    torch.onnx.export(final_operation,               # model to be exported, nn.Module subclass
                      tensor_data,                   # example input used to trace the computation graph of the model
                      model_onnx_path,               # where to save the model
                      export_params=True,            # store the trained parameter weights inside the model file
                      opset_version=11,              # the ONNX version to export the model to
                      do_constant_folding=True,      # enables optimization by evaluating constant expressions at export time
                      input_names=['input'],         # the model's input names
                      output_names=['output'])       # the model's output names

    # load the ONNX model from the file to the python object onnx_model
    onnx_model = onnx.load(model_onnx_path)
    # Run a validation check to ensure the model is well-formed and valid according to the ONNX specification
    onnx.checker.check_model(onnx_model)
    # print(onnx.helper.printable_graph(onnx_model.graph))

    # data_hash = calculate_file_hash(file_path) # HASH_UTILS.py

    ### Prepare the input (input shape, input data, output data) for the proof generation
    # Take PyTorch tensor - detach it from any computation graph - convert it to a NumPy array - flatten it to 1D 
    d = ((tensor_data).detach().numpy()).reshape([-1])
    # Create a dictionary "data"
    data = dict(
        input_shapes=[tensor_data.shape], # shape = how many elements along each axis
        input_data=[d.tolist()],
        output_data=[final_tensor.detach().numpy().reshape([-1]).tolist()],
        #data_hash = [cube.data_hash]
    )
    input_json_path = os.path.join(output_dir, 'input.json')
    with open(input_json_path, 'w') as f:
        json.dump(data, f) # serialize the data dictionary to a JSON file

    #await generate_proof(output_dir, model_onnx_path, input_json_path, logrows=17)
    proof_path, vk_path, settings_filename = await generate_proof(output_dir, model_onnx_path, input_json_path, logrows=18)

    # Print and save the final tensor after applying the OLAP operations in human-readable format
    # Remove rows from final_tensor that are all zeros
    non_zero_rows = ~torch.all(final_tensor == 0, dim=1)
    final_tensor = final_tensor[non_zero_rows] # after filtering

    # Save the final tensor as a CSV file after the query
    final_df = pd.DataFrame(final_tensor.detach().numpy())

    # Assign the original column names (from your input DataFrame) to the filtered DataFrame
    # filtered_columns = cube.df.columns[:final_df.shape[1]]
    
    # Remove columns of the slice
    all_indices = list(range(len(cube.df.columns)))
    kept_indices = [i for i in all_indices if i not in columns_to_remove_idx]
    kept_columns = [cube.df.columns[i] for i in kept_indices]

    # final_df.columns = [i for i in filtered_columns if i in kept_columns]
    final_df.columns = kept_columns 
    #print(f"Final DataFrame:\n{final_df}")

    # Sum the emissions for roll-up and slice
    final_df = group_rows(final_df)
    
    cat_map = OLAPCube.load_category_mappings("cat_map.json")
        #print("Category mappings loaded")
    filtered_cat_map = {col: mapping for col, mapping in cat_map.items() if col in final_df.columns}
    final_cube = OLAPCube(final_df, category_mappings=filtered_cat_map)
        #print("Final cube created")
    final_decoded_cube = final_cube.decode_categorical_columns()
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

    mod_selected_file = "mod_" + selected_file # mod = modified
    csv_output_path = os.path.join('data', 'modified', mod_selected_file)
    os.makedirs(os.path.dirname(csv_output_path), exist_ok=True)
    final_df.to_csv(csv_output_path, index=False)
    print(f"Query result saved to {csv_output_path}")

    CLI_verify_proof(file_path, proof_path, vk_path, settings_filename) # MAIN.py

def CLI_verify_proof(file_path, proof_path, vk_path, settings_filename):
    print(f"\nDo you want to verify the proof for the file '{os.path.basename(file_path)}'?")
    print("[1] Yes")
    print("[2] No")
    choice = input("Enter your choice (1 or 2): ")

    if choice == "1":
        try:
            res = op_verify_proof(proof_path, vk_path, settings_filename)
            if res:
                print("Proof verification completed.")
        except Exception as e:
            print(f"Proof verification failed: {e}")
    else:
        print("Proof verification skipped.")

def op_verify_proof(proof_path, vk_path, settings_filename):
    return ezkl.verify(proof_path, vk_path, settings_filename)


async def main():

    while True:
        print("\nSelect an option:")
        print("[1] Login as Organization 1 (data provider)")
        print("[2] Login as Organization 2 (data receiver)")
        print("[3] Exit")
        choice = input("Enter your choice (1, 2, or 3): ")

        if choice == "1":  # -------------------------------------- ORG 1
            print("You selected: Login as ORG 1 (data provider)")
            print("\nSelect an option:")
            print("[1] Upload File")
            print("[2] Publish Hash")
            print("[3] Update file")
            sub_choice = input("Enter your choice (1, 2, or 3): ")

            if sub_choice == "1":  # UPLOAD FILE
                op_generate_file()

            elif sub_choice == "2":  # PUBLISH HASH
                CLI_publish_hash()

            elif sub_choice == "3":  # UPDATE FILE
                CLI_update_file()

            else:
                print("Invalid choice. Returning to main menu.")

        elif choice == "2":  # ------------------------------------- ORG 2
            print("You selected: Login as ORG 2 (data receiver)")
            print("\nSelect an option:")
            print("[1] Perform Query")
            sub_choice = input("Enter your choice (1 or 2): ")

            if sub_choice == "1":  # PERFORM QUERY
                try:
                    await CLI_query()
                except Exception as e:
                    print(f"Failed to perform query: {e}")

            else:
                print("Invalid choice. Returning to main menu.")

        elif choice == "3":  # ------------------------------------- EXIT
            print("Exiting the program.")
            break

        else:
            print("Invalid choice. Please select a valid option.")

if __name__ == "__main__":
    asyncio.run(main())
