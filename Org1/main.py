import asyncio
import os
import json
import sys
import pandas as pd
import torch
import onnx

import poseidon
import numpy as np

from Org1.data_generators import CSV_Generator1
from Org1.data_generators import CSV_Generator2
from Org1.hash_utils import publish_hash
from Org1.operations.slice_model import SliceModel
from Org1.operations.dicing_model import DicingModel
from Org1.operations.rollup_model import RollUpModel
from Org1.models.olap_cube import OLAPCube
from Org1.ezkl_workflow.generate_proof import generate_proof
from Org1.data_generators.StarSchemeGenerator import main as star_scheme_main
from Org1.Dim_ID_Converter import CSV_converter



output_dir = os.path.join('Org1', 'output')
os.makedirs(output_dir, exist_ok=True)

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

""""
def op_generate_file():

    print("\nSelect a generator to use:")
    print("[1] Generator 1")
    print("[2] Generator 2")
    generator_choice = input("Enter your choice (1 or 2): ")

    if generator_choice == "1":
        output_file = "GHGe1.csv"
        CSV_Generator1.generate_CSV_1(2000, 1, output_file) # CSV_Generator1.py
        print("File generated with Generator 1.")
    elif generator_choice == "2":
        output_file="GHGe2.csv"
        CSV_Generator2.generate_CSV_2(2000, 1, output_file) # CSV_Generator2.py
        print("File generated with Generator 2.")
    else:
        print("Invalid choice. Returning to previous menu.")

    #file_path = os.path.join("Org1", 'data', 'uploaded', output_file)
    
    #update_metadata(file_path)
"""

async def CLI_publish_hash():
    # Try to open "Sale_Private.csv" from Org1/data; if it doesn't exist, show an error and return
    sale_file_path = os.path.join('Org1', 'PrivateDB', 'Sale_Private.csv')
    if os.path.exists(sale_file_path):
        file_path = sale_file_path
        files = ['Sale_Private.csv']
        file_index = 0
        print('\nAutomatically opened "Sale_Private.csv" from the Org1/PrivateDB folder.')
    else:
        print('\nError: "Sale_Private.csv" not found in the Org1/PrivateDB folder.')
        return

    print("\n")
    hash = await op_publish_hash(file_path) # MAIN.py

    # Save the hash in "published_hash.json" to share it with the customer
    published_hash_path = os.path.join('Shared', 'published_hash.json')
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

async def op_publish_hash(file_path):
    return await publish_hash(file_path) # HASH_UTILS.py

def decode_operations(operations, columns_to_remove_idx):
    decoded_ops = []
    if "Dicing" in operations:
        for dicing_args in operations["Dicing"]:
            decoded_ops.append(DicingModel(dicing_args))
    if "Rollup" in operations:
        decoded_ops.append(RollUpModel(columns_to_remove_idx))
    return decoded_ops

# This function applies a sequence of OLAP operations to a data tensor using an OLAP cube object:
# - cube instance of OLAPCube from olap_cube.py -> tell how to apply OLAP operations to the data
# - tensor_data: the data tensor to be processed
# - operations: a list of OLAP operations to be applied
def apply_olap_operations(cube, tensor_data, operations):
    result_tensor = tensor_data
    for operation in operations:
        result_tensor = cube.execute_model(operation, result_tensor)
    print("OLAP operations applied successfully.")
    return result_tensor


# operation example:    operations = {
#        "Rollup": [["Clothes Type"], ["Date", "Month"]], 
#        "Dicing": [{2: [0, 3]}]
#    }

async def op_perform_query(selected_file_name, operations, columns_to_remove_idx):
    selected_file_name = selected_file_name.replace(".csv", "_Converted.csv")
    print(selected_file_name)

    file_path = os.path.join('Org1', 'PrivateDB_Converted', selected_file_name)

    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip() # Remove leading and trailing whitespace from column names
    df = df.dropna() # Drop rows with NaN values

    print(f"Initial DataFrame: \n {df}")

    # Initialize the OLAP cube and transform the data into a tensor
    cube = OLAPCube(df)
    # save the mappings (categorical values - indexes) to a JSON file in Shared folder
    cube.save_category_mappings(os.path.join("Shared", "cat_map.json")) 
    tensor_data = cube.to_tensor()

    #print(f"DataFrame after dropping NaN values: \n {df}") # categorical columns are already encoded as integers
    #print(f"OLAP cube: {cube}")

    decoded_operations = decode_operations(operations, columns_to_remove_idx)

    # Apply the operations to the tensor data 
    final_tensor = apply_olap_operations(cube, tensor_data, decoded_operations)

    #print(f"Inital tensor:\n{tensor_data}")
    #print(f"Final tensor:\n{final_tensor}")

    # Export the model in ONNX format
    # Selects the last operation, which represents the final transformation of the tensor data
    final_operation = decoded_operations[-1]  
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


    # Prepare the input (input shape, input data, output data) for the proof generation in a dictionary format
    data = dict(
        input_shapes=[tensor_data.shape], # shape = how many elements along each axis
        # Take PyTorch tensor - detach it from any computation graph - convert it to a NumPy array - flatten it to 1D - list
        input_data=[(tensor_data).detach().numpy().reshape([-1]).tolist()],
        output_data=[final_tensor.detach().numpy().reshape([-1]).tolist()]
    )
    input_json_path = os.path.join(output_dir, 'input.json')
    with open(input_json_path, 'w') as f:
        json.dump(data, f) # serialize the data dictionary to a JSON file

    #await generate_proof(output_dir, model_onnx_path, input_json_path, logrows=17)
    await generate_proof(output_dir, model_onnx_path, input_json_path, logrows=17)

    """
    # Load input as ezkl would see it
    with open(input_json_path, "r") as f:
        data = json.load(f)
    flat_input = np.array(data["input_data"]).flatten()

    # Apply ezkl scaling
    input_scale = 7  # set this to match your ezkl settings.json
    scale = 2 ** input_scale
    field_inputs = [int(round(x * scale)) for x in flat_input]

    # Use only the first two elements (as ezkl does for t=3)
    field_inputs = field_inputs[:2]

    # Poseidon parameters
    prime = poseidon.parameters.prime_254
    security_level = 128
    alpha = 5
    input_rate = 2
    t = 3

    poseidon_hasher = poseidon.Poseidon(prime, security_level, alpha, input_rate, t)
    poseidon_digest = poseidon_hasher.run_hash(field_inputs)
    print("Poseidon hash (ezkl params):", hex(int(poseidon_digest)))
    """

    return final_tensor

def convert_CSV():
    db_folder = os.path.join("Org1", "PrivateDB")
    files = [f for f in os.listdir(db_folder) if os.path.isfile(os.path.join(db_folder, f))]

    if not files:
        print("No files found in Org1/PrivateDB folder.")
        return
    
    print("\nSelect a file from Org1/PrivateDB to convert:")
    for idx, fname in enumerate(files):
        print(f"[{idx+1}] {fname}")
    choice = input("Enter the number of the file to convert: ")
    try:
        idx = int(choice) - 1
        if idx < 0 or idx >= len(files):
            print("Invalid selection.")
            return
        selected_file = files[idx]
    except ValueError:
        print("Invalid input.")
        return
    
    input_path = os.path.join(db_folder, selected_file)
    
    CSV_converter(input_path)


async def main():

    while True:
        print("\n\nORG 1 (data provider) select an option:")
        print("[1] Upload (Generate) File")
        print("[2] Convert File")
        print("[3] Publish Hash")
        print("[0] Exit")
        sub_choice = input("Enter your choice (1, 2, 3, or 0): ")

        if sub_choice == "1":  # UPLOAD FILE
            star_scheme_main()
            print("File generated using StarSchemeGenerator and saved as 'Sale_Private.csv' in Org1/PrivateDB folder.")

        elif sub_choice == "2":  # CONVERT FILE
            convert_CSV()

        elif sub_choice == "3":  # PUBLISH HASH
            await CLI_publish_hash()

        elif sub_choice == "0":
            print("Exiting.")
            break

        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    asyncio.run(main())