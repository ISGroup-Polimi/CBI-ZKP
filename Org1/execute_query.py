import os
import json
import pandas as pd
import torch
import torch.nn as nn
import onnx
import numpy as np
from Org1.models.olap_cube import OLAPCube
from Org1.operations.slice_model import SliceModel
from Org1.operations.dicing_model import DicingModel
from Org1.operations.rollup_model import RollUpModel
from Org1.ezkl_workflow.generate_proof import generate_proof

output_dir = os.path.join('Org1', 'output')
os.makedirs(output_dir, exist_ok=True)

# Compose all OLAP operations into a single nn.Module
class ComposedOLAPModel(nn.Module):
    def __init__(self, operations):
        super().__init__()
        self.operations = nn.ModuleList(operations)
    def forward(self, x):
        for op in self.operations:
            x = op(x)
        return x

def decode_operations(operations, columns_to_remove_idx):
    decoded_ops = []
    if "Dicing" in operations:
        for dicing_args in operations["Dicing"]:
            decoded_ops.append(DicingModel(dicing_args))
    if "Rollup" in operations:
        decoded_ops.append(RollUpModel(columns_to_remove_idx))
    return decoded_ops

def apply_olap_operations(cube, tensor_data, operations):
    result_tensor = tensor_data
    for operation in operations:
        result_tensor = cube.execute_model(operation, result_tensor)
    print("OLAP operations applied successfully.")
    return result_tensor

# Execute the query and generate the proof
async def op_execute_query(operations, columns_to_remove_idx, timestamp):
    file_path = os.path.join('Org1', 'PR_DB', 'Sale_PR_C.csv')
    df = pd.read_csv(file_path)

    # Use TS column to filter and then drop it (it gives problem with the ezkl circuit calibration)
    df = df[df["TS"] <= timestamp] # select rows with TS <= timestamp
    df = df.drop(columns=["TS"])

    df.columns = df.columns.str.strip() # Remove leading and trailing whitespace from column names
    df = df.dropna() # Drop rows with NaN values

    """
    # Save the DataFrame to a CSV file in the 'test' folder before circuit processing
    test_dir = os.path.join('test')
    os.makedirs(test_dir, exist_ok=True)
    before_circuit_path = os.path.join(test_dir, 'BeforeCircuit.csv')
    df.to_csv(before_circuit_path, index=False)
    """
    
    #print(f"Initial DataFrame: \n {df}")

    # Initialize the OLAP cube and transform the data into a tensor
    cube = OLAPCube(df)
    # save the mappings (categorical values - indexes) to a JSON file in Shared folder
    #cube.save_category_mappings(os.path.join("Shared", "cat_map.json")) 
    tensor_data = cube.to_tensor()

    decoded_operations = decode_operations(operations, columns_to_remove_idx)

    # Apply the operations to the tensor data (Python-side)
    final_tensor = apply_olap_operations(cube, tensor_data, decoded_operations)

    """
    # Export the model in ONNX format
    final_operation = decoded_operations[-1]  
    final_operation.to(torch.device("cpu"))
    final_operation.eval()
    """

    # Compose all operations into a single model (ONNX-side)
    composed_model = ComposedOLAPModel(decoded_operations)
    composed_model.to(torch.device("cpu"))
    composed_model.eval()

    # Apply the composed model to the tensor data
    final_tensor_onnx = composed_model(tensor_data)

    # Compare Python-side and ONNX-side results
    if final_tensor.shape == final_tensor_onnx.shape:
        diff = (final_tensor - final_tensor_onnx).abs().max().item()
        print(f"Max absolute difference between Python and ONNX outputs: {diff}")
    else:
        print(f"Shape mismatch: Python {final_tensor.shape}, ONNX {final_tensor_onnx.shape}")

    # Export the composed model in ONNX format
    model_onnx_path = os.path.join(output_dir, 'model.onnx')
    torch.onnx.export(composed_model, tensor_data, model_onnx_path,
                      export_params=True, opset_version=11, do_constant_folding=True,
                      input_names=['input'], output_names=['output'])

    onnx_model = onnx.load(model_onnx_path)
    onnx.checker.check_model(onnx_model)

    data = dict(
        input_shapes=[tensor_data.shape],
        input_data=[(tensor_data).detach().numpy().reshape([-1]).tolist()],
        output_data=[final_tensor_onnx.detach().numpy().reshape([-1]).tolist()]
    )
    input_json_path = os.path.join(output_dir, 'input.json')
    with open(input_json_path, 'w') as f:
        json.dump(data, f)

    poseidon_hash = await generate_proof(output_dir, model_onnx_path, input_json_path, logrows=18)

    return final_tensor, poseidon_hash