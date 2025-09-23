import os
import json
import pandas as pd
import torch
import onnx
import numpy as np
from Org1.models.olap_cube import OLAPCube
from Org1.operations.slice_model import SliceModel
from Org1.operations.dicing_model import DicingModel
from Org1.operations.rollup_model import RollUpModel
from Org1.ezkl_workflow.generate_proof import generate_proof

output_dir = os.path.join('Org1', 'output')
os.makedirs(output_dir, exist_ok=True)

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
    file_path = os.path.join('Org1', 'PR_DB_C', 'Sale_PR_C.csv')

    df = pd.read_csv(file_path)
    # df = df[df["TS"] <= timestamp] # select rows with TS <= timestamp

    df.columns = df.columns.str.strip() # Remove leading and trailing whitespace from column names
    df = df.dropna() # Drop rows with NaN values

    print(df.isnull().sum())
    print(np.isinf(df.values).sum())

    print(f"Initial DataFrame: \n {df}")

    # Initialize the OLAP cube and transform the data into a tensor
    cube = OLAPCube(df)
    # save the mappings (categorical values - indexes) to a JSON file in Shared folder
    cube.save_category_mappings(os.path.join("Shared", "cat_map.json")) 
    tensor_data = cube.to_tensor()

    decoded_operations = decode_operations(operations, columns_to_remove_idx)

    # Apply the operations to the tensor data 
    final_tensor = apply_olap_operations(cube, tensor_data, decoded_operations)

    # Export the model in ONNX format
    final_operation = decoded_operations[-1]  
    final_operation.to(torch.device("cpu"))
    final_operation.eval()

    model_onnx_path = os.path.join(output_dir, 'model.onnx')
    torch.onnx.export(final_operation, tensor_data, model_onnx_path,
                      export_params=True, opset_version=11, do_constant_folding=True,
                      input_names=['input'], output_names=['output'])

    onnx_model = onnx.load(model_onnx_path)
    onnx.checker.check_model(onnx_model)

    data = dict(
        input_shapes=[tensor_data.shape],
        input_data=[(tensor_data).detach().numpy().reshape([-1]).tolist()],
        output_data=[final_tensor.detach().numpy().reshape([-1]).tolist()]
    )
    input_json_path = os.path.join(output_dir, 'input.json')
    with open(input_json_path, 'w') as f:
        json.dump(data, f)

    await generate_proof(output_dir, model_onnx_path, input_json_path, logrows=17)

    return final_tensor