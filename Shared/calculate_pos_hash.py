import json
import ezkl
import pandas as pd
from Org1.models.olap_cube import OLAPCube
import os

async def calculate_pos_hash (model_onnx_path, file_path, logrows):
    output_dir = 'Org1/output'

    settings_filename = os.path.join(output_dir, 'settings.json')
    os.makedirs(os.path.dirname(settings_filename), exist_ok=True)
    compiled_filename = os.path.join(output_dir, 'circuit.compiled')

    run_args = ezkl.PyRunArgs()
    run_args.input_visibility = "hashed/public"

    res = ezkl.gen_settings(model_onnx_path, settings_filename, py_run_args=run_args)
    assert res == True # file successfully generated

    try:
        print(f"Attempting to get SRS with logrows={logrows}")
        res = await ezkl.get_srs(settings_filename, logrows=logrows)
        assert res == True
        print(f"EZKL Get SRS: {res}")
    except Exception as e:
        print(f"Error during SRS generation: {e}")
        raise


    # dataframe is a bidimensional data structure with rows and columns
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip() # Remove leading and trailing whitespace from column names
    df = df.dropna() # Drop rows with NaN values
    cube = OLAPCube(df)
    tensor_data = cube.to_tensor()

    # Prepare the input (input shape, input data, output data) for the proof generation in a dictionary format
    data = dict(
        input_shapes=[tensor_data.shape], # shape = how many elements along each axis
        # Take PyTorch tensor - detach it from any computation graph - convert it to a NumPy array - flatten it to 1D - list
        input_data=[(tensor_data).detach().numpy().reshape([-1]).tolist()],
    )
    input_json_path = os.path.join(output_dir, 'input.json')
    with open(input_json_path, 'w') as f:
        json.dump(data, f) # serialize the data dictionary to a JSON file

    res = await ezkl.calibrate_settings(input_json_path, model_onnx_path, settings_filename, "resources")
    assert res == True
    print(f"EZKL Calibrate settings: {res}")


    res = ezkl.compile_circuit(model_onnx_path, compiled_filename, settings_filename)
    assert res == True
    print(f"EZKL Compile circuit: {res}")

    assert os.path.exists(settings_filename)
    assert os.path.exists(input_json_path)
    assert os.path.exists(model_onnx_path)

    """
    # Setup della prova con ezkl
    pk_path = os.path.join(output_dir, 'test.pk') # Proving Key (to generate the proof)
    vk_path = os.path.join('Shared', 'proof', 'test.vk') # Verifying Key (to verify the proof)
    # ezkl.setup() -> This function generates the proving and verifying keys needed for the zero-knowledge proof
    res = ezkl.setup(compiled_filename, vk_path, pk_path)
    assert res == True
    print(f"EZKL Setup: {res}")
    """

    witness_path = os.path.join(output_dir, "witness.json")
    try:
        # Witness generation needed to create the proof
        # Witness is a JSON file that contains the input data and intermediate values computed by the circuit
        res = await ezkl.gen_witness(input_json_path, compiled_filename, witness_path)
        if res:
            print("EZKL Witness Generation successful")
    except Exception as e:
        print(f"An error occurred: {e}")

    """
    proof_path = os.path.join(output_dir, 'test.pf')
    try:
        # Proof generation
        # "single" indicates that we are generating a single proof for the circuit
        res = ezkl.prove(witness_path, compiled_filename, pk_path, proof_path, "single")
        if res:
            print("EZKL Proof Generation successful")
    except Exception as e:
        print(f"An error occurred: {e}")
    """

    witness_path = os.path.join(output_dir, "witness.json")
    with open(witness_path, "r") as f:
        witness = json.load(f)
    poseidon_hash = witness["processed_inputs"]["poseidon_hash"]
    print("Poseidon hash calculate_pos_hash:", poseidon_hash)

    return poseidon_hash
    