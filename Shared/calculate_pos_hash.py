"""
import ezkl

def calculate_pos_hash(output_dir, model_onnx_path, input_json_path, logrows):



    try:
        # Witness generation needed to create the proof
        # Witness is a JSON file that contains the input data and intermediate values computed by the circuit
        res = await ezkl.gen_witness(input_json_path, compiled_filename, witness_path)
    if res:
        print("EZKL Witness Generation successful")
    except Exception as e:
        print(f"An error occurred: {e}")
"""
    