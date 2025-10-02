import os
import json
import logging
import ezkl
from web3 import Web3
import pandas as pd
import numpy as np

from Org1.models.olap_cube import OLAPCube
#from pymerkle import MerkleTree

logging.basicConfig(level=logging.INFO)

# Load contract addresses from configuration file
CONFIG_PATH = os.path.join('Blockchain', 'contract_addresses.json')
with open(CONFIG_PATH, 'r') as f:
    contract_addresses = json.load(f)

CONTRACT_ADDRESS = contract_addresses.get("HashStorage")

# ABI (Application Binary Interface) is a JSON description of the contract's functions and events
# It allows us to interact with the contract using web3.py
CONTRACT_ABI_SET_HASH = json.loads('''
[
    {
        "constant": false,
        "inputs": [
            {
                "name": "timestamp",
                "type": "uint256"
            },
            {
                "name": "_hash",
                "type": "bytes32"
            }
        ],
        "name": "setHash",
        "outputs": [],
        "payable": false,
        "stateMutability": "nonpayable",
        "type": "function"
    }
]
''')

def setup_web3():
    # Create web3 instance that tries to connect to Ethereum node running locally on the machine
    web3 = Web3(Web3.HTTPProvider("http://127.0.0.1:8545"))
    if not web3.is_connected():
        logging.error("Failed to connect to the blockchain.")
        raise ConnectionError("Failed to connect to the blockchain.")
    return web3

# It retrieves the contract instance using the address and ABI
# If the contract is not deployed at the given address, it will be deployed
def get_contract(web3, address, abi):
    return web3.eth.contract(address=address, abi=abi)

# Compute the Poseidon hash of the dataset considering only the rows before the given timestamp
def c_pos_hash(timestamp):
    file_path = os.path.join('Org1', 'PR_DB', "Sale_PR_C.csv")
    df = pd.read_csv(file_path)

    # Use TS column to filter and then drop it (it gives problem with the ezkl circuit calibration)
    df = df[df["TS"] <= timestamp] # select rows with TS <= timestamp
    df = df.drop(columns=["TS"])

    df.columns = df.columns.str.strip() # Remove leading and trailing whitespace from column names
    df = df.dropna() # Drop rows with NaN values

    """
    # Save the DataFrame to a CSV file in the 'test' folder 
    test_dir = os.path.join('test')
    os.makedirs(test_dir, exist_ok=True)
    before_circuit_path = os.path.join(test_dir, 'HashCalculation.csv')
    df.to_csv(before_circuit_path, index=False)
    """

    cube = OLAPCube(df)
    tensor_data = cube.to_tensor()

    # input_floats = [1.23, 4.56]

    # same scale as in ezkl settings.json
    scale = 13           # 2 ** 6 input_scale max
    
    # Flatten the tensor and clean invalid values
    flat_tensor = tensor_data.detach().numpy().reshape(-1)
    flat_tensor = np.nan_to_num(flat_tensor, nan=0.0, posinf=0.0, neginf=0.0)

    # Convert floats to field elements (as strings)
    field_elements = []
    for x in flat_tensor:
        try:
            field_elements.append(ezkl.float_to_felt(float(x), scale))
        except Exception as e:
            print(f"Failed to quantize value: {x} ({e})")
            raise

    # ezkl expects only 2 elements, pad if needed:
    while len(field_elements) < 2:
        field_elements.append(ezkl.float_to_felt(0.0, scale))

    poseidon_hash = ezkl.poseidon_hash(field_elements)
    print("ezkl Poseidon hash:", poseidon_hash)
    return poseidon_hash
    
# Publish the hash on the blockchain
async def publish_hash(timestamp):
    # Calculate the Poseidon hash of the dataset considering the rows before the timestamp
    poseidon_hash = c_pos_hash(timestamp)

    # Fix: extract string if it's a list
    if isinstance(poseidon_hash, list):
        poseidon_hash = poseidon_hash[0]
    if not poseidon_hash.startswith("0x"):
        poseidon_hash = "0x" + poseidon_hash

    bytes32_hash = Web3.to_bytes(hexstr=poseidon_hash)

    print("Poseidon hash PUBLISHED:", poseidon_hash)
    print("Poseidon hash PUBLISHED bytes32:", bytes32_hash)


    web3 = setup_web3()
    contract_set = get_contract(web3, CONTRACT_ADDRESS, CONTRACT_ABI_SET_HASH) # hash_utils.py
    account = web3.eth.accounts[0]

    try:
        # setHash() from HashStorage.sol Solidity contract
        tx_hash = contract_set.functions.setHash(timestamp, bytes32_hash).transact({'from': account})
        web3.eth.wait_for_transaction_receipt(tx_hash)
        logging.info(f"Hash {poseidon_hash} has been published to the blockchain.")

        return poseidon_hash
    
    except Exception as e:
        logging.error(f"Failed to publish hash: {e}")
        raise