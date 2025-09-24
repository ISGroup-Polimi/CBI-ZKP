import os
import json
import hashlib
import logging
import asyncio
import ezkl
from web3 import Web3
import pandas as pd
import numpy as np
import web3
from Org1.models.olap_cube import OLAPCube
#from pymerkle import MerkleTree
from Shared.calculate_pos_hash import calculate_pos_hash  

logging.basicConfig(level=logging.INFO)

# Load contract addresses from configuration file
CONFIG_PATH = os.path.join('Blockchain', 'contract_addresses.json')
with open(CONFIG_PATH, 'r') as f:
    contract_addresses = json.load(f)

CONTRACT_ADDRESS = contract_addresses.get("HashStorage")
DATA_FACT_MODEL_ADDRESS = contract_addresses.get("DataFactModel")

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
CONTRACT_ABI_GET_HASH = json.loads('''
[
    {
        "constant": true,
        "inputs": [
            {
                "name": "timestamp",
                "type": "uint256"
            }
        ],
        "name": "getHash",
        "outputs": [
            {
                "name": "",
                "type": "bytes32"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    }
]
''')
CONTRACT_ABI_QUERY_ALLOWED = json.loads('''
[
    {
        "constant": true,
        "inputs": [
            {
                "name": "queryDimensions",
                "type": "string[]"
            }
        ],
        "name": "isQueryAllowed",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": false,
        "stateMutability": "view",
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

def calculate_file_hash(file_path):
    # Calculate the SHA-256 hash of a file
    hasher = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            buf = f.read()
            hasher.update(buf)
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    return hasher.hexdigest()

def get_stored_hash(contract, timestamp):
    # return contract.functions.getHash().call()
    return contract.functions.getHash(timestamp).call()

def c_pos_hash(timestamp):
    file_path = os.path.join('Org1', 'PR_DB', "Sale_PR.csv")
    df = pd.read_csv(file_path)

    # Use TS column to filter and then drop it (it gives problem with the ezkl circuit calibration)
    df = df[df["TS"] <= timestamp] # select rows with TS <= timestamp
    df = df.drop(columns=["TS"])

    df.columns = df.columns.str.strip() # Remove leading and trailing whitespace from column names
    df = df.dropna() # Drop rows with NaN values

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
    

async def publish_hash(timestamp):
    # Calculate the Poseidon hash of the dataset considering the rows before the timestamp
    calculated_hash = c_pos_hash(timestamp)

    # Fix: extract string if it's a list
    if isinstance(calculated_hash, list):
        calculated_hash = calculated_hash[0]
    if not calculated_hash.startswith("0x"):
        calculated_hash = "0x" + calculated_hash

    bytes32_hash = Web3.to_bytes(hexstr=calculated_hash)

    print("Poseidon hash PUBLISHED:", calculated_hash)
    print("Poseidon hash PUBLISHED bytes32:", bytes32_hash)


    web3 = setup_web3()
    # call to get or create the contract instance
    contract = get_contract(web3, CONTRACT_ADDRESS, CONTRACT_ABI_SET_HASH) # hash_utils.py

    account = web3.eth.accounts[0]

    try:
        # setHash() from HashStorage.sol Solidity contract
            # tx_hash = contract.functions.setHash(bytes32_hash).transact({'from': account})
        tx_hash = contract.functions.setHash(timestamp, bytes32_hash).transact({'from': account})
        web3.eth.wait_for_transaction_receipt(tx_hash)
        logging.info(f"Hash {calculated_hash} has been published to the blockchain.")
        return calculated_hash
    except Exception as e:
        logging.error(f"Failed to publish hash: {e}")
        raise

def verify_dataset_hash(timestamp):
    file_path = os.path.join('Org1', 'PR_DB', "Sale_PR.csv")

    # Read the CSV file and filter rows where "TS" <= timestamp
    df = pd.read_csv(file_path)
    df_filtered = df[df["TS"] <= timestamp]

    # Optionally, save or process df_filtered as needed
    print(df_filtered)
    
    calculated_hash = c_pos_hash(timestamp)
    bytes32_hash = Web3.to_bytes(hexstr=calculated_hash)

    web3 = setup_web3()
    contract = get_contract(web3, CONTRACT_ADDRESS, CONTRACT_ABI_GET_HASH)

    stored_hash = get_stored_hash(contract, timestamp)

    #logging.info(f"Calculated hash: {bytes32_hash}")
    #logging.info(f"Stored hash: {stored_hash}")

    if bytes32_hash == stored_hash:
        logging.info("Hash verification successful. The dataset is authentic.")
    else:
        logging.error("Hash verification failed. The dataset has been tampered with.")
        raise ValueError("Hash verification failed. The dataset has been tampered with.")

def verify_query_allowed(query_dimensions, contract_address):
    web3 = setup_web3()
    contract = get_contract(web3, contract_address, CONTRACT_ABI_QUERY_ALLOWED)

    try:
        # .call() is used to call a function that does not modify the state of the blockchain
        is_allowed = contract.functions.isQueryAllowed(query_dimensions).call()
        logging.info(f"Query allowed: {is_allowed}")
        return is_allowed
    except Exception as e:
        logging.error(f"Failed to verify query: {e}")
        raise

"""
def calculate_merkle_root(file_path):
    df = pd.read_csv(file_path)
    tree = MerkleTree(hash_type="sha256")
    for row in df.values:
        # Convert each row to bytes and append to the Merkle tree
        row_bytes = str(row.tolist()).encode()
        tree.append_entry(row_bytes)
    return tree.root_hash    
"""