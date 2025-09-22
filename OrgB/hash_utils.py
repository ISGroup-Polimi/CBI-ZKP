import logging
from web3 import Web3
import json
import os

logging.basicConfig(level=logging.INFO)

# Load contract addresses from configuration file
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Blockchain', 'contract_addresses.json')
with open(CONFIG_PATH, 'r') as f:
    contract_addresses = json.load(f)

CONTRACT_ADDRESS = contract_addresses.get("HashStorage")
DATA_FACT_MODEL_ADDRESS = contract_addresses.get("DataFactModel")

# ABI (Application Binary Interface) is a JSON description of the contract's functions and events
# It allows us to interact with the contract using web3.py
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