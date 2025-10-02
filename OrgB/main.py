import asyncio
import datetime
import json
import os
import sys
from ezkl import ezkl
from web3 import Web3
import logging

from OrgB.hash_utils import verify_query_allowed
from Org1.execute_query import op_execute_query
from OrgB.hash_utils import compare_hash
from OrgB.hash_utils import get_query_dimensions
from OrgB.hash_utils import show_result
from OrgB.select_operations import select_operations

# Load contract addresses from configuration file
CONFIG_PATH = os.path.join('Blockchain', 'contract_addresses.json')
with open(CONFIG_PATH, 'r') as f:
    contract_addresses = json.load(f)

CONTRACT_ADDRESS = contract_addresses.get("HashStorage")
DATA_FACT_MODEL_ADDRESS = contract_addresses.get("DataFactModel")

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

# Create "published_hash_2.json" and "published_hash_3.json" if they don't exist
def ensure_org_published_hash_files():
    org_paths = [
        os.path.join('OrgB', 'Org2', 'published_hash_2.json'),
        os.path.join('OrgB', 'Org3', 'published_hash_3.json')
    ]
    for path in org_paths:
        dir_path = os.path.dirname(path)
        os.makedirs(dir_path, exist_ok=True)
        if not os.path.exists(path):
            with open(path, 'w') as f:
                json.dump({}, f)

ensure_org_published_hash_files()

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

async def CLI_query(org_n):

    if org_n == 2:
        published_hash_path = os.path.join('OrgB', 'Org2', 'published_hash_2.json')
    elif org_n == 3:   
        published_hash_path = os.path.join('OrgB', 'Org3', 'published_hash_3.json')
    else:
        print("\nInvalid organization number for publishing hash.")
        return

    with open(published_hash_path, 'r') as f:
        published_hashes = json.load(f)
    
    if not published_hashes:
        print(f"\nNo published hashes found in {published_hash_path}. Please ensure that Org1 has published a hash.")
        return
    
    print("\nAvailable timestamps (TS) for published hashes:")
    ts_list = list(published_hashes.keys())
    for idx, ts in enumerate(ts_list, start=1):
        print(f"[{idx}] {ts}")

    try:
        selected_idx = int(input("\nEnter the index of the timestamp (TS) you want to use: ").strip())
        selected_ts = ts_list[selected_idx - 1]
    except (ValueError, IndexError):
        print("Invalid index selected.")
        return

    await op_query(org_n, int(selected_ts))


# This function:
# - defines the OLAP operations to apply
# - verifies if the query is allowed by calling the smart contract in the blockchain
# - executes the query with the defined OLAP operations
# - compare the returned hash with the one stored on the blockchain
# - shows and saves the result of the query
async def op_query(org_n, timestamp): 
    # Define the OLAP operations to apply
    
    operations = {
        "Rollup": [["Clothes Type", "Product Name"], # rollup entire hierarchy 
                   ["Date", "Day"]], # rollup dimension
        "Dicing": [{2: [0, 3]}] 
    }
    
    # operations = select_operations()

    query_dimensions, columns_to_remove_idx = get_query_dimensions(operations) # MAIN.py


    # Verify if the query is allowed by calling the smart contract in the blockchain
    #is_query_allowed = verify_query_allowed(query_dimensions, data_fact_model_address) # HASH_UTILS.py
    # TO CHANGE !!! TO CHANGE !!! TO CHANGE !!!
    is_query_allowed = True 

    if not is_query_allowed:
        print("Query contains disallowed dimensions.")
        return
    print("Query is allowed. Proceeding with query execution...\n")


    try:
        final_tensor, poseidon_hash = await op_execute_query(operations, columns_to_remove_idx, timestamp) # MAIN ORG1.py
    except Exception as e:
        print(f"Failed to execute query: {e}")
        return   


    compare_hash(timestamp, poseidon_hash) # MAIN.py 


    show_result(final_tensor, columns_to_remove_idx, org_n) # MAIN.py


# Verify the zk proof with ezkl using the proof, vk and settings files stored in Shared/proof
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
    
async def main():

    # Ask the user to choose if they are Org2 or Org3
    org_number = input("Select your organization:" \
                        "\n[2] Org2" \
                        "\n[3] Org3\n" \
                        "Enter 2 or 3: ")
    org_n = int(org_number)

    while True:
        print(f"\n\nORG B (data receiver, Org{org_n}) select an option:")
        print("[1] Perform Query")
        print("[2] Verify Proof")
        print("[0] Exit")
        sub_choice = input("Enter your choice (1, 2, or 0): ")

        if sub_choice == "1":  # PERFORM QUERY
            try:
                await CLI_query(org_n)
            except Exception as e:
                print(f"Failed to execute query: {e}")

        elif sub_choice == "2":  # VERIFY PROOF
            try:
                op_verify_proof()
            except Exception as e:
                print(f"Failed to verify proof: {e}")

        elif sub_choice == "0":
            print("Exiting ORG B menu.")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    asyncio.run(main())