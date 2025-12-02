import asyncio
import os
import json
import sys
import pandas as pd

from Org1.StarSchemeGenerator import main as generate_star_scheme
from Org1.StarSchemeGenerator import sale_update as sale_update
from Shared.Dim_ID_Converter import CSV_converter
from Org1.hash_utils import publish_hash
from Org1.StarSchemeGenerator import update_products


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

async def CLI_publish_hash():
    file_path = os.path.join('Org1', 'PR_DB', "Sale_PR_C.csv")

    if not os.path.exists(file_path):
        print('\nSale_PR_C.csv not found in the Org1/PR_DB folder.')
        return
    
    # Get the TS as the max value in the TS column
    df = pd.read_csv(file_path)
    if "TS" not in df.columns:
        raise ValueError("Column 'TS' not found in the CSV file.")
    timestamp = int(df["TS"].max())

    # Ask the user to which organization to share the file with
    org_choice = input("Enter the organization number to share the file with (2 or 3): ")

    # Publish the hash on the blockchain
    hash = await publish_hash(timestamp) # HASH_UTILS.py

    # Share in published_hash.json
    share_file(timestamp, hash, org_choice) # MAIN.py

    print(f"File Sale_PR_C.csv shared with Org_{org_choice}.\n")

# This function represent the sharing of the file with another org
# It is done by writing in a json file the timestamp and the hash
def share_file(timestamp, hash, org_choice):
    # Determine the correct path based on org_choice
    if str(org_choice) == "2":
        published_hash_path = os.path.join('OrgB', 'Org2', 'published_hash_2.json')
    elif str(org_choice) == "3":
        published_hash_path = os.path.join('OrgB', 'Org3', 'published_hash_3.json')
    else:
        print("Invalid organization number for publishing hash.")
        return

    # Ensure the directory exists
    os.makedirs(os.path.dirname(published_hash_path), exist_ok=True)  

    # Create the file if it doesn't exist
    if not os.path.exists(published_hash_path):
        with open(published_hash_path, 'w') as f:
            json.dump({}, f, indent=4)

    # Read the existing published hashes
    with open(published_hash_path, 'r') as f:
        published_hashes = json.load(f)

    # Add or update the hash for the given timestamp
    published_hashes[str(timestamp)] = hash

    # Write the updated hashes back to the file
    with open(published_hash_path, 'w') as f:
        json.dump(published_hashes, f, indent=4)
    
"""
# Function to update a file by appending new rows
def CLI_update_file():
    db_folder = os.path.join("Org1", "PR_DB")
    files = [f for f in os.listdir(db_folder) if os.path.isfile(os.path.join(db_folder, f))]

    if not files:
        print("No files found in Org1/PR_DB folder.")
        return

    print("\nSelect a file from Org1/PR_DB to update:")
    for idx, fname in enumerate(files):
        print(f"[{idx+1}] {fname}")
    choice = input("Enter the number of the file to update: ")
    try:
        idx = int(choice) - 1
        if idx < 0 or idx >= len(files):
            print("Invalid selection.")
            return
        selected_file = files[idx]
        print(f"You selected: {selected_file}")
    except ValueError:
        print("Invalid input.")
        return
    
    file_path = os.path.join(db_folder, selected_file)

    sale_update(file_path)
"""


async def main():

    while True:
        print("\n\nORG 1 (data provider) select an option:")
        print("[1] Generate Data")
        print("[2] Update Data")
        print("[3] Update Dimension Table")
        print("[4] Publish Hash")
        print("[0] Exit")
        sub_choice = input("Enter your choice (1, 2, 3, 4, or 0): ")

        if sub_choice == "1":  # GENERATE FILE
            generate_star_scheme()
            CSV_converter()

        elif sub_choice == "2":  # UPDATE FILE
            sale_update()
            CSV_converter()

        elif sub_choice == "3":  # UPDATE DIMENSION TABLE
            update_products()

        elif sub_choice == "4":  # PUBLISH HASH
            await CLI_publish_hash()

        elif sub_choice == "0":
            print("Exiting.")
            break

        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    asyncio.run(main())