import asyncio
import os
import json
import sys

from Org1.StarSchemeGenerator import main as star_scheme_main
from Org1.StarSchemeGenerator import sale_update as sale_update
from Org1.Dim_ID_Converter import CSV_converter
from Org1.hash_utils import publish_hash


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
    # List all files in Org1/PR_DB and let the user choose which one to publish hash for
    db_folder = os.path.join('Org1', 'PR_DB')
    files = [f for f in os.listdir(db_folder) if os.path.isfile(os.path.join(db_folder, f))]

    if not files:
        print('\nNo files found in the Org1/PR_DB folder.')
        return

    print('\nSelect a file from Org1/PR_DB to publish hash:')
    for idx, fname in enumerate(files):
        print(f"[{idx+1}] {fname}")
    choice = input("Enter the number of the file: ")
    try:
        file_index = int(choice) - 1
        if file_index < 0 or file_index >= len(files):
            print("Invalid selection.")
            return
        file_path = os.path.join(db_folder, files[file_index])
    except ValueError:
        print("Invalid input.")
        return

    print("\n")
    hash = await publish_hash(file_path) # HASH_UTILS.py

    share_file(files, file_index, hash) # MAIN.py

    print(f"Hash for {files[file_index]} published successfully.")

# Share file = publish file name and hash in published_hash.json
def share_file(files, file_index, hash):
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

def CLI_convert_CSV():
    db_folder = os.path.join("Org1", "PR_DB")
    files = [f for f in os.listdir(db_folder) if os.path.isfile(os.path.join(db_folder, f))]

    if not files:
        print("No files found in Org1/PR_DB folder.")
        return

    print("\nSelect a file from Org1/PR_DB to convert:")
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
    
    sale_update(selected_file)



async def main():

    while True:
        print("\n\nORG 1 (data provider) select an option:")
        print("[1] Generate File")
        print("[2] Update File")
        print("[3] Convert File")
        print("[4] Publish Hash")
        print("[0] Exit")
        sub_choice = input("Enter your choice (1, 2, 3, 4, or 0): ")

        if sub_choice == "1":  # GENERATE FILE
            star_scheme_main()

        elif sub_choice == "2":  # UPDATE FILE
            CLI_update_file()

        elif sub_choice == "3":  # CONVERT FILE
            CLI_convert_CSV()

        elif sub_choice == "4":  # PUBLISH HASH
            await CLI_publish_hash()

        elif sub_choice == "0":
            print("Exiting.")
            break

        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    asyncio.run(main())