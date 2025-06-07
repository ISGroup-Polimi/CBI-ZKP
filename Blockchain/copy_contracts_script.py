# Script to copy JSON contract files from build/contracts to Blockchain/json_contracts
import shutil
import os

src_dir = 'build/contracts'
dst_dir = 'Blockchain/json_contracts'

os.makedirs(dst_dir, exist_ok=True)

for filename in os.listdir(src_dir):
    if filename.endswith('.json'):
        shutil.copy2(os.path.join(src_dir, filename), os.path.join(dst_dir, filename))
print("Contracts copied from from build/contracts to Blockchain/json_contracts.")