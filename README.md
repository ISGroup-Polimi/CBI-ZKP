# CBI-ZKP

This code implements a Zero-Knowledge Proofs framework, which allows Organizations to share analytical insights ensuring data sovereignty for the data owner (Org1) and data trustworthiness for the data receivers (Org2 and Org3).

Note: This project is developed and tested on Linux.

## Requirements
Install the dependencies with:

```bash
python3 -m pip install -r requirements.txt
```

## Blockchain (local node)
To start working with this project, first launch a local Ethereum development node to create a private lightweight blockchain that runs on your machine. Open a terminal and run the following command:

```bash
geth --dev --http --http.addr 127.0.0.1 --http.port 8545 --http.api eth,web3,personal,net
```

## Contracts
After the development node is running, open a new terminal and change into the Blockchain folder of the project:

```bash
cd CBI-ZKP/Blockchain
```

Compile the smart contracts (this generates the artifacts used by the rest of the system, e.g., build/contracts):

```bash
truffle compile
```

Deploy the compiled contracts to the local development chain:

```bash
truffle migrate
```

## Org1 (data owner)
From the _CBI-ZKP_ folder, launch Org1 (the data owner) with:

```bash
python3 -m Org1.main
```

Use the CLI command _`Generate Data`_ to create: 
- [Date.csv](./Date.csv), [Material.csv](./Material.csv) and [Products.csv](./Products.csv) dimensional tables;
- [Sale_PR.csv](./Sale_PR.csv) central fact table.

A human-readable version of the fact table is also produced automatically:
- [Sale_PR_C.csv](./Sale_PR_C.csv).

> Note
>
> You can view the logical model here [LogicalModel.jpg](./LogicalModel.jpg).

From Org1's CLI you can perform two update operations:

- _`Update Data`_ to append new rows to the Sales fact table;
- _`Update Dimensional Table`_ add rows to the dimensional tables.

Use the _`Publish Hash`_ operation to compute the Poseidon hash of the Sales fact table and publish that hash (with the current timestamp) on-chain via the deployed smart contract. This creates a tamper‑evident, auditable record stored on the blockchain.

From Org1’s CLI you will be prompted whether to share the on‑chain hash with Org2 or Org3. 

## OrgB (Org2 / Org3) — queries and verification
Run:

```bash
python3 -m OrgB.main
```

and select to login as Org2 or Org3.

Perform queries with _`Perform Query`_ command, select the data version by timestamp (TS) and choose the OLAP operation(s) to apply.
Supported OLAP operations include Rollup, Slice and Dice; pick the target dimensions and filter values for the operation.

> Note
>
> DataFactModel.sol enforces which dimensions may be used. ProductName and Day are excluded; any operation that tries to use those dimensions will be rejected. See the contract [DataFactModel.sol](./Blockchain/contracts/DataFactModel.sol), where it can be modified as desired.

After the smart contract verifies the operation is allowed, the ezkl ZKP circuit:
- computes the Poseidon hash of the selected data version together with its timestamp and compares it to the on‑chain value.
- produces the proof files (test.pf, test.vk and settings.json) and shares them with the counterparty (Org2 or Org3, depending on the login).

Lastly you can run the _`Verify Proof`_ command, which uses the proof files to verify the computation with ezkl.