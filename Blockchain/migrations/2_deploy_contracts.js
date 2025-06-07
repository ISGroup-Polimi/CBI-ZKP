// Imports Node.js's built-in File System module, which allows you to write the contract addresses to a JSON file
const fs = require('fs');
// Imports Node.js's built-in Path module, which helps in constructing file paths for contract_addresses.json file
const path = require('path');

// load the compiled DataFactModel contract artifacts from .json files
const DataFactModel = artifacts.require("DataFactModel");
const HashStorage = artifacts.require("HashStorage");

// module.exports is used to export a function or object from a file so that it can be used by other parts of the application
// module.exports = ... tells Truffle that this is the function it should run for this migration step
// The function receives the deployer object, which is used to deploy contracts to the blockchain
module.exports = async function (deployer) {
  // Deploy the DataFactModel
  await deployer.deploy(DataFactModel);
  // Wait for the deployment to complete and get the instance and the address of the deployed contract
  const dataFactModel = await DataFactModel.deployed();

  // Deploy the HashStorage contract
  await deployer.deploy(HashStorage);
  // Wait for the deployment to complete and get the instance and the address of the deployed contract
  const hashStorage = await HashStorage.deployed();

  // Creates a JavaScript object containing the deployed addresses of both contracts
  const addresses = {
    DataFactModel: dataFactModel.address,
    HashStorage: hashStorage.address,
  };

  // Save contract addresses to a JSON file
  const filePath = path.join(__dirname, '..', 'contract_addresses.json');
  fs.writeFileSync(filePath, JSON.stringify(addresses, null, 2));

  console.log("Contract addresses saved to contract_addresses.json");
};