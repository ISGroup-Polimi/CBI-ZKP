// This script deploys the DataFactModel contract to the Ethereum blockchain.

// load the compiled DataFactModel contract artifact from DataFactModel.json
const DataFactModel = artifacts.require("DataFactModel");

// module.exports is used to export a function or object from a file so that it can be used by other parts of the application
// module.exports = ... tells Truffle that this is the function it should run for this migration step
// The function receives the deployer object, which is used to deploy contracts to the blockchain
module.exports = function(deployer) {
  deployer.deploy(DataFactModel); // deploy the DataFactModel contract in the Ethereum
};
