// contracts/HashStorage.sol
pragma solidity ^0.8.0;

contract HashStorage {
    // Mapping from timestamp (int) to hash (bytes32)
    mapping(uint256 => bytes32) private timestampToHash;

    // Store a hash with an associated timestamp
    function setHash(uint256 timestamp, bytes32 _hash) public {
        timestampToHash[timestamp] = _hash;
    }

    // Get the hash associated with a timestamp
    function getHash(uint256 timestamp) public view returns (bytes32) {
        return timestampToHash[timestamp];
    }
}