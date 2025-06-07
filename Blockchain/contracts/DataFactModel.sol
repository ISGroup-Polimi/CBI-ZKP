// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract DataFactModel {
    struct Dimensions {
        bool ProductName;
        bool Category;
        bool Material;
        bool Year;
        bool Month;
        bool Day;
        bool TotalEmissionsKgCO2e;
    }

    Dimensions public allowedDimensions;

    event QueryCheck(address indexed user, string[] queryDimensions);

    constructor() {
        allowedDimensions.ProductName = false;
        allowedDimensions.Category = false;
        allowedDimensions.Material = true;
        allowedDimensions.Year = true;
        allowedDimensions.Month = true;
        allowedDimensions.Day = false;
        allowedDimensions.TotalEmissionsKgCO2e = true;
    }

    function isQueryAllowed(string[] memory queryDimensions) public view returns (bool) {
        for (uint i = 0; i < queryDimensions.length; i++) {
            if (keccak256(abi.encodePacked(queryDimensions[i])) == keccak256(abi.encodePacked("Product Name")) && !allowedDimensions.ProductName) return false;
            if (keccak256(abi.encodePacked(queryDimensions[i])) == keccak256(abi.encodePacked("Category")) && !allowedDimensions.Category) return false;
            if (keccak256(abi.encodePacked(queryDimensions[i])) == keccak256(abi.encodePacked("Material")) && !allowedDimensions.Material) return false;
            if (keccak256(abi.encodePacked(queryDimensions[i])) == keccak256(abi.encodePacked("Year")) && !allowedDimensions.Year) return false;
            if (keccak256(abi.encodePacked(queryDimensions[i])) == keccak256(abi.encodePacked("Month")) && !allowedDimensions.Month) return false;
            if (keccak256(abi.encodePacked(queryDimensions[i])) == keccak256(abi.encodePacked("Day")) && !allowedDimensions.Day) return false;
            if (keccak256(abi.encodePacked(queryDimensions[i])) == keccak256(abi.encodePacked("Total Emissions (kgCO2e)")) && !allowedDimensions.TotalEmissionsKgCO2e) return false;
        }
        return true;
    }

    function executeQuery(string[] memory queryDimensions) public {
        require(isQueryAllowed(queryDimensions), "Query contains disallowed dimensions");

        emit QueryCheck(msg.sender, queryDimensions);
    }
}