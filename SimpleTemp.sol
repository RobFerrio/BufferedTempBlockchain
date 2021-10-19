pragma solidity ^0.8.7;

contract SimpleTemp {
    struct Measurement {
        int128 temperature;
        uint128 timestamp;
    }

    mapping(address => Measurement[]) public measurements;

    function storeMeasurement(int128 _temp, uint128 _time) public {
        measurements[msg.sender].push(Measurement({temperature: _temp, timestamp: _time}));
    }

    function getMeasurements() public view returns (Measurement[] memory){
        return measurements[msg.sender];
    }
}

