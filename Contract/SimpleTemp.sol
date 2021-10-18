pragma solidity ^0.8.7;

contract SimpleTemp {
    struct Measurement {
        int temperature;
        uint timestamp;
    }

    Measurement[] public measurements;
    uint public measurements_counter;

    function storeMeasurement(int _temp, uint _time) public {
        measurements.push(Measurement({temperature: _temp, timestamp: _time}));
        measurements_counter++;
    }

    function getMeasurements() public view returns (Measurement[] memory){
        return measurements;
    }
}
