package edu.brown.benchmark.auctionmark;

public class PartitionIdGenerator {

    private int _numPartitions;
    private int _currentPartition;
    private long _currentId;
    private long _maximumId;

    public PartitionIdGenerator(int numPartitions, long startId, long maximumId) {
        if (numPartitions <= 0)
            throw new IllegalArgumentException("numPartitions must be more than 0 : " + numPartitions);
        if (startId < 0)
            throw new IllegalArgumentException("startId must be more than or equal to 0 : " + startId);
        if (maximumId < startId)
            throw new IllegalArgumentException("maximumId must be more than or equal to startId");

        _numPartitions = numPartitions;
        _currentPartition = 0;
        _currentId = startId;
        _maximumId = maximumId;
    }

    public long getNextId() {
        if (_currentId >= _maximumId) {
            throw new IndexOutOfBoundsException("the generated id is more than the maximumId " + _currentId + " > " + _maximumId);
        }
        long id = ((long) _currentPartition * _maximumId) + _currentId;
        _currentPartition++;
        if (_currentPartition == _numPartitions) {
            _currentPartition = 0;
            _currentId++;

        }
        return id;
    }
}
