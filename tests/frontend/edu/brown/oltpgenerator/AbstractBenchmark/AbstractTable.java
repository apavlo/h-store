package edu.brown.oltpgenerator.AbstractBenchmark;

import edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator.AbstractRandomGenerator;

public abstract class AbstractTable
{
    protected abstract String getName();

    protected abstract int getCardinality();

    protected abstract AbstractRandomGenerator[] getColumnValGenerators();
    
    protected abstract String getCsvLinkPath();
}
