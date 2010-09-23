package edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator;

import edu.brown.rand.RandomDistribution;

public class FlatNumGenerator extends AbstractNumberGenerator
{

    public FlatNumGenerator(int min, int max)
    {
        super(new RandomDistribution.Flat(s_seed, min, max));
    }
}
