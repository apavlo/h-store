package edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator;

import edu.brown.rand.RandomDistribution;

public class GaussianGenerator extends AbstractNumberGenerator
{
    public GaussianGenerator(int min, int max)
    {
        super(new RandomDistribution.Gaussian(s_seed, min, max));
    }
}
