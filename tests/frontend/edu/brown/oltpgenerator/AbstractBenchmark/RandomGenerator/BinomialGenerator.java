package edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator;

import edu.brown.rand.RandomDistribution;

public class BinomialGenerator extends AbstractNumberGenerator
{
    public BinomialGenerator(int min, int max, double p)
    {
        super(new RandomDistribution.Binomial(s_seed, min, max, p));
    }
}
