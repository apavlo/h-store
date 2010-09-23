package edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator;

import edu.brown.rand.RandomDistribution;

public class ZipfGenerator extends AbstractNumberGenerator
{
    public ZipfGenerator(int min, int max, double sigma, double epsilon)
    {
        super(new RandomDistribution.Zipf(s_seed, min, max, sigma, epsilon));
    }
}
