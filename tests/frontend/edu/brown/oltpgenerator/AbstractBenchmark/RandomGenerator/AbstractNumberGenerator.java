package edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator;

import java.util.Random;

import edu.brown.rand.RandomDistribution.DiscreteRNG;

public class AbstractNumberGenerator extends AbstractRandomGenerator
{
    protected static Random s_seed = new Random();
    protected DiscreteRNG   m_generator;

    protected AbstractNumberGenerator(DiscreteRNG generator)
    {
        m_generator = generator;
    }

    @Override
    public Object genRandVal()
    {
        return m_generator.nextInt();
    }

}
