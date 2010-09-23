package edu.brown.oltpgenerator.env.RandomDistribution;

import java.util.Map;

import edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator.ZipfGenerator;
import edu.brown.oltpgenerator.gui.common.RandomDistribution.RandomDistributionKey;

public class ZipfDistribution extends NumericDistribution
{

    protected ZipfDistribution(Map<String, Object> params)
    {
        super(params);
    }

    @Override
    protected Object[] getParas()
    {
        Object min = getUserInput(RandomDistributionKey.MIN.name());
        Object max = getUserInput(RandomDistributionKey.MAX.name());
        Object sigma = getUserInput(RandomDistributionKey.SIGMA.name());
        Object epsilon = getUserInput(RandomDistributionKey.EPSILON.name());

        return new Object[] { min, max, sigma, epsilon };
    }

    @Override
    protected String getRandomGeneratorClassName()
    {
        return ZipfGenerator.class.getSimpleName();
    }
}
