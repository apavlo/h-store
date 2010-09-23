package edu.brown.oltpgenerator.env.RandomDistribution;

import java.util.Map;

import edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator.BinomialGenerator;
import edu.brown.oltpgenerator.gui.common.RandomDistribution.RandomDistributionKey;

public class BinomialDistribution extends NumericDistribution
{

    protected BinomialDistribution(Map<String, Object> params)
    {
        super(params);
    }

    @Override
    protected Object[] getParas()
    {
        Object min = getUserInput(RandomDistributionKey.MIN.name());
        Object max = getUserInput(RandomDistributionKey.MAX.name());
        Object p = getUserInput(RandomDistributionKey.P.name());

        return new Object[] { min, max, p };
    }

    @Override
    protected String getRandomGeneratorClassName()
    {
        return BinomialGenerator.class.getSimpleName();
    }
}
