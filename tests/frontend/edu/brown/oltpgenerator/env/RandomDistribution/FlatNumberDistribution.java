package edu.brown.oltpgenerator.env.RandomDistribution;

import java.util.Map;

import edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator.FlatNumGenerator;
import edu.brown.oltpgenerator.gui.common.RandomDistribution.RandomDistributionKey;

public class FlatNumberDistribution extends NumericDistribution
{
    public FlatNumberDistribution(Map<String, Object> params)
    {
        super(params);
    }

    @Override
    protected Object[] getParas()
    {
        Object min = getUserInput(RandomDistributionKey.MIN.name());
        Object max = getUserInput(RandomDistributionKey.MAX.name());

        return new Object[] { min, max };
    }

    @Override
    protected String getRandomGeneratorClassName()
    {
        return FlatNumGenerator.class.getSimpleName();
    }
}
