package edu.brown.oltpgenerator.env.RandomDistribution;

import java.util.Map;

import edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator.StringGenerator;
import edu.brown.oltpgenerator.gui.common.RandomDistribution.RandomDistributionKey;

public class StringDistribution extends RandomDistribution
{

    public StringDistribution(Map<String, Object> params)
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
        return StringGenerator.class.getSimpleName();
    }

}
