package edu.brown.oltpgenerator.env.RandomDistribution;

import java.util.Map;

import edu.brown.oltpgenerator.gui.common.RandomDistribution.NumberDistribution;

public class NumericDistributionFactory
{

    public static NumericDistribution createNumberColumnEditProperty(NumberDistribution nd,
            Map<String, Object> params)
    {
        switch (nd)
        {
            case DISTRIBUTION_BINOMIAL:
                return new BinomialDistribution(params);
            case DISTRIBUTION_GAUSSIAN:
                return new GaussianDistribution(params);
            case DISTRIBUTION_FLAT:
                return new FlatNumberDistribution(params);
            case DISTRIBUTION_ZIPF:
                return new ZipfDistribution(params);
        }
        throw new RuntimeException("Invalid NumberDistribution type");
    }

}
