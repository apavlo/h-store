package edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator;

public abstract class AbstractRandomGenerator
{
    public abstract Object genRandVal();

    public static Object[] genRandVals(AbstractRandomGenerator[] generators)
    {
        Object[] ret = new Object[generators.length];
        for (int i = 0; i < generators.length; i++)
        {
            ret[i] = generators[i].genRandVal();
        }
        return ret;
    }
}
