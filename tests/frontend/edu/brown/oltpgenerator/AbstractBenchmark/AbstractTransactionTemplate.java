package edu.brown.oltpgenerator.AbstractBenchmark;

import org.voltdb.VoltProcedure;

import edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator.AbstractRandomGenerator;

/**
 * @author zhe
 * 
 */
public abstract class AbstractTransactionTemplate extends VoltProcedure
{
    /**
     * @return Probability this procedure is executed
     */
    protected abstract int getProbability();

    /**
     * @return index of this transaction among all transactions
     */
    protected abstract int getIndex();

    protected abstract AbstractRandomGenerator[] getParaValGenerators();
}
