package edu.brown.oltpgenerator.AbstractBenchmark;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.oltpgenerator.RandUtil;
import edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator.AbstractRandomGenerator;

public abstract class AbstractClient extends BenchmarkComponent
{
    private AbstractTransactionTemplate[] m_xacts;

    private AbstractTransactionTemplate   m_xactToInvoke;

    private ProcedureCallback     m_callBack    = new ProcedureCallback()
                                                {

                                                    @Override
                                                    public void clientCallback(ClientResponse clientResponse)
                                                    {
                                                        incrementTransactionCounter(clientResponse, m_xactToInvoke.getIndex());
                                                    }
                                                };

    private int[]                 m_tblIndices;

    private static final int      NO_TRASACTION = -1;

    public AbstractClient(String[] args)
    {
        super(args);

        m_xacts = getTransactions();
        if (m_xacts == null || m_xacts.length == 0)
        {
            throw new RuntimeException("No transactions!");
        }

        m_xactToInvoke = m_xacts[0];

        initIndicesTable(m_xacts);
    }

    protected abstract AbstractTransactionTemplate[] getTransactions();

    private void initIndicesTable(AbstractTransactionTemplate[] xacts)
    {
        m_tblIndices = new int[100];
        int i = 0;
        for (; i < 100; i++)
        {
            m_tblIndices[i] = NO_TRASACTION;
        }

        i = 0;
        for (AbstractTransactionTemplate xact : xacts)
        {
            for (int cnt = 0, idx = xact.getIndex(); cnt < xact.getProbability(); cnt++)
            {
                m_tblIndices[i++] = idx;
            }
        }
    }

    @Override
    public String[] getTransactionDisplayNames()
    {
        String[] ret = new String[m_xacts.length];
        for (int i = 0; i < ret.length; i++)
        {
            ret[i] = m_xacts[i].getClass().getSimpleName();
        }
        return ret;
    }

    @Override
    public void runLoop()
    {
        try
        {
            while (true)
            {
                m_xactToInvoke = pickTransaction();
                Object[] paras = AbstractRandomGenerator.genRandVals(m_xactToInvoke.getParaValGenerators());
                getClientHandle().callProcedure(m_callBack, m_xactToInvoke.getClass().getSimpleName(), paras);
                getClientHandle().backpressureBarrier();
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

    private AbstractTransactionTemplate pickTransaction()
    {
        int idxXact;
        while (NO_TRASACTION == m_tblIndices[(idxXact = RandUtil.randInt(0, 99))])
        {
            ;
        }

        return m_xacts[idxXact];
    }

}
