package edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator;

import java.util.Date;

import edu.brown.benchmark.tpce.util.RandUtil;

public class DateGenerator extends AbstractRandomGenerator
{
    private Date m_min;
    private Date m_max;
    
    public DateGenerator(Date min, Date max)
    {
        m_min = min;
        m_max = max;
    }

    @Override
    public Object genRandVal()
    {
        return RandUtil.randDate(m_min, m_max);
    }

}
