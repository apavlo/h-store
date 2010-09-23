package edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator;

import edu.brown.oltpgenerator.RandUtil;

public class StringGenerator extends AbstractRandomGenerator
{
    private int m_min;
    private int m_max;

    private static enum STRING_TYPE {
        NUMBER, CHAR;
    }

    public StringGenerator(int min, int max)
    {
        m_min = min;
        m_max = max;
    }

    @Override
    public Object genRandVal()
    {
        StringBuffer ret = new StringBuffer();
        int length = RandUtil.randInt(m_min, m_max);
        STRING_TYPE[] type = new STRING_TYPE[] { STRING_TYPE.CHAR, STRING_TYPE.NUMBER };

        for (int i = 0; i < length; i++)
        {
            switch ((STRING_TYPE) RandUtil.oneOf(type))
            {
                case NUMBER:
                    ret.append(RandUtil.randNString(1, 1));
                    break;
                default:
                    ret.append(RandUtil.randAString(1, 1));
            }
        }

        return ret.toString();
    }

}
