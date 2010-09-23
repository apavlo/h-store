package edu.brown.oltpgenerator.env.RandomDistribution;

import java.util.Map;

public abstract class RandomDistribution
{
    private Map<String, Object> m_mapParams;

    protected RandomDistribution(Map<String, Object> params)
    {
        m_mapParams = params;
    }

    public Map<String, Object> getUserInputMap()
    {
        return m_mapParams;
    }

    public Object getUserInput(String key)
    {
        return m_mapParams.get(key);
    }

    /**
     * For example,  "new FlatNumGenerator(1, 2)"
     * @return
     */
    public String getRandomGeneratorConstructingStatement()
    {
        String className = getRandomGeneratorClassName();
        Object[] paras = getParas();

        String ret = "new " + className + "(";
        int i = 0;
        for (; i < paras.length - 1; i++)
        {
            ret = ret + (paras[i].toString() + ", ");
        }
        ret = ret + paras[i].toString() + ")";

        return ret;
    }

    protected abstract String getRandomGeneratorClassName();

    protected abstract Object[] getParas();
}
