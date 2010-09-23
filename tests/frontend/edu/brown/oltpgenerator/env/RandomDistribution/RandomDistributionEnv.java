package edu.brown.oltpgenerator.env.RandomDistribution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;

import edu.brown.oltpgenerator.gui.common.RandomDistribution.NumberDistribution;
import edu.brown.oltpgenerator.gui.common.RandomDistribution.RandomDistributionKey;

public class RandomDistributionEnv
{

    private static Map<CatalogType, RandomDistribution> s_mapProperty = new HashMap<CatalogType, RandomDistribution>();

    public static Map<CatalogType, RandomDistribution> getPropertyMap()
    {
        return s_mapProperty;
    }

    public static boolean firstTimeSee(CatalogType catalogVal)
    {
        return !(s_mapProperty.containsKey(catalogVal));
    }

    public static void put(CatalogType catalogVal, RandomDistribution property)
    {
        s_mapProperty.put(catalogVal, property);
    }

    public static RandomDistribution get(CatalogType catalogVal)
    {
        return s_mapProperty.get(catalogVal);
    }

    public static void clearColumnEdits()
    {
        List<CatalogType> keysToClear = new ArrayList<CatalogType>();
        int cnt = 0;
        for (CatalogType key : s_mapProperty.keySet())
        {
            if (key instanceof Column)
            {
                keysToClear.add(key);
                cnt++;
            }
        }

        for (CatalogType key : keysToClear)
        {
            s_mapProperty.remove(key);
        }

        System.out.println(cnt + " ColumnEdit records removed in TableEnv");
    }

    public static void clearProcParaEdits()
    {
        List<CatalogType> keysToClear = new ArrayList<CatalogType>();
        int cnt = 0;
        for (CatalogType key : s_mapProperty.keySet())
        {
            if (key instanceof ProcParameter)
            {
                keysToClear.add(key);
                cnt++;
            }
        }

        for (CatalogType key : keysToClear)
        {
            s_mapProperty.remove(key);
        }

        System.out.println(cnt + " ParaEdit records removed in ProcEnv");
    }

    public static RandomDistribution getDefaultProperty(CatalogType catalogVal)
    {
        if (catalogVal instanceof Column)
        {
            return RandomDistributionEnv.getDefaultProperty(VoltType.get(((Column) catalogVal).getType()));
        }
        else
        {
            return RandomDistributionEnv.getDefaultProperty(VoltType.get(((ProcParameter) catalogVal).getType()));
        }
    }

    private static RandomDistribution getDefaultProperty(VoltType type)
    {
        switch (type)
        {
            case STRING:
                return getDefaultString();
            case TIMESTAMP:
                return getDefaultDate();
            default:
                return getDefaultNumber();
        }
    }

    private static Map<String, Object> getDefaultMap()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(RandomDistributionKey.MIN.name(), 1);
        map.put(RandomDistributionKey.MAX.name(), 2);
        return map;
    }

    private static RandomDistribution getDefaultNumber()
    {
        Map<String, Object> map = getDefaultMap();
        map.put(RandomDistributionKey.SELECTED_DISTRIBUTION.name(), NumberDistribution.DISTRIBUTION_FLAT.m_sName);
        return new FlatNumberDistribution(map);
    }

    private static RandomDistribution getDefaultDate()
    {
        return new DateDistribution(getDefaultMap());
    }

    private static RandomDistribution getDefaultString()
    {
        return new StringDistribution(getDefaultMap());
    }
}
