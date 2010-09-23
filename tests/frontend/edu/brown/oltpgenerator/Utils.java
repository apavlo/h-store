package edu.brown.oltpgenerator;

import org.voltdb.VoltType;
import org.voltdb.catalog.Column;

public class Utils
{

    public static Column[] getNonNullElements(Column[] itemsArray)
    {
        int nCnt = 0;
        for (Object o : itemsArray)
        {
            if (o != null)
                nCnt++;
        }

        Column[] ret = new Column[nCnt];
        int iSrc = 0, iDest = 0;
        for (; iSrc < itemsArray.length; iSrc++)
        {
            if (itemsArray[iSrc] != null)
                ret[iDest++] = itemsArray[iSrc];
        }
        return ret;
    }

    public static String voltTypeToJavaType(VoltType voltType)
    {
        switch (voltType)
        {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
                return "long";
            case FLOAT:
                return "double";
            case STRING:
                return "String";
            case TIMESTAMP:
                return "Date";
            default:
                throw new RuntimeException("Invalid Volttype: " + voltType);
        }
    }
}
