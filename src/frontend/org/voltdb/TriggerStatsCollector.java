package org.voltdb;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Derivation of StatsSource to expose timing information of trigger execution.
 *
 */
//@SuppressWarnings("unused")
public final class TriggerStatsCollector extends SiteStatsSource {


    /**
     * Record trigger name
     */
    String m_triggerName = "";

    /**
     * timed execution time
     */
    private long m_timedExecutionTime = 0;
    
    /**
     * Constructor requires no args because it has access to the enclosing classes members.
     */
    public TriggerStatsCollector() {
        super("XXX", 1, false);
        
//        super(m_site.getCorrespondingSiteId() + " " + catProc.getClassname(),
//              m_site.getCorrespondingSiteId());
    }
    
    
    
    public final void addTriggerInfo(String name, long executionTime) {
        
        m_triggerName = name;
        m_timedExecutionTime = executionTime;
        
    }

    /**
     * Update the rowValues array with the latest statistical information.
     * This method is overrides the super class version
     * which must also be called so that it can update its columns.
     * @param values Values of each column of the row of stats. Used as output.
     */
    @Override
    protected void updateStatsRow(Object rowKey, Object rowValues[]) {
        super.updateStatsRow(rowKey, rowValues);
        rowValues[columnNameToIndex.get("TRIGGER")] = m_triggerName;
        rowValues[columnNameToIndex.get("EXECUTION_TIME")] = m_timedExecutionTime;
    }

    /**
     * Specifies the columns of statistics that are added by this class to the schema of a statistical results.
     * @param columns List of columns that are in a stats row.
     */
    @Override
    protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new VoltTable.ColumnInfo("TRIGGER", VoltType.STRING));
        columns.add(new VoltTable.ColumnInfo("EXECUTION_TIME", VoltType.BIGINT));
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new Iterator<Object>() {
            boolean returnRow = true;

            @Override
            public boolean hasNext() {
                return returnRow;
            }

            @Override
            public Object next() {
                if (returnRow) {
                    returnRow = false;
                    return new Object();
                } else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public String toString() {
        return ""; //catalog_proc.getTypeName();
    }
}

