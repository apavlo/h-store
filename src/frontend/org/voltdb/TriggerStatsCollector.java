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
     * Record procedure execution time ever N invocations
     */
    //final int timeCollectionInterval = 20;

    /**
     * Number of times this procedure has been invoked.
     */
    private long m_invocations = 0;

    /**
     * Number of timed invocations
     */
    private long m_timedInvocations = 0;

    /**
     * Total amount of timed execution time
     */
    private long m_totalTimedExecutionTime = 0;

    /**
     * Shortest amount of time this procedure has executed in
     */
    private long m_minExecutionTime = Long.MAX_VALUE;

    /**
     * Longest amount of time this procedure has executed in
     */
    private long m_maxExecutionTime = Long.MIN_VALUE;

    /**
     * Record trigger name
     */
    String m_triggerName = "";

    /**
     * Constructor requires no args because it has access to the enclosing classes members.
     */
    public TriggerStatsCollector() {
        super("XXX", 1, false);
        
//        super(m_site.getCorrespondingSiteId() + " " + catProc.getClassname(),
//              m_site.getCorrespondingSiteId());
    }
    
    
    
    public final void addTriggerInfo(String name, long executionTime) {
        
        System.out.println("hawk: entering addTriggerInfo...");
        m_triggerName = name;
        
        //if (m_invocations % timeCollectionInterval == 0) 
        {
            {
                final int delta = (int)executionTime;
                m_totalTimedExecutionTime += delta;
                m_timedInvocations++;
                m_minExecutionTime = Math.min( delta, m_minExecutionTime);
                m_maxExecutionTime = Math.max( delta, m_maxExecutionTime);
                
            }
        }
        
        m_invocations++;

        System.out.println("hawk: ending addTriggerInfo with" +  
                Long.toString(executionTime) + " " +  
                Long.toString(m_totalTimedExecutionTime) + " " +  
                Long.toString(m_timedInvocations) + " " +  
                Long.toString(m_minExecutionTime) + " " +  
                Long.toString(m_maxExecutionTime) + " " +  
                Long.toString(m_invocations));
        
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
        
        long invocations = m_invocations;
        long totalTimedExecutionTime = m_totalTimedExecutionTime;
        long timedInvocations = m_timedInvocations;
        long minExecutionTime = m_minExecutionTime;
        long maxExecutionTime = m_maxExecutionTime;
        
        rowValues[columnNameToIndex.get("TRIGGER")] = m_triggerName;
        rowValues[columnNameToIndex.get("INVOCATIONS")] = invocations;
        rowValues[columnNameToIndex.get("TIMED_INVOCATIONS")] = timedInvocations;
        rowValues[columnNameToIndex.get("MIN_EXECUTION_TIME")] = minExecutionTime;
        rowValues[columnNameToIndex.get("MAX_EXECUTION_TIME")] = maxExecutionTime;
        if (timedInvocations != 0) {
            rowValues[columnNameToIndex.get("AVG_EXECUTION_TIME")] =
                 (totalTimedExecutionTime / timedInvocations);
        } else {
            rowValues[columnNameToIndex.get("AVG_EXECUTION_TIME")] = 0L;
        }
    }

    /**
     * Specifies the columns of statistics that are added by this class to the schema of a statistical results.
     * @param columns List of columns that are in a stats row.
     */
    @Override
    protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new VoltTable.ColumnInfo("TRIGGER", VoltType.STRING));
        columns.add(new VoltTable.ColumnInfo("INVOCATIONS", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("TIMED_INVOCATIONS", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("MIN_EXECUTION_TIME", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("MAX_EXECUTION_TIME", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("AVG_EXECUTION_TIME", VoltType.BIGINT));
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
        return ""; 
    }
}

