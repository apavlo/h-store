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
    private long m_lastInvocations = 0;

    /**
     * Number of timed invocations
     */
    private long m_timedInvocations = 0;
    private long m_lastTimedInvocations = 0;

    /**
     * Total amount of timed execution time
     */
    private long m_totalTimedExecutionTime = 0;
    private long m_lastTotalTimedExecutionTime = 0;

    /**
     * Shortest amount of time this procedure has executed in
     */
    private long m_minExecutionTime = Long.MAX_VALUE;
    private long m_lastMinExecutionTime = Long.MAX_VALUE;

    /**
     * Longest amount of time this procedure has executed in
     */
    private long m_maxExecutionTime = Long.MIN_VALUE;
    private long m_lastMaxExecutionTime = Long.MIN_VALUE;

    /**
     * Whether to return results in intervals since polling or since the beginning
     */
    private boolean m_interval = false;

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
        
        m_triggerName = name;
        
        //if (m_invocations % timeCollectionInterval == 0) 
        {
            {
                final int delta = (int)executionTime;
                m_totalTimedExecutionTime += delta;
                m_timedInvocations++;
                m_minExecutionTime = Math.min( delta, m_minExecutionTime);
                m_maxExecutionTime = Math.max( delta, m_maxExecutionTime);
                m_lastMinExecutionTime = Math.min( delta, m_lastMinExecutionTime);
                m_lastMaxExecutionTime = Math.max( delta, m_lastMaxExecutionTime);
                
            }
        }
        
        m_invocations++;
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
        
        if (m_interval) {
            invocations = m_invocations - m_lastInvocations;
            m_lastInvocations = m_invocations;

            totalTimedExecutionTime = m_totalTimedExecutionTime - m_lastTotalTimedExecutionTime;
            m_lastTotalTimedExecutionTime = m_totalTimedExecutionTime;

            timedInvocations = m_timedInvocations - m_lastTimedInvocations;
            m_lastTimedInvocations = m_timedInvocations;

            minExecutionTime = m_lastMinExecutionTime;
            maxExecutionTime = m_lastMaxExecutionTime;
            m_lastMinExecutionTime = Long.MAX_VALUE;
            m_lastMaxExecutionTime = Long.MIN_VALUE;
        }
        
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
        m_interval = interval;
        return new Iterator<Object>() {
            boolean givenNext = false;
            @Override
            public boolean hasNext() {
                if (!m_interval) {
                    if (m_invocations == 0) {
                        return false;
                    }
                } else if (m_invocations - m_lastInvocations == 0){
                    return false;
                }
                return !givenNext;
            }

            @Override
            public Object next() {
                if (!givenNext) {
                    givenNext = true;
                    return new Object();
                }
                return null;
            }

            @Override
            public void remove() {}

        };
    }

    @Override
    public String toString() {
        return ""; 
    }
}

