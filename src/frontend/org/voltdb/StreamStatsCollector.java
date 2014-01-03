package org.voltdb;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Derivation of StatsSource to expose timing information of trigger execution.
 *
 */
//@SuppressWarnings("unused")
public final class StreamStatsCollector extends SiteStatsSource {

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
    private long m_totalDeleteTimedExecutionTime = 0;

    /**
     * Shortest amount of time this procedure has executed in
     */
    private long m_minExecutionTime = Long.MAX_VALUE;
    private long m_minDeleteExecutionTime = Long.MAX_VALUE;

    /**
     * Longest amount of time this procedure has executed in
     */
    private long m_maxExecutionTime = Long.MIN_VALUE;
    private long m_maxDeleteExecutionTime = Long.MIN_VALUE;

    /**
     * Record trigger name
     */
    String m_streamName = "";

    /**
     * Constructor requires no args because it has access to the enclosing classes members.
     */
    public StreamStatsCollector() {
        super("XXX", 1, false);
        
//        super(m_site.getCorrespondingSiteId() + " " + catProc.getClassname(),
//              m_site.getCorrespondingSiteId());
    }
    
    
    
    public final void addStreamInfo(String name, long executionTime, long deleteTime) {
        
        System.out.println("hawk - FrontEnd: entering addStreamInfo..." + Long.toString(executionTime)+" " + Long.toString(deleteTime));
        m_streamName = name;
        
        //if (m_invocations % timeCollectionInterval == 0) 
        {
            {
                final int delta = (int)executionTime;
                final int delete_Delta = (int)deleteTime;
                
                m_totalTimedExecutionTime += delta;
                m_totalDeleteTimedExecutionTime += delete_Delta;

                m_timedInvocations++;
                m_minExecutionTime = Math.min( delta, m_minExecutionTime);
                m_minDeleteExecutionTime = Math.min( delete_Delta, m_minDeleteExecutionTime);
                m_maxExecutionTime = Math.max( delta, m_maxExecutionTime);
                m_maxDeleteExecutionTime = Math.max( delete_Delta, m_maxDeleteExecutionTime);
                
            }
        }
        
        m_invocations++;

        System.out.println("hawk - FrontEnd: ending addStreamInfo with" +  
                Long.toString(executionTime) + " " +  
                Long.toString(deleteTime) + " " +  
                Long.toString(m_totalTimedExecutionTime) + " " +  
                Long.toString(m_totalDeleteTimedExecutionTime) + " " +  
                Long.toString(m_minExecutionTime) + " " +  
                Long.toString(m_maxExecutionTime) + " " +  
                Long.toString(m_minDeleteExecutionTime) + " " +  
                Long.toString(m_maxDeleteExecutionTime) + " " +  
                Long.toString(m_timedInvocations) + " " +  
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
        long timedInvocations = m_timedInvocations;
        long totalTimedExecutionTime = m_totalTimedExecutionTime;
        long totalDeleteTimedExecutionTime = m_totalDeleteTimedExecutionTime;
        long minExecutionTime = m_minExecutionTime;
        long maxExecutionTime = m_maxExecutionTime;
        long minDeleteExecutionTime = m_minDeleteExecutionTime;
        long maxDeleteExecutionTime = m_maxDeleteExecutionTime;
        
        rowValues[columnNameToIndex.get("STREAM")] = m_streamName;
        rowValues[columnNameToIndex.get("INVOCATIONS")] = invocations;
        rowValues[columnNameToIndex.get("TIMED_INVOCATIONS")] = timedInvocations;
        rowValues[columnNameToIndex.get("MIN_EXECUTION_TIME")] = minExecutionTime;
        rowValues[columnNameToIndex.get("MAX_EXECUTION_TIME")] = maxExecutionTime;
        rowValues[columnNameToIndex.get("MIN_DELETE_EXECUTION_TIME")] = minDeleteExecutionTime;
        rowValues[columnNameToIndex.get("MAX_DELETE_EXECUTION_TIME")] = maxDeleteExecutionTime;
        if (timedInvocations != 0) {
            rowValues[columnNameToIndex.get("AVG_EXECUTION_TIME")] =
                 (totalTimedExecutionTime / timedInvocations);
            rowValues[columnNameToIndex.get("AVG_DELETE_EXECUTION_TIME")] =
                    (totalDeleteTimedExecutionTime / timedInvocations);
        } else {
            rowValues[columnNameToIndex.get("AVG_EXECUTION_TIME")] = 0L;
        }
        System.out.println("hawk - FrontEnd: ending updateStatsRow with" +  
                "name: " + (m_streamName) + " " +  
                "invocations: " + Long.toString(invocations) + " " +  
                "invocations: " + Long.toString(minExecutionTime) + " " +  
                "invocations: " + Long.toString(maxExecutionTime) + " " +  
                "totalTimedExecutionTime: " + Long.toString(totalTimedExecutionTime) + " " +  
                "totalDeleteTimedExecutionTime: " + Long.toString(totalDeleteTimedExecutionTime));

    }

    /**
     * Specifies the columns of statistics that are added by this class to the schema of a statistical results.
     * @param columns List of columns that are in a stats row.
     */
    @Override
    protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new VoltTable.ColumnInfo("STREAM", VoltType.STRING));
        columns.add(new VoltTable.ColumnInfo("INVOCATIONS", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("TIMED_INVOCATIONS", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("MIN_EXECUTION_TIME", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("MAX_EXECUTION_TIME", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("AVG_EXECUTION_TIME", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("MIN_DELETE_EXECUTION_TIME", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("MAX_DELETE_EXECUTION_TIME", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("AVG_DELETE_EXECUTION_TIME", VoltType.BIGINT));
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

