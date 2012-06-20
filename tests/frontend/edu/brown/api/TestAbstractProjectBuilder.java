package edu.brown.api;

import junit.framework.TestCase;

import org.junit.Test;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.utils.ProjectType;

public class TestAbstractProjectBuilder extends TestCase {

    /**
     * testTransactionFrequencies
     */
    @Test
    public void testTransactionFrequencies() throws Exception {
        for (ProjectType type : new ProjectType[] { ProjectType.AUCTIONMARK,
                                                    ProjectType.TPCC,
                                                    ProjectType.TM1,
                                                    ProjectType.TPCE } ) {
            AbstractProjectBuilder pb = AbstractProjectBuilder.getProjectBuilder(type);
            assertNotNull(type.toString(), pb);
            String txn_freq = pb.getTransactionFrequencyString();
            assertNotNull(txn_freq);
            assert(txn_freq.length() > 0);
        } // FOR
    }
    
}
