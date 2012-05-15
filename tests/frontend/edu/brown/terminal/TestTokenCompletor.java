package edu.brown.terminal;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.types.QueryType;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestTokenCompletor extends BaseTestCase {
    
    private TokenCompletor completor;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.completor = new TokenCompletor(catalog);
    }
    
    private List<String> getCandidates(String sql) {
        int cursor = sql.length();
        List<String> clist = new ArrayList<String>();
        this.completor.complete(sql, cursor, clist);
        System.err.println(clist);
        assertFalse(sql, clist.isEmpty());
        for (int i = 0, cnt = clist.size(); i < cnt; i++) {
            clist.set(i, clist.get(i).trim());
        } // FOR
        return (clist);
    }
    
    /**
     * testInitialComplete
     */
    @Test
    public void testInitialComplete() throws Exception {
        String sql = "";
        List<String> clist = this.getCandidates(sql);
        
        // Our candidate should be all our valid query types
        for (QueryType qtype : QueryType.values()) {
            if (qtype == QueryType.INVALID || qtype == QueryType.NOOP) continue;
            assertTrue(qtype.toString(), clist.contains(qtype.name()));
        } // FOR
    }
    
    /**
     * testFromComplete
     */
    @Test
    public void testFromComplete() throws Exception {
        String sql = "SELECT * FROM CU";
        List<String> clist = this.getCandidates(sql);
        
        // Our candidate matches should be table names
        for (String match : clist) {
            assertNotNull(match, catalog_db.getTables().getIgnoreCase(match));
        } // FOR
    }
    
    /**
     * testExecComplete
     */
    @Test
    public void testExecComplete() throws Exception {
        String sql = "EXEC " + neworder.class.getSimpleName().substring(0, 4);
        List<String> clist = this.getCandidates(sql);
        
        // Our candidate matches should be procedure names
        // This is case sensitive!
        for (String match : clist) {
            assertNotNull(match, catalog_db.getProcedures().get(match));
        } // FOR
    }

}
