package edu.brown.mappings;

import java.io.File;
import java.util.*;

import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.mappings.AbstractMapping;
import edu.brown.mappings.ParameterMapping;
import edu.brown.mappings.MappingCalculator;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.mappings.MappingCalculator.*;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.workload.*;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.ProcParameterValueFilter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestMappingCalculator extends BaseTestCase {
    private static final int WORKLOAD_XACT_LIMIT = 1000;
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    
    private static Workload workload;
    private Random rand = new Random(0);
    private MappingCalculator pc;
    private Procedure catalog_proc;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);

        if (workload == null) {
            File file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalog);
            Filter filter = new ProcedureNameFilter(false)
                    .include(TARGET_PROCEDURE.getSimpleName())
                    .attach(new ProcParameterValueFilter().include(1, new Integer(1))) // D_ID
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            ((Workload) workload).load(file, catalog_db, filter);
        }
        assert(workload.getTransactionCount() > 0);
        
        // Setup
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.pc = new MappingCalculator(catalog_db);
    }
    
    /**
     * testQueryInstance
     */
    public void testQueryInstance() {
        ProcedureMappings procc = this.pc.getProcedureCorrelations(this.catalog_proc);
        procc.start();
        for (Statement catalog_stmt : this.catalog_proc.getStatements()) {
            Set<QueryInstance> previous = new HashSet<QueryInstance>();
            for (int i = 0; i < 2; i++) {
                QueryInstance query_instance = procc.getQueryInstance(catalog_stmt);
                assertNotNull(query_instance);
                assertEquals(i, query_instance.getSecond().intValue());
                assertFalse(previous.contains(query_instance));
                previous.add(query_instance);
            } // FOR
        } // FOR
        procc.finish();
    }
    
    /**
     * testProcParameterCorrelation
     */
    public void testProcParameterCorrelation() {
        ProcedureMappings procc = this.pc.getProcedureCorrelations(this.catalog_proc);
        procc.start();
        
        Statement catalog_stmt = CollectionUtil.first(this.catalog_proc.getStatements());
        assertNotNull(catalog_stmt);

        QueryInstance query_instance = procc.getQueryInstance(catalog_stmt);
        assertNotNull(query_instance);
        assertEquals(0, query_instance.getSecond().intValue());
        for (StmtParameter catalog_stmt_param : catalog_stmt.getParameters()) {
            Set<AbstractMapping> previous = new HashSet<AbstractMapping>();
            
            for (ProcParameter catalog_proc_param : this.catalog_proc.getParameters()) {
                ProcParameterCorrelation ppc = query_instance.getProcParameterCorrelation(catalog_stmt_param, catalog_proc_param);
                assertNotNull(ppc);
                assertEquals(catalog_proc_param.getIsarray(), ppc.getIsArray());
                assertEquals(catalog_proc_param, ppc.getProcParameter());
                
                int cnt = (ppc.getIsArray() ? rand.nextInt(10) : 1);
                for (int i = 0; i < cnt; i++) {
                    AbstractMapping correlation = ppc.getAbstractCorrelation(i);
                    assertFalse(previous.contains(correlation));
                    previous.add(correlation);
                } // FOR
            } // FOR
        } // FOR
        procc.finish();
    }
    
    /**
     * testProcessTransaction
     */
    public void testProcessTransaction() throws Exception {
        int ctr = 0;
        for (TransactionTrace xact_trace : workload) {
            assertNotNull(xact_trace);
            this.pc.processTransaction(xact_trace);
            if (ctr++ >= 100) break;
        } // FOR
        this.pc.calculate();
        
        ProcedureMappings procc = this.pc.getProcedureCorrelations(this.catalog_proc);
        assertNotNull(procc);
//         System.err.println(xact_trace.debug(catalog_db));
        
        Statement catalog_stmt = this.catalog_proc.getStatements().get("getStockInfo");
        assertNotNull(catalog_stmt);
        
        double threshold = 1.0d;
        ParameterMappingsSet pc = procc.getCorrelations(threshold);
        assertNotNull(pc);
//        System.err.println(pc.debug());
        SortedMap<Integer, SortedMap<StmtParameter, SortedSet<ParameterMapping>>> stmt_correlations = pc.get(catalog_stmt);
        assertNotNull(stmt_correlations);
        assertFalse(stmt_correlations.isEmpty());
//        assertEquals(1, stmt_correlations.size());
        SortedMap<StmtParameter, SortedSet<ParameterMapping>> param_correlations = stmt_correlations.get(0);
        assertEquals(catalog_stmt.getParameters().size(), param_correlations.size());
        
//        System.err.println(procc.debug(catalog_stmt));
//        System.err.print(CorrelationCalculator.DEFAULT_DOUBLE_LINE);
//        for (StmtParameter catalog_stmt_param : param_correlations.keySet()) {
//            System.err.println(catalog_stmt_param + ": " + param_correlations.get(catalog_stmt_param));
//        }
//        System.err.print(CorrelationCalculator.DEFAULT_DOUBLE_LINE);
//        System.err.println(pc.debug(catalog_stmt));
//        System.err.print(CorrelationCalculator.DEFAULT_DOUBLE_LINE);
//        System.err.println("FULL DUMP:");
//        for (Correlation c : pc) {
//            if (c.getStatement().equals(catalog_stmt)) System.err.println("   " + c);
//        }
//        System.err.print(CorrelationCalculator.DEFAULT_DOUBLE_LINE);
        
        ProcParameter expected_param[] = new ProcParameter[] {
                this.catalog_proc.getParameters().get(4),
                this.catalog_proc.getParameters().get(5),
        };
        int expected_index[] = { 0, 14 }; // ???
        
        for (int i = 0, cnt = catalog_stmt.getParameters().size(); i < cnt; i++) {
            StmtParameter catalog_param = catalog_stmt.getParameters().get(i);
            assertNotNull(catalog_param);
            ParameterMapping c = CollectionUtil.first(param_correlations.get(catalog_param));
            assertNotNull(c);
            
            assert(c.getCoefficient() >= threshold);
            assertEquals("[" + i + "]", expected_param[i], c.getProcParameter());
            assertEquals("[" + i + "]", expected_index[i], c.getProcParameterIndex());
        } // FOR
    }
    
}
