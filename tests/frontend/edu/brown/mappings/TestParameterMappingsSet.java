package edu.brown.mappings;

import java.util.Random;

import org.json.*;
import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogKey;
import edu.brown.mappings.ParameterMapping;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestParameterMappingsSet extends BaseTestCase {

    private final Random rand = new Random();
    private ParameterMappingsSet pc;
    private Procedure catalog_proc;
    private ProcParameter catalog_proc_param;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        this.pc = new ParameterMappingsSet();
        this.catalog_proc = catalog_db.getProcedures().get("GetNewDestination");
        this.catalog_proc_param = this.catalog_proc.getParameters().get(0);
        
        double coefficient = 0.0;
        for (Statement catalog_stmt : this.catalog_proc.getStatements()) {
            for (StmtParameter catalog_stmt_param : catalog_stmt.getParameters()) {
                int num_correlations = rand.nextInt(3) + 1;
                for (int i = 0; i < num_correlations; i++) {
                    ParameterMapping c = new ParameterMapping(
                            catalog_stmt,
                            0,
                            catalog_stmt_param,
                            this.catalog_proc_param,
                            0,
                            coefficient
                    );
                    this.pc.add(c);
                    coefficient += 0.03d;
                } // FOR
            } // FOR (StmtParameter)
        } // FOR (Statement)
    }
    
    /**
     * testGet
     */
    public void testGet() throws Exception {
        ParameterMapping c = CollectionUtil.get(this.pc, 2);
        assertNotNull(c);
        ParameterMapping other = CollectionUtil.getFirst(this.pc.get(c.getStatement(), c.getStatementIndex(), c.getStmtParameter()));
        assert(other != null);
        assert(c.equals(other));
    }
    
    /**
     * testToJSONString
     */
    public void testToJSONString() throws Exception {
        String json_string = this.pc.toJSONString();
        assertNotNull(json_string);
        assertFalse(json_string.isEmpty());

        for (Statement catalog_stmt : this.catalog_proc.getStatements()) {
            for (StmtParameter catalog_stmt_param : catalog_stmt.getParameters()) {
                assertTrue(json_string.contains(CatalogKey.createKey(catalog_stmt_param)));
            } // FOR
        } // FOR
    }
    
    /**
     * testFromJSONObject
     */
    public void testFromJSONObject() throws Exception {
        String json_string = this.pc.toJSONString();
        assertNotNull(json_string);
        assertFalse(json_string.isEmpty());

        JSONObject json_object = new JSONObject(json_string);
        assertNotNull(json_object);
        ParameterMappingsSet clone = new ParameterMappingsSet();
        clone.fromJSON(json_object, catalog_db);
        
        // System.err.println(json_object.toString(2));
        
        assertEquals(this.pc.size(), clone.size());
        for (ParameterMapping c : this.pc) {
            assert(clone.contains(c));
        } // FOR
        
    }
}
