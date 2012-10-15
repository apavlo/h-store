package edu.brown.mappings;

import org.json.*;
import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.mappings.ParameterMapping;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestParameterMapping extends BaseTestCase {

    private static final double DEFAULT_CORRELATION_COEFFCIENT = 0.5d;
    
    private Procedure catalog_proc;
    private Procedure catalog_other_proc;
    private ProcParameter catalog_proc_param;
    private Statement catalog_other_stmt;
    private ParameterMapping correlation;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        this.catalog_proc = this.getProcedure("GetNewDestination");
        this.catalog_proc_param = this.catalog_proc.getParameters().get(0);
        assertNotNull(this.catalog_proc_param);
        
        this.catalog_other_proc = this.getProcedure("UpdateLocation");
        this.catalog_other_stmt = CollectionUtil.first(catalog_other_proc.getStatements());
        assertNotNull(this.catalog_other_stmt);
        
        for (Statement stmt : this.catalog_proc.getStatements()) {
            this.correlation = new ParameterMapping(
                    stmt,                           // Statement
                    0,                              // Statement Instance Index
                    stmt.getParameters().get(0),    // StmtParameter
                    this.catalog_proc_param,        // ProcParameter
                    0,                              // ProcParameter Array Index
                    DEFAULT_CORRELATION_COEFFCIENT  // Correlation Coefficient
            );
            break;
        } // FOR
        this.examineCorrelation(this.correlation);
    }
    
    private void examineCorrelation(ParameterMapping c) {
        assertNotNull(c);
        assertNotNull(c.getStatement());
        assertNotNull(c.getStatementIndex());
        assertNotNull(c.getStmtParameter());
        assertNotNull(c.getProcParameter());
        assertNotNull(c.getProcParameterIndex());
        assertNotNull(c.getCoefficient());
        assertTrue(c.getStatementIndex() >= 0);
        assertTrue(c.getCoefficient() >= 0);
        
        if (c.getProcParameter().getIsarray()) {
            assertTrue(c.getProcParameterIndex() >= 0);
        } else {
            assertEquals(ParametersUtil.NULL_PROC_PARAMETER_OFFSET, c.getProcParameterIndex());
        }
    }
    
    /**
     * testCompareTo
     */
    public void testCompareTo() {
        ParameterMapping clone = null;
        
        double coefficients[] = {
                this.correlation.getCoefficient(),
                1.0d,
                0.0d,
        };
        int expected_results[] = { 0, 1, -1 };
        
        for (int i = 0; i < coefficients.length; i++) {
            clone = new ParameterMapping(
                    this.correlation.getStatement(),
                    this.correlation.getStatementIndex(),
                    this.correlation.getStmtParameter(),
                    this.correlation.getProcParameter(),
                    this.correlation.getProcParameterIndex(),
                    coefficients[i]);
            assertEquals("i=" + i, expected_results[i], this.correlation.compareTo(clone));
        } // FOR
        
        clone = new ParameterMapping(
                catalog_other_stmt,
                0,
                catalog_other_stmt.getParameters().get(0),
                catalog_other_proc.getParameters().get(0),
                0,
                0.99999d
        );
        assert(this.correlation.compareTo(clone) > 0);
    }
    
    /**
     * testEquals
     */
    public void testEquals() {
        ParameterMapping clone = null;

        clone = new ParameterMapping(
                this.correlation.getStatement(),
                this.correlation.getStatementIndex(),
                this.correlation.getStmtParameter(),
                this.correlation.getProcParameter(),
                this.correlation.getProcParameterIndex(),
                0.99999d);
        assert(this.correlation.equals(clone));
        
        clone = new ParameterMapping(
                catalog_other_stmt,
                0,
                catalog_other_stmt.getParameters().get(0),
                catalog_other_proc.getParameters().get(0),
                0,
                0.99999d);
        assert(!this.correlation.equals(clone)) : this.correlation + " != " + clone;

    }
    
    /**
     * testHashCode
     */
    public void testHashCode() {
        ParameterMapping clone = null;

        clone = new ParameterMapping(
                this.correlation.getStatement(),
                this.correlation.getStatementIndex(),
                this.correlation.getStmtParameter(),
                this.correlation.getProcParameter(),
                this.correlation.getProcParameterIndex(),
                0.99999d);
        assertEquals(this.correlation.hashCode(), clone.hashCode());
        
        clone = new ParameterMapping(
                catalog_other_stmt,
                0,
                catalog_other_stmt.getParameters().get(0),
                catalog_other_proc.getParameters().get(0),
                0,
                0.99999d);
        assertNotSame(this.correlation.hashCode(), clone.hashCode());
    }
    
    
    /**
     * testToJSONString
     */
    public void testToJSONString() throws Exception {
        String json = this.correlation.toJSONString();
        assertFalse(json.isEmpty());
        assert(json.contains(this.catalog_proc_param.getName()));
    }
    
    
    /**
     * testFromJSONString
     */
    public void testFromJSONString() throws Exception {
        String json = this.correlation.toJSONString();
        assertFalse(json.isEmpty());
        
        JSONObject json_obj = new JSONObject(json);
        assertNotNull(json_obj);
        
        // System.err.println(json_obj.toString(2));
        
        ParameterMapping clone = new ParameterMapping();
        clone.fromJSON(json_obj, catalog_db);
        this.examineCorrelation(clone);

        assertEquals(this.correlation.getStatement(), clone.getStatement());
        assertEquals(this.correlation.getStatementIndex(), clone.getStatementIndex());
        assertEquals(this.correlation.getStmtParameter(), clone.getStmtParameter());
        assertEquals(this.correlation.getProcParameter(), clone.getProcParameter());
        assertEquals(this.correlation.getProcParameterIndex(), clone.getProcParameterIndex());
        assertEquals(this.correlation.getCoefficient(), clone.getCoefficient());
    }
}
