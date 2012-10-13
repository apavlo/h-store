package edu.brown.mappings;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.utils.Pair;

import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public class ParameterMappingsSet extends HashSet<ParameterMapping> implements JSONSerializable {
    private static final long serialVersionUID = 1L;
//    private static final Logger LOG = Logger.getLogger(ParameterCorrelations.class.getName());

    /**
     * Dear son,
     * This right here is nasty old boy! Don't do what Daddy did...
     **/
    private final transient HashMap<Statement, StatementMappings> stmtMappings = new HashMap<Statement, StatementMappings>();
    private final transient HashMap<Procedure, ProcedureMappings> procMappings = new HashMap<Procedure, ProcedureMappings>();
    
    @SuppressWarnings("serial")
    protected static class StatementMappings extends TreeMap<Integer, SortedMap<StmtParameter, SortedSet<ParameterMapping>>> { }
    @SuppressWarnings("serial")
    protected static class ProcedureMappings extends TreeMap<ProcParameter, SortedSet<ParameterMapping>> { }
    
    /**
     * Constructor
     * @param catalog_db
     */
    public ParameterMappingsSet() {
        super();
        // Nothing for now...
    }
    
    /**
     * Removes all of the elements from this set and the internal index.
     */
    @Override
    public void clear() {
        this.stmtMappings.clear();
        this.procMappings.clear();
        super.clear();
    }

    /**
     * Add a new Correlation object for the given StmtParameter
     * @param c
     */
    @Override
    public boolean add(ParameterMapping c) {
        assert(c != null);
        Statement catalog_stmt = c.getStatement();
        assert(catalog_stmt != null);
        StmtParameter catalog_stmt_param = c.getStmtParameter();
        assert(catalog_stmt_param != null);
        Procedure catalog_proc = catalog_stmt.getParent();
        assert(catalog_proc != null);
        ProcParameter catalog_proc_param = c.getProcParameter();
        
        // Procedure Index
        if (!this.procMappings.containsKey(catalog_proc)) {
            this.procMappings.put(catalog_proc, new ProcedureMappings());
        }
        if (!this.procMappings.get(catalog_proc).containsKey(catalog_proc_param)) {
            this.procMappings.get(catalog_proc).put(catalog_proc_param, new TreeSet<ParameterMapping>());
        }
        this.procMappings.get(catalog_proc).get(catalog_proc_param).add(c);
        
        // Statement Index
        if (!this.stmtMappings.containsKey(catalog_stmt)) {
            this.stmtMappings.put(catalog_stmt, new StatementMappings());
        }
        if (!this.stmtMappings.get(catalog_stmt).containsKey(c.getStatementIndex())) {
            this.stmtMappings.get(catalog_stmt).put(c.getStatementIndex(), new TreeMap<StmtParameter, SortedSet<ParameterMapping>>());
        }
        if (!this.stmtMappings.get(catalog_stmt).get(c.getStatementIndex()).containsKey(catalog_stmt_param)) {
            this.stmtMappings.get(catalog_stmt).get(c.getStatementIndex()).put(catalog_stmt_param, new TreeSet<ParameterMapping>());
        }
        this.stmtMappings.get(catalog_stmt).get(c.getStatementIndex()).get(catalog_stmt_param).add(c);
        
        // Now add it to our internal set
        return (super.add(c));
    }

    /**
     * Return the stored Correlation for the StmtParameter in the Statement executed at the provided index 
     * @param catalog_stmt
     * @param catalog_stmt_index
     * @param catalog_stmt_param
     * @return
     */
    public Collection<ParameterMapping> get(Statement catalog_stmt, int catalog_stmt_index, StmtParameter catalog_stmt_param) {
        assert(catalog_stmt != null);
        assert(catalog_stmt_index >= 0);
        assert(catalog_stmt_param != null);
        
        Collection<ParameterMapping> ret = null;
        StatementMappings mappings = this.stmtMappings.get(catalog_stmt);
        if (mappings != null) {
            ret = mappings.get(catalog_stmt_index).get(catalog_stmt_param);
        }
        return (ret);
    }
    
    /**
     * Get all of the ParameterMapping for this StmtParameter, regardless of the catalog_stmt_index
     * @param catalog_stmt
     * @param catalog_stmt_param
     * @return
     */
    public Collection<ParameterMapping> get(Statement catalog_stmt, StmtParameter catalog_stmt_param) {
        assert(catalog_stmt != null);
        assert(catalog_stmt_param != null);
        
        Collection<ParameterMapping> set = new TreeSet<ParameterMapping>();
        StatementMappings mappings = this.stmtMappings.get(catalog_stmt);
        if (mappings != null) {
            for (SortedMap<StmtParameter, SortedSet<ParameterMapping>> m : mappings.values()) {
                if (m.containsKey(catalog_stmt_param)) {
                    set.addAll(m.get(catalog_stmt_param));
                }
            } // FOR
        }
        return (set);
    }
    
    /**
     * Return a sorted list of the ParameterMapping for this ProcParameter regardless of ProcParameter index
     * @param catalog_proc_param
     * @return
     */
    public Collection<ParameterMapping> get(ProcParameter catalog_proc_param) {
        assert(catalog_proc_param != null);
        Procedure catalog_proc = catalog_proc_param.getParent();
        assert(catalog_proc != null);
        
        Collection<ParameterMapping> ret = null;
        ProcedureMappings mappings = this.procMappings.get(catalog_proc);
        if (mappings != null && mappings.containsKey(catalog_proc_param)) {
            ret = this.procMappings.get(catalog_proc).get(catalog_proc_param);
        }
        return (ret);
    }
    
    /**
     * Return all of the ParameterMappings for the given ProcParameter that are mapped to 
     * a particular Column via the StmtParameter. If you have to ask, then you probably don't need
     * this method...
     * @param catalog_proc_param
     * @param catalog_col
     * @return
     */
    public Collection<ParameterMapping> get(ProcParameter catalog_proc_param, Column catalog_col) {
        assert(catalog_proc_param != null);
        assert(catalog_col != null);
        
        Pair<ProcParameter, Column> p = Pair.of(catalog_proc_param, catalog_col);
        SortedSet<ParameterMapping> set = CACHE_ProcParameter_Column.get(p);
        if (set == null) {
            Procedure catalog_proc = catalog_proc_param.getParent();
            assert(catalog_proc != null);
            set = new TreeSet<ParameterMapping>();
            ProcedureMappings mappings = this.procMappings.get(catalog_proc);
            if (mappings != null && mappings.containsKey(catalog_proc_param)) {
                for (ParameterMapping c : mappings.get(catalog_proc_param)) {
                    if (c.getColumn().equals(catalog_col)) set.add(c);
                } // FOR
            }
            CACHE_ProcParameter_Column.put(p, set);
        }
        return (set);
    }
    private final Map<Pair<ProcParameter, Column>, SortedSet<ParameterMapping>> CACHE_ProcParameter_Column = new HashMap<Pair<ProcParameter,Column>, SortedSet<ParameterMapping>>();

    /**
     * Return all of the correlation objects where the StmtParameter in the Statement
     * references the given column
     * @param catalog_col
     * @return
     */
    public Collection<ParameterMapping> get(Column catalog_col) {
        SortedSet<ParameterMapping> set = new TreeSet<ParameterMapping>();
        for (ParameterMapping c : this) {
            if (c.getColumn().equals(catalog_col)) set.add(c);
        } // FOR
        return (set);
    }

    /**
     * 
     * @param catalog_stmt
     * @return
     */
    protected StatementMappings get(Statement catalog_stmt) {
        return (this.stmtMappings.get(catalog_stmt));
    }
    
    /**
     * Return a mapping from StmtParameters to ParameterMapping
     * @param catalog_stmt
     * @param catalog_stmt_index
     * @return
     */
    public Map<StmtParameter, SortedSet<ParameterMapping>> get(Statement catalog_stmt, Integer catalog_stmt_index) {
        StatementMappings mappings = this.stmtMappings.get(catalog_stmt);
        if (mappings != null) {
            return (mappings.get(catalog_stmt_index));
        }
        return (null);
    }
    
    public String debug() {
        return (this.debug(this.stmtMappings.keySet()));
    }
    
    public String debug(Statement...catalog_stmts) {
        return (this.debug(CollectionUtil.addAll(new ArrayList<Statement>(), catalog_stmts)));
    }
 
    public String debug(Collection<Statement> catalog_stmts) {
        StringBuilder sb = new StringBuilder();
        for (Statement catalog_stmt : catalog_stmts) {
            if (this.stmtMappings.containsKey(catalog_stmt)) {
                int num_instances = this.stmtMappings.get(catalog_stmt).size();
                sb.append(catalog_stmt.getName() + " [# of Instances=" + num_instances + "]\n");
                for (Integer catalog_stmt_index : this.stmtMappings.get(catalog_stmt).keySet()) {
                    if (num_instances > 1) sb.append(String.format("   Instance #%02d:\n", catalog_stmt_index));
    
                    if (this.stmtMappings.get(catalog_stmt).containsKey(catalog_stmt_index)) {
                        SortedMap<StmtParameter, SortedSet<ParameterMapping>> params = this.stmtMappings.get(catalog_stmt).get(catalog_stmt_index);
                        for (StmtParameter catalog_stmt_param : params.keySet()) {
                            for (ParameterMapping c : params.get(catalog_stmt_param)) {
                                sb.append("   " + c + "\n");
                            } // FOR (correlation)
                        } // FOR (catalog_stmt_param)
                    } else {
                        sb.append("   <NONE>\n");
                    }
                } // FOR (catalog_stmt_index)
                sb.append(MappingCalculator.DEFAULT_SINGLE_LINE);
            }
        } // FOR (catalot_stmt)
        return (sb.toString());
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }
    
    @Override
    public void save(File output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }
    
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }
    
    public void toJSON(JSONStringer stringer) throws JSONException {
        stringer.key("MAPPINGS").array();
        for (ParameterMapping c : this) {
            assert(c != null);
            stringer.value(c);   
        } // FOR
        stringer.endArray();
    }
    
    public void fromJSON(JSONObject object, Database catalog_db) throws JSONException {
        JSONArray json_array = object.getJSONArray("MAPPINGS");
        for (int i = 0, cnt = json_array.length(); i < cnt; i++) {
            JSONObject json_object = json_array.getJSONObject(i);
            ParameterMapping c = new ParameterMapping();
            c.fromJSON(json_object, catalog_db);
            this.add(c);
        } // FOR
        assert(json_array.length() == this.size());
    }
}