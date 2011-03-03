package edu.brown.correlations;

import java.io.IOException;
import java.util.*;

import org.json.*;
import org.voltdb.catalog.*;
import org.voltdb.utils.Pair;

import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public class ParameterCorrelations extends TreeSet<Correlation> implements JSONSerializable {
    private static final long serialVersionUID = 1L;
//    private static final Logger LOG = Logger.getLogger(ParameterCorrelations.class.getName());

    /**
     * Dear son,
     * This right here is nasty old boy! Don't do what Daddy did...
     **/
    private final transient HashMap<Statement, SortedMap<Integer, SortedMap<StmtParameter, SortedSet<Correlation>>>> stmt_correlations = new HashMap<Statement, SortedMap<Integer,SortedMap<StmtParameter, SortedSet<Correlation>>>>();
    private final transient HashMap<Procedure, SortedMap<ProcParameter, SortedSet<Correlation>>> proc_correlations = new HashMap<Procedure, SortedMap<ProcParameter,SortedSet<Correlation>>>();
    
    /**
     * Constructor
     * @param catalog_db
     */
    public ParameterCorrelations() {
        super();
        // Nothing for now...
    }
    
    /**
     * Removes all of the elements from this set and the internal index.
     */
    @Override
    public void clear() {
        this.stmt_correlations.clear();
        this.proc_correlations.clear();
        super.clear();
    }

    /**
     * Add a new Correlation object for the given StmtParameter
     * @param c
     */
    @Override
    public boolean add(Correlation c) {
        assert(c != null);
        Statement catalog_stmt = c.getStatement();
        assert(catalog_stmt != null);
        StmtParameter catalog_stmt_param = c.getStmtParameter();
        assert(catalog_stmt_param != null);
        Procedure catalog_proc = catalog_stmt.getParent();
        assert(catalog_proc != null);
        ProcParameter catalog_proc_param = c.getProcParameter();
        
        // Procedure Index
        if (!this.proc_correlations.containsKey(catalog_proc)) {
            this.proc_correlations.put(catalog_proc, new TreeMap<ProcParameter, SortedSet<Correlation>>());
        }
        if (!this.proc_correlations.get(catalog_proc).containsKey(catalog_proc_param)) {
            this.proc_correlations.get(catalog_proc).put(catalog_proc_param, new TreeSet<Correlation>());
        }
        this.proc_correlations.get(catalog_proc).get(catalog_proc_param).add(c);
        
        // Statement Index
        if (!this.stmt_correlations.containsKey(catalog_stmt)) {
            this.stmt_correlations.put(catalog_stmt, new TreeMap<Integer, SortedMap<StmtParameter, SortedSet<Correlation>>>());
        }
        if (!this.stmt_correlations.get(catalog_stmt).containsKey(c.getStatementIndex())) {
            this.stmt_correlations.get(catalog_stmt).put(c.getStatementIndex(), new TreeMap<StmtParameter, SortedSet<Correlation>>());
        }
        if (!this.stmt_correlations.get(catalog_stmt).get(c.getStatementIndex()).containsKey(catalog_stmt_param)) {
            this.stmt_correlations.get(catalog_stmt).get(c.getStatementIndex()).put(catalog_stmt_param, new TreeSet<Correlation>());
        }
        this.stmt_correlations.get(catalog_stmt).get(c.getStatementIndex()).get(catalog_stmt_param).add(c);
        
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
    public SortedSet<Correlation> get(Statement catalog_stmt, int catalog_stmt_index, StmtParameter catalog_stmt_param) {
        assert(catalog_stmt != null);
        assert(catalog_stmt_index >= 0);
        assert(catalog_stmt_param != null);
        
        SortedSet<Correlation> set = new TreeSet<Correlation>();
        if (this.stmt_correlations.containsKey(catalog_stmt)) {
            if (this.stmt_correlations.get(catalog_stmt).containsKey(catalog_stmt_index)) {
                set.addAll(this.stmt_correlations.get(catalog_stmt).get(catalog_stmt_index).get(catalog_stmt_param));
            }
        }
        return (set);
    }
    
    /**
     * Get all of the Correlations for this StmtParameter, regardless of the catalog_stmt_index
     * @param catalog_stmt
     * @param catalog_stmt_param
     * @return
     */
    public SortedSet<Correlation> get(Statement catalog_stmt, StmtParameter catalog_stmt_param) {
        assert(catalog_stmt != null);
        assert(catalog_stmt_param != null);
        
        SortedSet<Correlation> set = new TreeSet<Correlation>();
        if (this.stmt_correlations.containsKey(catalog_stmt)) {
            for (SortedMap<StmtParameter, SortedSet<Correlation>> m : this.stmt_correlations.get(catalog_stmt).values()) {
                if (m.containsKey(catalog_stmt_param)) {
                    set.addAll(m.get(catalog_stmt_param));
                }
            } // FOR
        }
        return (set);
    }
    
    /**
     * 
     */
    public SortedSet<Correlation> get(ProcParameter catalog_proc_param) {
        assert(catalog_proc_param != null);
        Procedure catalog_proc = catalog_proc_param.getParent();
        assert(catalog_proc != null);
        
        SortedSet<Correlation> set = new TreeSet<Correlation>();
        if (this.proc_correlations.containsKey(catalog_proc) && this.proc_correlations.get(catalog_proc).containsKey(catalog_proc_param)) {
            set.addAll(this.proc_correlations.get(catalog_proc).get(catalog_proc_param));
        }
        return (set);
    }
    
    /**
     * Return all of the Correlation objects for the given ProcParameter that are mapped to 
     * a particular Column via the StmtParameter. If you have to ask, then you probably don't need
     * this method...
     * @param catalog_proc_param
     * @param catalog_col
     * @return
     */
    public SortedSet<Correlation> get(ProcParameter catalog_proc_param, Column catalog_col) {
        assert(catalog_proc_param != null);
        assert(catalog_col != null);
        
        Pair<ProcParameter, Column> p = Pair.of(catalog_proc_param, catalog_col);
        SortedSet<Correlation> set = CACHE_ProcParameter_Column.get(p);
        if (set == null) {
            Procedure catalog_proc = catalog_proc_param.getParent();
            assert(catalog_proc != null);
            set = new TreeSet<Correlation>();
            if (this.proc_correlations.containsKey(catalog_proc) && this.proc_correlations.get(catalog_proc).containsKey(catalog_proc_param)) {
                for (Correlation c : this.proc_correlations.get(catalog_proc).get(catalog_proc_param)) {
                    if (c.getColumn().equals(catalog_col)) set.add(c);
                } // FOR
            }
            CACHE_ProcParameter_Column.put(p, set);
        }
        return (set);
    }
    private final Map<Pair<ProcParameter, Column>, SortedSet<Correlation>> CACHE_ProcParameter_Column = new HashMap<Pair<ProcParameter,Column>, SortedSet<Correlation>>();

    /**
     * Return all of the correlation objects where the StmtParameter in the Statement
     * references the given column
     * @param catalog_col
     * @return
     */
    public SortedSet<Correlation> get(Column catalog_col) {
        SortedSet<Correlation> set = new TreeSet<Correlation>();
        for (Correlation c : this) {
            if (c.getColumn().equals(catalog_col)) set.add(c);
        } // FOR
        return (set);
    }

    /**
     * 
     * @param catalog_stmt
     * @return
     */
    public SortedMap<Integer, SortedMap<StmtParameter, SortedSet<Correlation>>> get(Statement catalog_stmt) {
        return (this.stmt_correlations.get(catalog_stmt));
    }
    
    /**
     * Return a mapping from StmtParameters to Correlations
     * @param catalog_stmt
     * @param catalog_stmt_index
     * @return
     */
    public SortedMap<StmtParameter, SortedSet<Correlation>> get(Statement catalog_stmt, Integer catalog_stmt_index) {
        if (this.stmt_correlations.containsKey(catalog_stmt)) {
            return (this.stmt_correlations.get(catalog_stmt).get(catalog_stmt_index));
        }
        return (null);
    }
    
    public String debug() {
        return (this.debug(this.stmt_correlations.keySet()));
    }
    
    public String debug(Statement...catalog_stmts) {
        return (this.debug(CollectionUtil.addAll(new ArrayList<Statement>(), catalog_stmts)));
    }
 
    public String debug(Collection<Statement> catalog_stmts) {
        StringBuilder sb = new StringBuilder();
        for (Statement catalog_stmt : catalog_stmts) {
            if (this.stmt_correlations.containsKey(catalog_stmt)) {
                int num_instances = this.stmt_correlations.get(catalog_stmt).size();
                sb.append(catalog_stmt.getName() + " [# of Instances=" + num_instances + "]\n");
                for (Integer catalog_stmt_index : this.stmt_correlations.get(catalog_stmt).keySet()) {
                    if (num_instances > 1) sb.append(String.format("   Instance #%02d:\n", catalog_stmt_index));
    
                    if (this.stmt_correlations.get(catalog_stmt).containsKey(catalog_stmt_index)) {
                        SortedMap<StmtParameter, SortedSet<Correlation>> params = this.stmt_correlations.get(catalog_stmt).get(catalog_stmt_index);
                        for (StmtParameter catalog_stmt_param : params.keySet()) {
                            for (Correlation c : params.get(catalog_stmt_param)) {
                                sb.append("   " + c + "\n");
                            } // FOR (correlation)
                        } // FOR (catalog_stmt_param)
                    } else {
                        sb.append("   <NONE>\n");
                    }
                } // FOR (catalog_stmt_index)
                sb.append(CorrelationCalculator.DEFAULT_SINGLE_LINE);
            }
        } // FOR (catalot_stmt)
        return (sb.toString());
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

    @Override
    public void load(String input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }
    
    @Override
    public void save(String output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }
    
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }
    
    public void toJSON(JSONStringer stringer) throws JSONException {
        stringer.key("CORRELATIONS").array();
        for (Correlation c : this) {
            assert(c != null);
            stringer.value(c);   
        } // FOR
        stringer.endArray();
    }
    
    public void fromJSON(JSONObject object, Database catalog_db) throws JSONException {
        JSONArray json_array = object.getJSONArray("CORRELATIONS");
        for (int i = 0, cnt = json_array.length(); i < cnt; i++) {
            JSONObject json_object = json_array.getJSONObject(i);
            Correlation c = new Correlation();
            c.fromJSON(json_object, catalog_db);
            this.add(c);
        } // FOR
        assert(json_array.length() == this.size());
    }
}