package edu.brown.mappings;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * @author pavlo
 */
public class ParametersUtil {
    /** java.util.logging logger. */
    private static final Logger LOG = Logger.getLogger(ParametersUtil.class.getName());

    /**
     *
     */
    public static class DefaultParameterMapping extends HashMap<String, Map<Integer, Integer>> {
        private static final long serialVersionUID = 1L;
        private final Map<String, Map<Integer, String>> stmt_param_names = new HashMap<String, Map<Integer, String>>();
        private final Map<String, Map<String, Integer>> stmt_param_idxs = new HashMap<String, Map<String, Integer>>();
        private final Map<Integer, String> proc_param_names = new HashMap<Integer, String>();
        private final Map<String, Integer> proc_param_idxs = new HashMap<String, Integer>();

        public DefaultParameterMapping() {
            super();
        }

        public Set<String> getStmtParamNames(String stmt_name) {
            return (this.stmt_param_idxs.get(stmt_name).keySet());
        }

        public String getStmtParamName(String stmt_name, int idx) {
            if (this.stmt_param_names.containsKey(stmt_name)) {
                return (this.stmt_param_names.get(stmt_name).get(idx));
            }
            return (null);
        }

        public Integer getStmtParamIndex(String stmt_name, String param_name) {
            if (this.stmt_param_idxs.containsKey(stmt_name)) {
                return (this.stmt_param_idxs.get(stmt_name).get(param_name));
            }
            return (null);
        }

        public Set<String> getProcParamNames() {
            return (this.proc_param_idxs.keySet());
        }

        public String getProcParamName(int idx) {
            return (this.proc_param_names.get(idx));
        }

        public Integer getProcParamIndex(String param_name) {
            return (this.proc_param_idxs.get(param_name));
        }

        /**
         * Adds a new mapping entry for a particular query in the catalogs. The
         * stmt_param is the index of the StmtParameter in the catalogs that
         * gets mapped to an index location of a ProcParamter
         * 
         * @param stmt_name
         *            - the name of the Statement for this parameter mapping
         * @param stmt_param
         *            - the index of the StmtParameter for this query
         * @param proc_param
         *            - the index of the ProcParameter for this StmtParameter
         *            corresponds to
         */
        public void add(String stmt_name, Integer stmt_param, String stmt_param_name, Integer proc_param, String proc_param_name) {
            if (!this.containsKey(stmt_name)) {
                this.put(stmt_name, new HashMap<Integer, Integer>());
                this.stmt_param_names.put(stmt_name, new HashMap<Integer, String>());
                this.stmt_param_idxs.put(stmt_name, new HashMap<String, Integer>());
            }
            this.get(stmt_name).put(stmt_param, proc_param);
            this.stmt_param_names.get(stmt_name).put(stmt_param, stmt_param_name);
            this.stmt_param_idxs.get(stmt_name).put(stmt_param_name, stmt_param);
            this.proc_param_names.put(proc_param, proc_param_name);
            this.proc_param_idxs.put(proc_param_name, proc_param);
        }
        
        /**
         * Adds a new mapping entry for a particular query in the catalogs.
         * @param proc_param
         * @param stmt_name
         * @param stmt_param
         */
        public void add(int proc_param, String stmt_name, int stmt_param) {
            if (!this.containsKey(stmt_name)) {
                this.put(stmt_name, new HashMap<Integer, Integer>());
                this.stmt_param_names.put(stmt_name, new HashMap<Integer, String>());
                this.stmt_param_idxs.put(stmt_name, new HashMap<String, Integer>());
            }
            this.get(stmt_name).put(stmt_param, proc_param);
        }
    } // END CLASS

    /**
     * Instead of doing all this work every time the class is loaded into the
     * namespace, we'll construct a static object that is initialized only when
     * the static methods are called. There you go old boy, I'm so clever!
     */
    private static ParametersUtil cache;

    private static ParametersUtil getCachedObject() {
        if (ParametersUtil.cache == null)
            ParametersUtil.cache = new ParametersUtil();
        return (ParametersUtil.cache);
    }

    /**
     * Return a map from procedure names to ParameterMappings
     * 
     * @param project_type
     * @return
     */
    public static Map<String, ParametersUtil.DefaultParameterMapping> getParameterMapping(ProjectType project_type) {
        return (ParametersUtil.getCachedObject().PARAMETER_MAPS.get(project_type));
    }

    /**
     * Return the estimated Statement parameter name for a given
     * Procedure+Statement
     * 
     * @param catalog_type
     * @param proc_name
     * @param stmt_name
     * @param idx
     * @return
     */
    public static String getStmtParameterName(ProjectType project_type, String proc_name, String stmt_name, int idx) {
        ParametersUtil putil = ParametersUtil.getCachedObject();
        Map<String, DefaultParameterMapping> map = putil.PARAMETER_MAPS.get(project_type);
        assert (map != null) : "Invalid catalog type '" + project_type + "'";
        if (map.containsKey(proc_name)) {
            DefaultParameterMapping param_map = map.get(proc_name);
            return (param_map.getStmtParamName(stmt_name, idx));
        }
        return (null);
    }

    /**
     * Return the estimated Statement parameter index for a given
     * Procedure+Statement
     * 
     * @param catalog_type
     * @param proc_name
     * @param stmt_name
     * @param param_name
     * @return
     */
    public static Integer getStmtParameterIndex(ProjectType project_type, String proc_name, String stmt_name, String param_name) {
        ParametersUtil putil = ParametersUtil.getCachedObject();
        Map<String, DefaultParameterMapping> map = putil.PARAMETER_MAPS.get(project_type);
        assert (map != null) : "Invalid catalog type '" + project_type + "'";
        if (map.containsKey(proc_name)) {
            DefaultParameterMapping param_map = map.get(proc_name);
            return (param_map.getStmtParamIndex(stmt_name, param_name));
        }
        return (null);
    }

    /**
     * Return the list of parameter names for this procedure
     * 
     * @param project_type
     * @param proc_name
     * @return
     */
    public static Set<String> getProcParameterNames(ProjectType project_type, String proc_name) {
        ParametersUtil putil = ParametersUtil.getCachedObject();
        Map<String, DefaultParameterMapping> map = putil.PARAMETER_MAPS.get(project_type);
        assert (map != null) : "Invalid catalog type '" + project_type + "'";
        return (map.get(proc_name).getProcParamNames());
    }

    /**
     * Return the estimated Procedure parameter name
     * 
     * @param catalog_type
     * @param proc_name
     * @param idx
     * @return
     */
    public static String getProcParameterName(ProjectType project_type, String proc_name, int idx) {
        ParametersUtil putil = ParametersUtil.getCachedObject();
        Map<String, DefaultParameterMapping> map = putil.PARAMETER_MAPS.get(project_type);
        assert (map != null) : "Invalid catalog type '" + project_type + "'";
        if (map.containsKey(proc_name)) {
            DefaultParameterMapping param_map = map.get(proc_name);
            return (param_map.getProcParamName(idx));
        }
        return (null);
    }

    /**
     * Return the estimated Procedure parameter index
     * 
     * @param catalog_type
     * @param proc_name
     * @param param_name
     * @return
     */
    public static Integer getProcParameterIndex(ProjectType project_type, String proc_name, String param_name) {
        ParametersUtil putil = ParametersUtil.getCachedObject();
        Map<String, DefaultParameterMapping> map = putil.PARAMETER_MAPS.get(project_type);
        assert (map != null) : "Invalid catalog type '" + project_type + "'";
        if (map.containsKey(proc_name)) {
            DefaultParameterMapping param_map = map.get(proc_name);
            return (param_map.getProcParamIndex(param_name));
        }
        return (null);
    }

    /**
     * Convert a ParameterCorrelations object into a map
     * String->ParameterMapping
     * 
     * @param catalog_db
     * @param mappings
     * @return
     * @throws Exception
     */
    public static Map<String, DefaultParameterMapping> generateXrefFromParameterMappings(Database catalog_db, ParameterMappingsSet mappings) {
        assert (catalog_db != null);
        assert (mappings != null);
        Map<String, DefaultParameterMapping> xrefMap = new HashMap<String, DefaultParameterMapping>();

        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            String proc_name = catalog_proc.getName();
            DefaultParameterMapping map = new DefaultParameterMapping();

            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                String stmt_name = catalog_stmt.getName();

                for (StmtParameter catalog_stmt_param : catalog_stmt.getParameters()) {
                    String stmt_param_name = catalog_stmt_param.getName();
                    Set<ParameterMapping> m = mappings.get(catalog_stmt, catalog_stmt_param);

                    if (m.isEmpty()) {
                        LOG.debug("No ParameterMapping found for " + CatalogUtil.getDisplayName(catalog_stmt_param) + ". Skipping...");
                        continue;
                    }
                    // HACK: I'm lazy, just take the first one for now
                    ParameterMapping c = CollectionUtil.first(m);
                    assert (c != null);

                    Integer proc_param = c.getProcParameter().getIndex();
                    String proc_param_name = c.getProcParameter().getName();
                    map.add(stmt_name, catalog_stmt_param.getIndex(), stmt_param_name, proc_param, proc_param_name);
                } // FOR (StmtParameter)
            } // FOR (Statement)
            xrefMap.put(proc_name, map);
        } // FOR (Procedure)
        assert (xrefMap.size() == catalog_db.getProcedures().size());
        return (xrefMap);
    }

    public static void applyParameterMappings(Database catalog_db, ParameterMappingsSet mappings) {
        Map<String, DefaultParameterMapping> proc_mapping = ParametersUtil.generateXrefFromParameterMappings(catalog_db, mappings);
        ParametersUtil.populateCatalog(catalog_db, proc_mapping, true);
        return;
    }

    /**
     * @param catalog_db
     * @param proc_mapping
     * @throws Exception
     */
    public static void populateCatalog(Database catalog_db, Map<String, DefaultParameterMapping> proc_mapping) throws Exception {
        populateCatalog(catalog_db, proc_mapping, false);
    }

    public static void populateCatalog(Database catalog_db, Map<String, DefaultParameterMapping> proc_mapping, boolean force) {
        // For each Procedure in the catalog, we need to find the matching record
        // in the mapping and update the catalog elements as necessary
        for (String proc_name : proc_mapping.keySet()) {
            Procedure catalog_proc = catalog_db.getProcedures().getIgnoreCase(proc_name);
            if (catalog_proc == null) {
                // throw new RuntimeException("Unknown Procedure name '" + proc_name + "' in ParameterMapping");
                continue;
            }
            LOG.debug("Updating parameter mapping for Procedure '" + proc_name + "'");
            
            DefaultParameterMapping map = proc_mapping.get(proc_name);
            for (String stmt_name : map.keySet()) {
                Statement catalog_stmt = catalog_proc.getStatements().get(stmt_name);
                if (catalog_stmt == null) {
                    throw new RuntimeException("Unknown Statement name '" + stmt_name + "' in ParameterMapping");
                }

                for (Integer stmt_param : map.get(stmt_name).keySet()) {
                    StmtParameter catalog_stmt_param = catalog_stmt.getParameters().get(stmt_param);

                    Integer proc_param = map.get(stmt_name).get(stmt_param);
                    ProcParameter catalog_proc_param = catalog_proc.getParameters().get(proc_param);

                    // Skip if it already has the proper ProcParameter set
                    if (!force && catalog_stmt_param.getProcparameter() != null && catalog_stmt_param.getProcparameter().equals(catalog_proc_param)) {
                        LOG.debug("Skipping ProcParameter mapping in " + catalog_stmt + " because it is already set");
                    } else {
                        catalog_stmt_param.setProcparameter(catalog_proc_param);
                        LOG.debug("Added parameter mapping in Statement '" + stmt_name + "' from StmtParameter '" + catalog_stmt_param.getName() + "' to '" + catalog_proc_param.getName() + "'");
                    }
                } // FOR
            } // FOR
        } // FOR
        return;
    }

    // -------------------------------------------------------
    // TPC-C Parameter Mapping
    // -------------------------------------------------------
    public final Map<String, DefaultParameterMapping> TPCC_PARAMS = new HashMap<String, DefaultParameterMapping>();
    {
        String proc_name;
        String stmt_name;
        DefaultParameterMapping map;

        //
        // NEWORDER:
        // w_id, [0]
        // d_id, [1]
        // c_id, [2]
        // timestamp, [3]
        // item_id[], [4]
        // supware[], [5]
        // quantity[] [6]
        //
        proc_name = "neworder";
        map = new DefaultParameterMapping();

        stmt_name = "getWarehouseTaxRate";
        map.add(stmt_name, 0, "w_id", 0, "w_id");

        stmt_name = "getDistrict";
        map.add(stmt_name, 0, "d_id", 1, "d_id");
        map.add(stmt_name, 1, "w_id", 0, "w_id");

        stmt_name = "incrementNextOrderId";
        map.add(stmt_name, 1, "d_id", 1, "d_id");
        map.add(stmt_name, 2, "w_id", 0, "w_id");

        stmt_name = "getCustomer";
        map.add(stmt_name, 0, "w_id", 0, "w_id");
        map.add(stmt_name, 1, "d_id", 1, "d_id");
        map.add(stmt_name, 2, "c_id", 2, "c_id");

        stmt_name = "createOrder";
        map.add(stmt_name, 1, "d_id", 1, "d_id");
        map.add(stmt_name, 2, "w_id", 0, "w_id");
        map.add(stmt_name, 3, "c_id", 2, "c_id");
        map.add(stmt_name, 4, "timestamp", 3, "timestamp");

        stmt_name = "createNewOrder";
        map.add(stmt_name, 1, "d_id", 1, "d_id");
        map.add(stmt_name, 2, "w_id", 0, "w_id");

        stmt_name = "getItemInfo";
        map.add(stmt_name, 0, "ol_i_id", 4, "item_id");

        for (int i = 1; i <= 10; i++) {
            stmt_name = String.format("getStockInfo%02d", i);
            map.add(stmt_name, 0, "ol_i_id", 4, "item_id");
            map.add(stmt_name, 1, "ol_supply_w_id", 5, "supware");
        } // FOR

        stmt_name = "updateStock";
        map.add(stmt_name, 4, "ol_i_id", 4, "item_id");
        map.add(stmt_name, 5, "ol_supply_w_id", 5, "supware");

        stmt_name = "createOrderLine";
        map.add(stmt_name, 1, "d_id", 1, "d_id");
        map.add(stmt_name, 2, "w_id", 0, "w_id");
        map.add(stmt_name, 4, "ol_i_id", 4, "item_id");
        map.add(stmt_name, 5, "ol_supply_w_id", 5, "supware");

        this.TPCC_PARAMS.put(proc_name, map);

        //
        // SLEV:
        // w_id, [0]
        // d_id, [1]
        // threshold [2]
        //
        proc_name = "slev";
        map = new DefaultParameterMapping();

        stmt_name = "GetOId";
        map.add(stmt_name, 0, "d_w_id", 0, "w_id");
        map.add(stmt_name, 1, "d_id", 1, "d_id");

        stmt_name = "GetStockCount";
        map.add(stmt_name, 0, "ol_w_id", 0, "w_id");
        map.add(stmt_name, 1, "ol_d_id", 1, "d_id");
        map.add(stmt_name, 4, "s_w_id", 0, "w_id");
        map.add(stmt_name, 5, "s_quantity", 2, "threshold");

        this.TPCC_PARAMS.put(proc_name, map);

        //
        // DELIVERY:
        // w_id, [0]
        // o_carrier_id, [1]
        // timestamp [2]
        //
        proc_name = "delivery";
        map = new DefaultParameterMapping();

        stmt_name = "getNewOrder";
        map.add(stmt_name, 1, "no_w_id", 0, "w_id");

        stmt_name = "deleteNewOrder";
        map.add(stmt_name, 1, "no_w_id", 0, "w_id");

        stmt_name = "getCId";
        map.add(stmt_name, 2, "o_w_id", 0, "w_id");

        stmt_name = "updateOrders";
        map.add(stmt_name, 0, "o_carrier_id", 1, "o_carrier_id");
        map.add(stmt_name, 3, "o_w_id", 0, "w_id");

        stmt_name = "updateOrderLine";
        map.add(stmt_name, 0, "ol_delivery_d", 2, "timestamp");
        map.add(stmt_name, 3, "ol_w_id", 0, "w_id");

        stmt_name = "sumOLAmount";
        map.add(stmt_name, 2, "ol_w_id", 0, "w_id");

        stmt_name = "updateCustomer";
        map.add(stmt_name, 3, "c_w_id", 0, "w_id");

        this.TPCC_PARAMS.put(proc_name, map);

        //
        // ostatByCustomerId:
        // w_id, [0]
        // d_id, [1]
        // c_id [2]
        //
        proc_name = "ostatByCustomerId";
        map = new DefaultParameterMapping();

        stmt_name = "getCustomerByCustomerId";
        map.add(stmt_name, 0, "w_id", 0, "c_w_id");
        map.add(stmt_name, 1, "d_id", 1, "c_d_id");
        map.add(stmt_name, 2, "c_id", 2, "c_id");

        stmt_name = "getLastOrder";
        map.add(stmt_name, 0, "w_id", 0, "o_w_id");
        map.add(stmt_name, 1, "d_id", 1, "o_d_id");
        map.add(stmt_name, 2, "c_id", 2, "o_c_id");

        stmt_name = "getOrderLines";
        map.add(stmt_name, 0, "w_id", 0, "ol_w_id");
        map.add(stmt_name, 1, "d_id", 1, "ol_d_id");
        map.add(stmt_name, 2, "c_id", 2, "ol_c_id");

        this.TPCC_PARAMS.put(proc_name, map);

        //
        // ostatByCustomerName
        // w_id [0]
        // d_id [1]
        // c_last [2]
        //
        proc_name = "ostatByCustomerName";
        map = new DefaultParameterMapping();

        stmt_name = "getCustomersByLastName";
        map.add(stmt_name, 0, "w_id", 0, "c_w_id");
        map.add(stmt_name, 1, "d_id", 1, "c_d_id");
        map.add(stmt_name, 2, "c_last", 2, "c_last");

        stmt_name = "getLastOrder";
        map.add(stmt_name, 0, "w_id", 0, "o_w_id");
        map.add(stmt_name, 1, "d_id", 1, "o_d_id");

        stmt_name = "getOrderLines";
        map.add(stmt_name, 0, "w_id", 0, "ol_w_id");
        map.add(stmt_name, 1, "d_id", 1, "ol_d_id");

        this.TPCC_PARAMS.put(proc_name, map);

        //
        // paymentByCustomerIdC
        // w_id [0]
        // d_id [1]
        // h_amount [2]
        // c_w_id [3]
        // c_d_id [4]
        // c_id [5]
        // timestamp [6]
        //
        proc_name = "paymentByCustomerIdC";
        map = new DefaultParameterMapping();

        stmt_name = "getCustomersByCustomerId";
        map.add(stmt_name, 0, "c_id", 5, "c_id");
        map.add(stmt_name, 1, "c_d_id", 4, "c_d_id");
        map.add(stmt_name, 2, "c_w_id", 3, "c_w_id");

        stmt_name = "updateBCCustomer";
        map.add(stmt_name, 0, "c_w_id", 3, "c_w_id");
        map.add(stmt_name, 1, "c_d_id", 4, "c_d_id");
        map.add(stmt_name, 2, "c_id", 5, "c_id");
        this.TPCC_PARAMS.put(proc_name, map);

        //
        // paymentByCustomerName
        // w_id [0]
        // d_id [1]
        // h_amount [2]
        // c_w_id [3]
        // c_d_id [4]
        // c_last [5]
        // timestamp [6]
        //
        proc_name = "paymentByCustomerName";
        map = new DefaultParameterMapping();

        stmt_name = "getWarehouse";
        map.add(stmt_name, 0, "w_id", 0, "w_id");

        stmt_name = "updateWarehouseBalance";
        map.add(stmt_name, 0, "w_ytd", 2, "h_amount");
        map.add(stmt_name, 1, "w_id", 0, "w_id");

        stmt_name = "getDistrict";
        map.add(stmt_name, 0, "d_w_id", 0, "w_id");
        map.add(stmt_name, 1, "d_id", 1, "d_id");

        stmt_name = "updateDistrictBalance";
        map.add(stmt_name, 0, "d_ytd", 2, "h_amount");
        map.add(stmt_name, 1, "d_w_id", 0, "w_id");
        map.add(stmt_name, 2, "d_id", 1, "d_id");

        stmt_name = "insertHistory";
        map.add(stmt_name, 1, "c_d_id", 4, "c_d_id");
        map.add(stmt_name, 2, "c_w_id", 3, "c_w_id");
        map.add(stmt_name, 3, "d_id", 1, "d_id");
        map.add(stmt_name, 4, "w_id", 0, "w_id");
        map.add(stmt_name, 5, "timestamp", 6, "timestamp");
        map.add(stmt_name, 6, "h_amount", 2, "h_amount");

        this.TPCC_PARAMS.put(proc_name, map);

        //
        // paymentByCustomerNameW
        // w_id [0]
        // d_id [1]
        // h_amount [2]
        // c_w_id [3]
        // c_d_id [4]
        // c_id [5]
        // timestamp [6]
        //
        // proc_name = "paymentByCustomerNameW";
        // map = new ParameterMapping();
        //
        // stmt_name = "getWarehouse";
        // map.add(stmt_name, 0, "w_id", 0, "w_id");
        //
        // stmt_name = "updateWarehouseBalance";
        // map.add(stmt_name, 0, "w_ytd", 2, "h_amount");
        // map.add(stmt_name, 1, "w_id", 0, "w_id");
        //
        // stmt_name = "getDistrict";
        // map.add(stmt_name, 0, "d_w_id", 0, "w_id");
        // map.add(stmt_name, 1, "d_id", 1, "d_id");
        //
        // stmt_name = "updateDistrictBalance";
        // map.add(stmt_name, 0, "d_ytd", 2, "h_amount");
        // map.add(stmt_name, 1, "d_w_id", 0, "w_id");
        // map.add(stmt_name, 2, "d_id", 1, "d_id");
        //
        // stmt_name = "insertHistory";
        // map.add(stmt_name, 0, "c_id", 5, "c_id");
        // map.add(stmt_name, 1, "c_d_id", 4, "c_d_id");
        // map.add(stmt_name, 2, "c_w_id", 3, "c_w_id");
        // map.add(stmt_name, 3, "d_id", 1, "d_id");
        // map.add(stmt_name, 4, "w_id", 0, "w_id");
        // map.add(stmt_name, 5, "timestamp", 6, "timestamp");
        // map.add(stmt_name, 6, "h_amount", 2, "h_amount");
        //
        // this.TPCC_PARAMS.put(proc_name, map);

        //
        // paymentByCustomerId
        // w_id [0]
        // d_id [1]
        // h_amount [2]
        // c_w_id [3]
        // c_d_id [4]
        // c_id [5]
        // timestamp [6]
        //
        proc_name = "paymentByCustomerId";
        map = new DefaultParameterMapping();

        stmt_name = "getCustomersByCustomerId";
        map.add(stmt_name, 0, "c_id", 5, "c_id");
        map.add(stmt_name, 0, "c_d_id", 4, "c_d_id");
        map.add(stmt_name, 0, "c_w_id", 3, "c_w_id");

        stmt_name = "getWarehouse";
        map.add(stmt_name, 0, "w_id", 0, "w_id");

        stmt_name = "updateWarehouseBalance";
        map.add(stmt_name, 0, "w_ytd", 2, "h_amount");
        map.add(stmt_name, 1, "w_id", 0, "w_id");

        stmt_name = "getDistrict";
        map.add(stmt_name, 0, "d_w_id", 0, "w_id");
        map.add(stmt_name, 1, "d_id", 1, "d_id");

        stmt_name = "updateDistrictBalance";
        map.add(stmt_name, 0, "d_ytd", 2, "h_amount");
        map.add(stmt_name, 1, "d_w_id", 0, "w_id");
        map.add(stmt_name, 2, "d_id", 1, "d_id");

        stmt_name = "updateBCCustomer";
        map.add(stmt_name, 4, "c_w_id", 3, "c_w_id");
        map.add(stmt_name, 5, "c_d_id", 4, "c_d_id");
        map.add(stmt_name, 6, "c_id", 5, "c_id");

        stmt_name = "updateGCCustomer";
        map.add(stmt_name, 3, "c_w_id", 3, "c_w_id");
        map.add(stmt_name, 4, "c_d_id", 4, "c_d_id");
        map.add(stmt_name, 5, "c_id", 5, "c_id");

        stmt_name = "insertHistory";
        map.add(stmt_name, 0, "c_id", 5, "c_id");
        map.add(stmt_name, 1, "c_d_id", 4, "c_d_id");
        map.add(stmt_name, 2, "c_w_id", 3, "c_w_id");
        map.add(stmt_name, 3, "d_id", 1, "d_id");
        map.add(stmt_name, 4, "w_id", 0, "w_id");
        map.add(stmt_name, 5, "timestamp", 6, "timestamp");
        map.add(stmt_name, 6, "h_amount", 2, "h_amount");

        this.TPCC_PARAMS.put(proc_name, map);

        //
        // paymentByCustomerIdW
        // w_id [0]
        // d_id [1]
        // h_amount [2]
        // c_w_id [3]
        // c_d_id [4]
        // c_id [5]
        // timestamp [6]
        //
        // proc_name = "paymentByCustomerIdW";
        // map = new ParameterMapping();
        //
        // stmt_name = "getWarehouse";
        // map.add(stmt_name, 0, "w_id", 0, "w_id");
        //
        // stmt_name = "updateWarehouseBalance";
        // map.add(stmt_name, 0, "w_ytd", 2, "h_amount");
        // map.add(stmt_name, 1, "w_id", 0, "w_id");
        //
        // stmt_name = "getDistrict";
        // map.add(stmt_name, 0, "d_w_id", 0, "w_id");
        // map.add(stmt_name, 1, "d_id", 1, "d_id");
        //
        // stmt_name = "updateDistrictBalance";
        // map.add(stmt_name, 0, "d_ytd", 2, "h_amount");
        // map.add(stmt_name, 1, "d_w_id", 0, "w_id");
        // map.add(stmt_name, 2, "d_id", 1, "d_id");
        //
        // stmt_name = "insertHistory";
        // map.add(stmt_name, 0, "c_id", 5, "c_id");
        // map.add(stmt_name, 1, "c_d_id", 4, "c_d_id");
        // map.add(stmt_name, 2, "c_w_id", 3, "c_w_id");
        // map.add(stmt_name, 3, "d_id", 1, "d_id");
        // map.add(stmt_name, 4, "w_id", 0, "w_id");
        // map.add(stmt_name, 5, "timestamp", 6, "timestamp");
        // map.add(stmt_name, 6, "h_amount", 2, "h_amount");
        //
        // this.TPCC_PARAMS.put(proc_name, map);

    } // STATIC

    // -------------------------------------------------------
    // TM1 Parameter Mapping
    // -------------------------------------------------------
    public final Map<String, DefaultParameterMapping> TM1_PARAMS = new HashMap<String, DefaultParameterMapping>();
    {
        String proc_name;
        String stmt_name;
        DefaultParameterMapping map;

        //
        // DeleteCallForwarding
        // sub_nbr [0]
        // sf_type [1]
        // start_time [2]
        //
        proc_name = "DeleteCallForwarding";
        map = new DefaultParameterMapping();

        stmt_name = "query";
        map.add(stmt_name, 0, "sub_nbr", 0, "sub_nbr");
        stmt_name = "update";
        map.add(stmt_name, 1, "sf_type", 1, "sf_type");
        map.add(stmt_name, 2, "start_time", 2, "start_time");

        this.TM1_PARAMS.put(proc_name, map);

        //
        // GetAccessData
        // s_id [0]
        // ai_type [1]
        //
        proc_name = "GetAccessData";
        map = new DefaultParameterMapping();

        stmt_name = "GetData";
        map.add(stmt_name, 0, "s_id", 0, "s_id");
        map.add(stmt_name, 1, "ai_type", 1, "ai_type");

        this.TM1_PARAMS.put(proc_name, map);

        //
        // GetNewDestination
        // s_id [0]
        // sf_type [1]
        // start_time [2]
        // end_time [3]
        //
        proc_name = "GetNewDestination";
        map = new DefaultParameterMapping();

        stmt_name = "GetData";
        map.add(stmt_name, 0, "s_id", 0, "s_id");
        map.add(stmt_name, 1, "sf_type", 1, "sf_type");
        map.add(stmt_name, 2, "start_time", 2, "start_time");
        map.add(stmt_name, 3, "end_time", 3, "end_time");

        this.TM1_PARAMS.put(proc_name, map);

        //
        // GetSubscriberData
        // s_id [0]
        //
        proc_name = "GetSubscriberData";
        map = new DefaultParameterMapping();

        stmt_name = "GetData";
        map.add(stmt_name, 0, "s_id", 0, "s_id");

        this.TM1_PARAMS.put(proc_name, map);

        //
        // InsertCallForwarding
        // sub_nbr [0]
        // sf_type [1]
        // start_time [2]
        // end_time [3]
        // numberx [4]
        //
        proc_name = "InsertCallForwarding";
        map = new DefaultParameterMapping();

        stmt_name = "query1";
        map.add(stmt_name, 0, "sub_nbr", 0, "sub_nbr");

        stmt_name = "query2";
        // Nothing

        stmt_name = "update";
        map.add(stmt_name, 1, "sf_type", 1, "sf_type");
        map.add(stmt_name, 2, "start_time", 2, "start_time");
        map.add(stmt_name, 3, "end_time", 3, "end_time");
        map.add(stmt_name, 4, "numberx", 4, "numberx");

        stmt_name = "check";
        map.add(stmt_name, 1, "sf_type", 1, "sf_type");
        map.add(stmt_name, 2, "start_time", 2, "start_time");

        this.TM1_PARAMS.put(proc_name, map);

        //
        // UpdateLocation
        // location [0]
        // sub_nbr [1]
        //
        proc_name = "UpdateLocation";
        map = new DefaultParameterMapping();

        stmt_name = "update";
        map.add(stmt_name, 0, "location", 0, "location");
        map.add(stmt_name, 1, "sub_nbr", 1, "sub_nbr");

        //
        // UpdateSubscriberData
        // bit_1 [0]
        // s_id [1]
        // data_a [2]
        // sf_type [3]
        //
        proc_name = "UpdateSubscriberData";
        map = new DefaultParameterMapping();

        stmt_name = "update1";
        map.add(stmt_name, 0, "bit_1", 0, "bit_1");
        map.add(stmt_name, 1, "s_id", 1, "s_id");

        stmt_name = "update2";
        map.add(stmt_name, 0, "data_a", 2, "data_a");
        map.add(stmt_name, 1, "s_id", 1, "s_id");
        map.add(stmt_name, 2, "sf_type", 3, "sf_type");

        this.TM1_PARAMS.put(proc_name, map);
    };

    //
    // Mappings between param names and procparam->stmtparam
    // Nasty I know, but what else can I do?
    //
    public final Map<ProjectType, Map<String, DefaultParameterMapping>> PARAMETER_MAPS = new HashMap<ProjectType, Map<String, DefaultParameterMapping>>();
    {
        PARAMETER_MAPS.put(ProjectType.TPCC, TPCC_PARAMS);
        PARAMETER_MAPS.put(ProjectType.TM1, TM1_PARAMS);
    }
}
