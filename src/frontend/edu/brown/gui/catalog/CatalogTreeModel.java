package edu.brown.gui.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import org.apache.log4j.Logger;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ConflictPair;
import org.voltdb.catalog.ConflictSet;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.ConflictType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

/**
 * 
 * @author pavlo
 *
 */
public class CatalogTreeModel extends DefaultTreeModel {
    private static final Logger LOG = Logger.getLogger(CatalogTreeModel.class);
    
    private static final long serialVersionUID = 1L;
    private final Catalog catalog;

    protected DefaultMutableTreeNode procedures_node;
    protected DefaultMutableTreeNode tables_node;
    protected ProcedureConflictGraphNode conflictgraph_node;
    
    protected final Set<Procedure> conflictGraphExcludes = new HashSet<Procedure>(); 
    
    // ----------------------------------------------
    // SEARCH INDEXES
    // ----------------------------------------------
    protected final Map<Integer, DefaultMutableTreeNode> guid_node_xref = new HashMap<Integer, DefaultMutableTreeNode>();
    protected final Map<String, Set<DefaultMutableTreeNode>> name_node_xref = new HashMap<String, Set<DefaultMutableTreeNode>>();
    protected final Map<Integer, Set<DefaultMutableTreeNode>> plannode_node_xref = new HashMap<Integer, Set<DefaultMutableTreeNode>>();
    
    public CatalogTreeModel(ArgumentsParser args, Catalog catalog, String catalog_path) {
        super(new DefaultMutableTreeNode(catalog.getName() + " [" + catalog_path + "]"));
        this.catalog = catalog;
        
        // Procedures to exclude in Conflict Graph
        if (args.hasParam(ArgumentsParser.PARAM_CONFLICTS_EXCLUDE_PROCEDURES)) {
            String param = args.getParam(ArgumentsParser.PARAM_CONFLICTS_EXCLUDE_PROCEDURES);
            Database catalog_db = CatalogUtil.getDatabase(this.catalog);
            for (String procName : param.split(",")) {
                Procedure catalog_proc = catalog_db.getProcedures().getIgnoreCase(procName);
                if (catalog_proc != null) {
                    this.conflictGraphExcludes.add(catalog_proc);
                } else {
                    LOG.warn("Invalid procedure name to exclude '" + procName + "'");
                }
            } // FOR
            LOG.debug("Excluded ConflictGraph Procedures: " + this.conflictGraphExcludes);
        }
        
        this.buildModel();
    }
    
    public Map<Integer, DefaultMutableTreeNode> getGuidNodeXref() {
        return this.guid_node_xref;
    }
    public Map<Integer, Set<DefaultMutableTreeNode>> getPlanNodeGuidNodeXref() {
        return this.plannode_node_xref;
    }
    public Map<String, Set<DefaultMutableTreeNode>> getNameNodeXref() {
        return this.name_node_xref;
    }
    
    
    public ProcedureConflictGraphNode getProcedureConflictGraphNode() {
        return (this.conflictgraph_node);
    }
    
    /**
     * @return the procedures_node
     */
    public DefaultMutableTreeNode getProceduresNode() {
        return procedures_node;
    }

    /**
     * @return the tables_node
     */
    public DefaultMutableTreeNode getTablesNode() {
        return tables_node;
    }

    /**
     * Adds new information to the search index
     * @param catalog_obj
     * @param node
     */
    protected void buildSearchIndex(CatalogType catalog_obj, DefaultMutableTreeNode node) {
        //this.guid_node_xref.put(catalog_obj.getGuid(), node);
        
        List<String> keys = new ArrayList<String>();
        keys.add(catalog_obj.getName());
        
        // Add the SQL statements
        if (catalog_obj instanceof Statement) {
            Statement catalog_stmt = (Statement)catalog_obj;
            keys.add(catalog_stmt.getSqltext());
        }
        
        for (String k : keys) {
            k = k.toLowerCase();
            if (!this.name_node_xref.containsKey(k)) {
                this.name_node_xref.put(k, new HashSet<DefaultMutableTreeNode>());
            }
            this.name_node_xref.get(k).add(node);
        } // FOR
        
        if (catalog_obj instanceof Statement) {
            Statement catalog_stmt = (Statement)catalog_obj;
            try {
                AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
                for (Integer guid : PlanNodeUtil.getAllPlanColumnGuids(root)) {
                    if (this.plannode_node_xref.containsKey(guid) == false) {
                        this.plannode_node_xref.put(guid, new HashSet<DefaultMutableTreeNode>());
                    }
                    this.plannode_node_xref.get(guid).add(node);
                } // FOR
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }
    
    /**
     * 
     */
    protected void buildModel() {
        Map<Table, MaterializedViewInfo> vertical_partitions = CatalogUtil.getVerticallyPartitionedTables(catalog);
        
        // Clusters
        DefaultMutableTreeNode root_node = (DefaultMutableTreeNode)this.getRoot();
        int cluster_ctr = 0;
        for (Cluster cluster_cat : catalog.getClusters()) {
            DefaultMutableTreeNode cluster_node = new DefaultMutableTreeNode(new WrapperNode(cluster_cat));
            this.insertNodeInto(cluster_node, root_node, cluster_ctr++);
            buildSearchIndex(cluster_cat, cluster_node);
            
            // Databases
            for (Database database_cat : cluster_cat.getDatabases()) {
                DefaultMutableTreeNode database_node = new DefaultMutableTreeNode(new WrapperNode(database_cat));
                cluster_node.add(database_node);
                buildSearchIndex(database_cat, database_node);
            
                // Tables
                tables_node = new CatalogMapTreeNode(Table.class, "Tables", database_cat.getTables());
                database_node.add(tables_node);

                // List data tables first, views second
                List<Table> tables = new ArrayList<Table>();
                tables.addAll(CatalogUtil.getDataTables(database_cat));
                tables.addAll(CatalogUtil.getViewTables(database_cat));
                
                for (Table catalog_tbl : tables) {  
                    WrapperNode wrapper = null;
                    if (catalog_tbl.getMaterializer() != null) {
                        wrapper = new WrapperNode(catalog_tbl, "VIEW:"+catalog_tbl.getName());
                    } else {
                        wrapper = new WrapperNode(catalog_tbl);
                    }
                    DefaultMutableTreeNode table_node = new DefaultMutableTreeNode(wrapper);
                    tables_node.add(table_node);
                    buildSearchIndex(catalog_tbl, table_node);
                    
                    // Columns
                    DefaultMutableTreeNode columns_node = new CatalogMapTreeNode(Column.class, "Columns", catalog_tbl.getColumns());
                    table_node.add(columns_node);
                    for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
                        DefaultMutableTreeNode column_node = new DefaultMutableTreeNode(new WrapperNode(catalog_col) {
                            @Override
                            public String toString() {
                                Column column_cat = (Column)this.getCatalogType();
                                String type = VoltType.get((byte)column_cat.getType()).toSQLString();
                                return (super.toString() + " (" + type + ")");
                            }
                        });
                        columns_node.add(column_node);
                        buildSearchIndex(catalog_col, column_node);
                    } // FOR (columns)
                    // Indexes
                    if (!catalog_tbl.getIndexes().isEmpty()) {
                        DefaultMutableTreeNode indexes_node = new CatalogMapTreeNode(Index.class, "Indexes", catalog_tbl.getIndexes());
                        table_node.add(indexes_node);
                        for (Index catalog_idx : catalog_tbl.getIndexes()) {
                            DefaultMutableTreeNode index_node = new DefaultMutableTreeNode(new WrapperNode(catalog_idx));
                            indexes_node.add(index_node);
                            buildSearchIndex(catalog_idx, index_node);
                        } // FOR (indexes)
                    }
                    // Constraints
                    if (!catalog_tbl.getConstraints().isEmpty()) {
                        DefaultMutableTreeNode constraints_node = new CatalogMapTreeNode(Constraint.class, "Constraints", catalog_tbl.getConstraints());
                        table_node.add(constraints_node);
                        for (Constraint catalog_cnst : catalog_tbl.getConstraints()) {  
                            DefaultMutableTreeNode constraint_node = new DefaultMutableTreeNode(new WrapperNode(catalog_cnst));
                            constraints_node.add(constraint_node);
                            buildSearchIndex(catalog_cnst, constraint_node);
                        } // FOR (constraints)
                    }
                    // Vertical Partitions
                    final MaterializedViewInfo catalog_vp = vertical_partitions.get(catalog_tbl); 
                    if (catalog_vp != null) {
                        DefaultMutableTreeNode vp_node = new CatalogMapTreeNode(MaterializedViewInfo.class, "Vertical Partition", catalog_vp.getGroupbycols());
                        table_node.add(vp_node);
                        for (Column catalog_col : CatalogUtil.getSortedCatalogItems(CatalogUtil.getColumns(catalog_vp.getGroupbycols()), "index")) {
                            DefaultMutableTreeNode column_node = new DefaultMutableTreeNode(new WrapperNode(catalog_col) {
                                @Override
                                public String toString() {
                                    Column column_cat = (Column)this.getCatalogType();
                                    String type = VoltType.get((byte)column_cat.getType()).toSQLString();
                                    return (String.format("%s.%s (%s)", catalog_vp.getName(), super.toString(), type));
                                }
                            });
                            vp_node.add(column_node);
//                            buildSearchIndex(catalog_col, column_node);
                        } // FOR
                    }
                } // FOR (tables)
            
                // System Stored Procedures
                Collection<Procedure> sysProcs = CatalogUtil.getSysProcedures(database_cat);
                procedures_node = new CatalogMapTreeNode(Procedure.class, "System Procedures", sysProcs.size());
                database_node.add(procedures_node);
                this.buildProceduresTree(procedures_node, sysProcs);
                
                // Benchmark Stored Procedures
                List<Procedure> procs = new ArrayList<Procedure>(database_cat.getProcedures());
                procs.removeAll(sysProcs);
                procedures_node = new CatalogMapTreeNode(Procedure.class, "Stored Procedures", procs.size());
                database_node.add(procedures_node);
                
                // Conflicts Graph
                // Remove anything that should be excluded
                Set<Procedure> conflictProcs = new HashSet<Procedure>(procs);
                conflictProcs.removeAll(this.conflictGraphExcludes);
                this.conflictgraph_node = new ProcedureConflictGraphNode(conflictProcs);
                DefaultMutableTreeNode conflictNode = new DefaultMutableTreeNode(this.conflictgraph_node);
                this.procedures_node.add(conflictNode);
                
                this.buildProceduresTree(this.procedures_node, procs);
                
            } // FOR (databases)
            
            // Construct a xref mapping between host->sites and site->partitions
            Map<Host, Set<Site>> host_site_xref = new HashMap<Host, Set<Site>>();
            Map<Site, Collection<Partition>> site_partition_xref = new HashMap<Site, Collection<Partition>>();
            for (Site site_cat : cluster_cat.getSites()) {
                Host host_cat = site_cat.getHost();
                if (!host_site_xref.containsKey(host_cat)) {
                    host_site_xref.put(host_cat, new HashSet<Site>());
                }
                host_site_xref.get(host_cat).add(site_cat);
                
                Collection<Partition> partitions = CollectionUtil.addAll(new HashSet<Partition>(), site_cat.getPartitions());
                site_partition_xref.put(site_cat, partitions);
            } // FOR
            
            // Hosts
            DefaultMutableTreeNode hosts_node = new CatalogMapTreeNode(Host.class, "Hosts", cluster_cat.getHosts());
            cluster_node.add(hosts_node);
            for (Host host_cat : cluster_cat.getHosts()) {
                DefaultMutableTreeNode host_node = new DefaultMutableTreeNode(new WrapperNode(host_cat, host_cat.getIpaddr()));
                hosts_node.add(host_node);
                buildSearchIndex(host_cat, host_node);
                
                // Sites
                if (host_site_xref.containsKey(host_cat)) {
                    for (Site site_cat : host_site_xref.get(host_cat)) {
                        DefaultMutableTreeNode site_node = new DefaultMutableTreeNode(new WrapperNode(site_cat, true));
                        host_node.add(site_node);
                        buildSearchIndex(site_cat, site_node);
                        
                        // Partitions
                        if (site_partition_xref.containsKey(site_cat)) {
                            for (Partition part_cat : site_partition_xref.get(site_cat)) {
                                DefaultMutableTreeNode part_node = new DefaultMutableTreeNode(new WrapperNode(part_cat, true));
                                site_node.add(part_node);
                                buildSearchIndex(part_cat, part_node);
                            } // FOR
                        }
                    } // FOR
                }
            } // FOR
        } // FOR (clusters)
    }
    
    private void buildProceduresTree(DefaultMutableTreeNode parentNode, Collection<Procedure> procedures) {
        for (Procedure catalog_proc : procedures) {
            DefaultMutableTreeNode procNode = new DefaultMutableTreeNode(new WrapperNode(catalog_proc));
            parentNode.add(procNode);
            buildSearchIndex(catalog_proc, procNode);

            // Parameters
            DefaultMutableTreeNode parameters_node = new CatalogMapTreeNode(ProcParameter.class, "Parameters", catalog_proc.getParameters());
            procNode.add(parameters_node);
            for (ProcParameter param_cat : CatalogUtil.getSortedCatalogItems(catalog_proc.getParameters(), "index")) {
                DefaultMutableTreeNode param_node = new DefaultMutableTreeNode(new WrapperNode(param_cat) {
                    @Override
                    public String toString() {
                        ProcParameter param_cat = (ProcParameter)this.getCatalogType();
                        VoltType type = VoltType.get((byte)param_cat.getType());
                        return (super.toString() + " :: " + type.name());
                    }
                });
                parameters_node.add(param_node);
                buildSearchIndex(param_cat, param_node);
            } // FOR (parameters)
            
            // Statements
            if (catalog_proc.getSystemproc() == false) {
                DefaultMutableTreeNode statementRootNode = new CatalogMapTreeNode(Statement.class, "Statements", catalog_proc.getStatements());
                procNode.add(statementRootNode);                  
                for (Statement statement_cat : catalog_proc.getStatements()) {
                    DefaultMutableTreeNode statement_node = new DefaultMutableTreeNode(new WrapperNode(statement_cat));
                    statementRootNode.add(statement_node);
                    buildSearchIndex(statement_cat, statement_node);
                    
                    // Plan Trees
                    for (boolean is_singlepartition : new boolean[] { true, false }) {
                        if (is_singlepartition && !statement_cat.getHas_singlesited()) continue;
                        if (!is_singlepartition && !statement_cat.getHas_multisited()) continue;

                        String label = (is_singlepartition ? "Single-Partition" : "Distributed") + " Plan Fragments";
//                            String attributes = "";
                        AbstractPlanNode node = null;
                        
                        try {
                            node = PlanNodeUtil.getRootPlanNodeForStatement(statement_cat, is_singlepartition);
                        } catch (Exception e) {
                            String msg = e.getMessage();
                            if (msg == null || msg.length() == 0) {
                                e.printStackTrace();
                            } else {
                                LOG.warn(msg);
                            }
                        }
                        
                        CatalogMap<PlanFragment> fragments = (is_singlepartition ? statement_cat.getFragments() : statement_cat.getMs_fragments());
                        PlanTreeCatalogNode planTreeNode = new PlanTreeCatalogNode(label, fragments, node);
                        DefaultMutableTreeNode planNode = new DefaultMutableTreeNode(planTreeNode);
                        statement_node.add(planNode);
                        
                        // Plan Fragments
                        for (PlanFragment fragment_cat : CatalogUtil.getSortedCatalogItems(fragments, "id")) {
                            DefaultMutableTreeNode fragment_node = new DefaultMutableTreeNode(new WrapperNode(fragment_cat));
                            planNode.add(fragment_node);
                            buildSearchIndex(fragment_cat, fragment_node);
                        } // FOR (fragments)
                    }
                
                    // Statement Parameter
                    DefaultMutableTreeNode paramRootNode = new CatalogMapTreeNode(StmtParameter.class, "Parameters", statement_cat.getParameters());
                    statement_node.add(paramRootNode);
                    for (StmtParameter param_cat : CatalogUtil.getSortedCatalogItems(statement_cat.getParameters(), "index")) {
                        DefaultMutableTreeNode param_node = new DefaultMutableTreeNode(new WrapperNode(param_cat) {
                            @Override
                            public String toString() {
                                 StmtParameter param_cat = (StmtParameter)this.getCatalogType();
                                 VoltType type = VoltType.get((byte)param_cat.getJavatype());
                                 return (super.toString() + " :: " + type.name());
                            }
                        });
                        paramRootNode.add(param_node);
                        buildSearchIndex(param_cat, param_node);
                    } // FOR (parameters)
                    // Output Columns
                    DefaultMutableTreeNode columnRootNode = new DefaultMutableTreeNode("Output Columns");
                    statement_node.add(columnRootNode);
                    for (Column column_cat : statement_cat.getOutput_columns()) {
                        DefaultMutableTreeNode column_node = new DefaultMutableTreeNode(new WrapperNode(column_cat) {
                            @Override
                            public String toString() {
                                Column column_cat = (Column)this.getCatalogType();
                                String type = VoltType.get((byte)column_cat.getType()).toSQLString();
                                return (super.toString() + " (" + type + ")");
                            }
                        });
                        columnRootNode.add(column_node);
                        buildSearchIndex(column_cat, column_node);
                    } // FOR (output columns)
                } // FOR (statements)
            
                // Conflicts
                if (catalog_proc.getConflicts().isEmpty() == false) {
                    DefaultMutableTreeNode conflictRootNode = new CatalogMapTreeNode(ConflictSet.class, "Conflicts", catalog_proc.getConflicts());
                    procNode.add(conflictRootNode);
                    Database catalog_db = CatalogUtil.getDatabase(catalog_proc);
                    
                    for (ConflictSet conflicts : catalog_proc.getConflicts()) {
                        final Procedure other = catalog_db.getProcedures().getIgnoreCase(conflicts.getName());
                        assert(other != null) : "Invalid conflict procedure name '" + conflicts.getName() + "'";
                        String attrText = "";
                        
                        // READ-WRITE CONFLICTS
                        attrText += this.formatConflictSet(conflicts.getReadwriteconflicts().values(), ConflictType.READ_WRITE);
                        
                        // WRITE-WRITE CONFLICTS
                        attrText += this.formatConflictSet(conflicts.getWritewriteconflicts().values(), ConflictType.WRITE_WRITE);

                        AttributesNode conflict_node = new AttributesNode(other.getName(), attrText);
                        conflictRootNode.add(new DefaultMutableTreeNode(conflict_node));
                    } // FOR
                } // SYSPROC
            }
        } // FOR (procedures)
    }
    
    private String formatConflictSet(ConflictPair conflicts[], ConflictType conflictType) {
        StringBuilder sb = new StringBuilder();
        sb.append(StringUtil.header(conflictType.name().toUpperCase() + " CONFLICTS")).append("\n");
        if (conflicts.length == 0) {
            sb.append("<NONE>\n\n");
        } else {
            int ctr = 0;
            for (ConflictPair cp : conflicts) {
                sb.append(String.format("[%02d] %s -> %s\n", ctr++, cp.getStatement0().fullName(), cp.getStatement1().fullName()));
                sb.append(String.format("     Always Conflict: %s\n", cp.getAlwaysconflicting()));
                sb.append(String.format("     Tables: %s\n", CatalogUtil.getDisplayNames(CatalogUtil.getTablesFromRefs(cp.getTables()))));
                sb.append("\n");
            } // FOR
        }
        return (sb.toString());
    }
}
