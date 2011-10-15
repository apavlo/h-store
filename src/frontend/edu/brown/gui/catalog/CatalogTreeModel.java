package edu.brown.gui.catalog;

import java.util.*;

import org.apache.log4j.Logger;

import javax.swing.tree.*;

import org.voltdb.VoltType;
import org.voltdb.catalog.*;
import org.voltdb.plannodes.AbstractPlanNode;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.plannodes.PlanNodeUtil;

/**
 * 
 * @author pavlo
 *
 */
public class CatalogTreeModel extends DefaultTreeModel {
    private static final Logger LOG = Logger.getLogger(CatalogTreeModel.class.getName());
    
    private static final long serialVersionUID = 1L;
    private final Catalog catalog;

    protected DefaultMutableTreeNode procedures_node;
    protected DefaultMutableTreeNode tables_node;
    
    // ----------------------------------------------
    // SEARCH INDEXES
    // ----------------------------------------------
    protected final Map<Integer, DefaultMutableTreeNode> guid_node_xref = new HashMap<Integer, DefaultMutableTreeNode>();
    protected final Map<String, Set<DefaultMutableTreeNode>> name_node_xref = new HashMap<String, Set<DefaultMutableTreeNode>>();
    protected final Map<Integer, Set<DefaultMutableTreeNode>> plannode_node_xref = new HashMap<Integer, Set<DefaultMutableTreeNode>>();
    
    public CatalogTreeModel(Catalog catalog, String catalog_path) {
        super(new DefaultMutableTreeNode(catalog.getName() + " [" + catalog_path + "]"));
        this.catalog = catalog;
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
    
    private static class CatalogMapTreeNode extends DefaultMutableTreeNode {
        private static final long serialVersionUID = 1L;

        public CatalogMapTreeNode(String label, CatalogMap<? extends CatalogType> items) {
            super(label + " (" + items.size() + ")");
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
                tables_node = new CatalogMapTreeNode("Tables", database_cat.getTables());
                database_node.add(tables_node);
                for (Table catalog_tbl : database_cat.getTables()) {  
                    DefaultMutableTreeNode table_node = new DefaultMutableTreeNode(new WrapperNode(catalog_tbl));
                    tables_node.add(table_node);
                    buildSearchIndex(catalog_tbl, table_node);
                    
                    // Columns
                    DefaultMutableTreeNode columns_node = new CatalogMapTreeNode("Columns", catalog_tbl.getColumns());
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
                        DefaultMutableTreeNode indexes_node = new CatalogMapTreeNode("Indexes", catalog_tbl.getIndexes());
                        table_node.add(indexes_node);
                        for (Index catalog_idx : catalog_tbl.getIndexes()) {
                            DefaultMutableTreeNode index_node = new DefaultMutableTreeNode(new WrapperNode(catalog_idx));
                            indexes_node.add(index_node);
                            buildSearchIndex(catalog_idx, index_node);
                        } // FOR (indexes)
                    }
                    // Constraints
                    if (!catalog_tbl.getConstraints().isEmpty()) {
                        DefaultMutableTreeNode constraints_node = new CatalogMapTreeNode("Constraints", catalog_tbl.getConstraints());
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
                        DefaultMutableTreeNode vp_node = new CatalogMapTreeNode("Vertical Partition", catalog_vp.getGroupbycols());
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
            
                // Stored Procedures
                procedures_node = new CatalogMapTreeNode("Stored Procedures", database_cat.getProcedures());
                database_node.add(procedures_node);
                for (Procedure procedure_cat : database_cat.getProcedures()) {
                    DefaultMutableTreeNode procedure_node = new DefaultMutableTreeNode(new WrapperNode(procedure_cat));
                    procedures_node.add(procedure_node);
                    buildSearchIndex(procedure_cat, procedure_node);

                    // Parameters
                    DefaultMutableTreeNode parameters_node = new CatalogMapTreeNode("Parameters", procedure_cat.getParameters());
                    procedure_node.add(parameters_node);
                    for (ProcParameter param_cat : CatalogUtil.getSortedCatalogItems(procedure_cat.getParameters(), "index")) {
                        DefaultMutableTreeNode param_node = new DefaultMutableTreeNode(new WrapperNode(param_cat) {
                            @Override
                            public String toString() {
                                ProcParameter param_cat = (ProcParameter)this.getCatalogType();
                                VoltType type = VoltType.get((byte)param_cat.getType());
                                return (super.toString() + " :: " + type);
                            }
                        });
                        parameters_node.add(param_node);
                        buildSearchIndex(param_cat, param_node);
                    } // FOR (parameters)
                    
                    // Statements
                    DefaultMutableTreeNode statementRootNode = new CatalogMapTreeNode("Statements", procedure_cat.getStatements());
                    procedure_node.add(statementRootNode);                  
                    for (Statement statement_cat : procedure_cat.getStatements()) {
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
                            DefaultMutableTreeNode planNode = new DefaultMutableTreeNode(new PlanTreeCatalogNode(label, statement_cat, node));
                            statement_node.add(planNode);
                            
                            // Plan Fragments
                            CatalogMap<PlanFragment> fragments = (is_singlepartition ? statement_cat.getFragments() : statement_cat.getMs_fragments());
                            for (PlanFragment fragment_cat : fragments) {
                                DefaultMutableTreeNode fragment_node = new DefaultMutableTreeNode(new WrapperNode(fragment_cat));
                                planNode.add(fragment_node);
                                buildSearchIndex(fragment_cat, fragment_node);
                            } // FOR (fragments)
                        }
                        
                        // Statement Parameter
                        DefaultMutableTreeNode paramRootNode = new CatalogMapTreeNode("Parameters", statement_cat.getParameters());
                        statement_node.add(paramRootNode);
                        for (StmtParameter param_cat : statement_cat.getParameters()) {
                            DefaultMutableTreeNode param_node = new DefaultMutableTreeNode(new WrapperNode(param_cat) {
                                @Override
                                public String toString() {
                                    // StmtParameter param_cat = (StmtParameter)this.getCatalogType();
                                    //HZType type = HZType.get((byte)param_cat.getJatype());
                                    return (super.toString()); // + " :: " + type);
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
                } // FOR (procedures)
            } // FOR (databses)
            
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
            DefaultMutableTreeNode hosts_node = new CatalogMapTreeNode("Hosts", cluster_cat.getHosts());
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
}
