/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.utils.JarClassLoader;

import edu.brown.catalog.CatalogUtil;

public class CatalogContext {

    /** Pass this to constructor for catalog path in tests */
    public static final String NO_PATH = "EMPTY_PATH";

    // THE CATALOG!
    public final Catalog catalog;

    // PUBLIC IMMUTABLE CACHED INFORMATION
    public final Cluster cluster;
    public final Database database;
    public final CatalogMap<Procedure> procedures;
    public final CatalogMap<Site> sites;
    public final int numberOfPartitions;
    public final int numberOfExecSites;
    public final int numberOfNodes;

    @Deprecated
    public final AuthSystem authSystem;
    
    @Deprecated
    public final SiteTracker siteTracker;

    // PRIVATE
    private final String m_path;
    private final JarClassLoader m_catalogClassLoader;
    
    private final Partition partitions[];
    private final Collection<Integer> partitionIdCollection = new ArrayList<Integer>();
    private final Integer partitionIdArray[];
    
    // ------------------------------------------------------------
    // TABLES
    // ------------------------------------------------------------
    
    private final Collection<Table> sysTables = new ArrayList<Table>();
    private final Collection<Table> dataTables = new ArrayList<Table>();
    private final Collection<Table> viewTables = new ArrayList<Table>();
    private final Collection<Table> mapReduceTables = new ArrayList<Table>();
    private final Collection<Table> replicatedTables = new ArrayList<Table>();
    private final Collection<Table> evictableTables = new ArrayList<Table>();
    
    // ------------------------------------------------------------
    // PLANFRAGMENTS
    // ------------------------------------------------------------
    
    /**
     * PlanFragmentId -> TableIds Read/Written
     */
    private final Map<Integer, int[]> fragmentReadTables = new HashMap<Integer, int[]>(); 
    private final Map<Integer, int[]> fragmentWriteTables = new HashMap<Integer, int[]>();
    

    public CatalogContext(Catalog catalog, String pathToCatalogJar) {
        // check the heck out of the given params in this immutable class
        assert(catalog != null);
        assert(pathToCatalogJar != null);
        if (catalog == null)
            throw new RuntimeException("Can't create CatalogContext with null catalog.");
        if (pathToCatalogJar == null)
            throw new RuntimeException("Can't create CatalogContext with null jar path.");

        m_path = pathToCatalogJar;
        if (pathToCatalogJar.startsWith(NO_PATH) == false)
            m_catalogClassLoader = new JarClassLoader(pathToCatalogJar);
        else
            m_catalogClassLoader = null;
        this.catalog = catalog;
        cluster = catalog.getClusters().get("cluster");
        database = cluster.getDatabases().get("database");
        procedures = database.getProcedures();
        authSystem = new AuthSystem(database, cluster.getSecurityenabled());
        sites = cluster.getSites();
        siteTracker = null; // new SiteTracker(cluster.getSites());

        // count nodes
        numberOfNodes = cluster.getHosts().size();

        // count exec sites
        numberOfExecSites = cluster.getSites().size();

        // count partitions
        numberOfPartitions = cluster.getNum_partitions();
        this.partitions = new Partition[numberOfPartitions];
        for (Partition p : CatalogUtil.getAllPartitions(catalog)) {
            this.partitions[p.getId()] = p;
        }
        this.partitionIdArray = new Integer[numberOfPartitions];
        for (int i = 0; i < numberOfPartitions; i++) {
            this.partitionIdArray[i] = Integer.valueOf(i);
            this.partitionIdCollection.add(this.partitionIdArray[i]);
        }
        
        // TABLES
        for (Table tbl : database.getTables()) {
            if (tbl.getSystable()) {
                sysTables.add(tbl);
            }
            else if (tbl.getMapreduce()) {
                mapReduceTables.add(tbl);
            }
            else if (tbl.getMaterializer() != null) {
                viewTables.add(tbl);
            }
            else {
                dataTables.add(tbl);
                if (tbl.getIsreplicated()) {
                    replicatedTables.add(tbl);
                }
                if (tbl.getEvictable()) {
                    evictableTables.add(tbl);
                }
            }
        } // FOR
        
        // PLANFRAGMENTS
        this.initPlanFragments();
    }
    
    private void initPlanFragments() {
        Set<PlanFragment> allFrags = new HashSet<PlanFragment>();
        for (Procedure proc : database.getProcedures()) {
            for (Statement stmt : proc.getStatements()) {
                allFrags.clear();
                allFrags.addAll(stmt.getFragments());
                allFrags.addAll(stmt.getMs_fragments());
                for (PlanFragment frag : allFrags) {
                    Collection<Table> tables = CatalogUtil.getReferencedTables(frag);
                    int tableIds[] = new int[tables.size()];
                    int i = 0;
                    for (Table tbl : tables) {
                        tableIds[i++] = tbl.getRelativeIndex();
                    } // FOR
                    if (frag.getReadonly()) {
                        this.fragmentReadTables.put(frag.getId(), tableIds);
                    } else {
                        this.fragmentWriteTables.put(frag.getId(), tableIds);
                    }
                } // FOR (frag)
            } // FOR (stmt)
        } // FOR (proc)
    }
    

    public CatalogContext deepCopy() {
        return new CatalogContext(catalog.deepCopy(), m_path);
    }

    public CatalogContext update(String pathToNewJar, String diffCommands) {
        Catalog newCatalog = catalog.deepCopy();
        newCatalog.execute(diffCommands);
        CatalogContext retval = new CatalogContext(newCatalog, pathToNewJar);
        return retval;
    }

    /**
     * Given a class name in the catalog jar, loads it from the jar, even if the
     * jar is served from a url and isn't in the classpath.
     *
     * @param procedureClassName The name of the class to load.
     * @return A java Class variable assocated with the class.
     * @throws ClassNotFoundException if the class is not in the jar file.
     */
    public Class<?> classForProcedure(String procedureClassName) throws ClassNotFoundException {
        //System.out.println("Loading class " + procedureClassName);

        // this is a safety mechanism to prevent catalog classes overriding voltdb stuff
        if (procedureClassName.startsWith("org.voltdb."))
            return Class.forName(procedureClassName);

        // look in the catalog for the file
        return m_catalogClassLoader.loadClass(procedureClassName);
    }
    
    /**
     * Return the Partition catalog object for the given PartitionId
     * 
     * @param catalog_item
     * @param id
     * @return
     */
    public Partition getPartitionById(int id) {
        return (this.partitions[id]);
    }
    
    /**
     * Return  of all the partition ids in this H-Store database cluster
     */
    public Collection<Integer> getAllPartitionIdCollection() {
        return (this.partitionIdCollection);
    }
    
    /**
     * Return  of all the partition ids in this H-Store database cluster
     */
    public Integer[] getAllPartitionIdArray() {
        return (this.partitionIdArray);
    }
    
    // ------------------------------------------------------------
    // TABLES + COLUMNS
    // ------------------------------------------------------------

    /**
     * Return all of the internal system tables for the database
     */
    public Collection<Table> getSysTables() {
        return (sysTables);
    }

    /**
     * Return all of the user-defined data tables for the database
     */
    public Collection<Table> getDataTables() {
        return (dataTables);
    }
    
    /**
     * Return all of the materialized view tables for the database
     */
    public Collection<Table> getViewTables() {
        return (viewTables);
    }

    /**
     * Return all of the MapReduce input data tables for the database
     */
    public Collection<Table> getMapReduceTables() {
        return (mapReduceTables);
    }
    
    /**
     * Return all of the replicated tables for the database
     */
    public Collection<Table> getReplicatedTables() {
        return (replicatedTables);
    }
    
    /**
     * Return all of the evictable tables for the database
     */
    public Collection<Table> getEvictableTables() {
        return (evictableTables);
    }
    
    // ------------------------------------------------------------
    // PLANFRAGMENTS
    // ------------------------------------------------------------
    
    /**
     * Return the tableIds that are read by this PlanFragment
     * @param planFragmentId
     * @return
     */
    public int[] getReadTableIds(Integer planFragmentId) {
        return (this.fragmentReadTables.get(planFragmentId));
    }

    /**
     * Return the tableIds that are written by this PlanFragment
     * @param planFragmentId
     * @return
     */
    public int[] getWriteTableIds(Integer planFragmentId) {
        return (this.fragmentWriteTables.get(planFragmentId));
    }
}
