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

package org.voltdb.compiler;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.ProcInfoData;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.utils.Pair;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.ClusterConfiguration;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.VerticalPartitionColumn;
import edu.brown.interfaces.Deferrable;
import edu.brown.interfaces.Prefetchable;
import edu.brown.mappings.ParameterMapping;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.mappings.ParametersUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;

/**
 * Alternate (programmatic) interface to VoltCompiler. Give the class all of
 * the information a user would put in a VoltDB project file and it will go
 * and build the project file and run the compiler on it.
 *
 */
public class VoltProjectBuilder {
    private static final Logger LOG = Logger.getLogger(VoltProjectBuilder.class);

    final LinkedHashSet<String> m_schemas = new LinkedHashSet<String>();
    protected final String project_name;

    public static final class ProcedureInfo {
        private final String users[];
        private final String groups[];
        private final Class<?> cls;
        private final String name;
        private final String sql;
        private final String partitionInfo;

        public ProcedureInfo(final String users[], final String groups[], final Class<?> cls) {
            this.users = users;
            this.groups = groups;
            this.cls = cls;
            this.name = cls.getSimpleName();
            this.sql = null;
            this.partitionInfo = null;
            assert(this.name != null);
        }

        public ProcedureInfo(final String users[], final String groups[], final String name, final String sql, final String partitionInfo) {
            assert(name != null);
            this.users = users;
            this.groups = groups;
            this.cls = null;
            this.name = name;
            this.sql = sql;
            this.partitionInfo = partitionInfo;
            assert(this.name != null);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof ProcedureInfo) {
                final ProcedureInfo oInfo = (ProcedureInfo)o;
                return name.equals(oInfo.name);
            }
            return false;
        }
    }

    public static final class UserInfo {
        private final String name;
        private final boolean adhoc;
        private final boolean sysproc;
        private final String password;
        private final String groups[];

        public UserInfo (final String name, final boolean adhoc, final boolean sysproc, final String password, final String groups[]){
            this.name = name;
            this.adhoc = adhoc;
            this.sysproc = sysproc;
            this.password = password;
            this.groups = groups;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof UserInfo) {
                final UserInfo oInfo = (UserInfo)o;
                return name.equals(oInfo.name);
            }
            return false;
        }
    }

    public static final class GroupInfo {
        private final String name;
        private final boolean adhoc;
        private final boolean sysproc;

        public GroupInfo(final String name, final boolean adhoc, final boolean sysproc){
            this.name = name;
            this.adhoc = adhoc;
            this.sysproc = sysproc;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof GroupInfo) {
                final GroupInfo oInfo = (GroupInfo)o;
                return name.equals(oInfo.name);
            }
            return false;
        }
    }

    /** An export/tables/table entry */
    public static final class ELTTableInfo {
        final public String m_tablename;
        final public boolean m_export_only;
        ELTTableInfo(String tablename, boolean append) {
            m_tablename = tablename;
            m_export_only = append;
        }
    }
    final ArrayList<ELTTableInfo> m_eltTables = new ArrayList<ELTTableInfo>();

    final LinkedHashSet<UserInfo> m_users = new LinkedHashSet<UserInfo>();
    final LinkedHashSet<GroupInfo> m_groups = new LinkedHashSet<GroupInfo>();
    final LinkedHashSet<ProcedureInfo> m_procedures = new LinkedHashSet<ProcedureInfo>();
    final LinkedHashSet<Class<?>> m_supplementals = new LinkedHashSet<Class<?>>();
    final LinkedHashMap<String, String> m_partitionInfos = new LinkedHashMap<String, String>();
    
    /**
     * Replicated SecondaryIndex Info
     * TableName -> Pair<CreateIndex, ColumnNames>
     */
    private final LinkedHashMap<String, Pair<Boolean, Collection<String>>> m_replicatedSecondaryIndexes = new LinkedHashMap<String, Pair<Boolean, Collection<String>>>();
    private boolean m_replicatedSecondaryIndexesEnabled = true;
    
    /**
     * Evictable Tables
     */
    private final HashSet<String> m_evictableTables = new HashSet<String>();
    
    private final HashSet<String> m_batchEvictableTables = new HashSet<String>();
    
    /**
     * Prefetchable Queries
     * ProcedureName -> StatementName
     * @see Prefetchable
     */
    private final HashMap<String, Set<String>> m_prefetchQueries = new HashMap<String, Set<String>>();

    /**
     * Deferrable Queries
     * ProcedureName -> StatementName
     * @see Deferrable
     */
    private final HashMap<String, Set<String>> m_deferQueries = new HashMap<String, Set<String>>();
    
    /**
     * File containing ParameterMappingsSet
     */
    private File m_paramMappingsFile;
    
    /**
     * Values for a ParameterMappingsSet that we will construct
     * after we have compile the catalog
     */
    final LinkedHashMap<String, Map<Integer, Pair<String, Integer>>> m_paramMappings = new LinkedHashMap<String, Map<Integer,Pair<String,Integer>>>();
    
    String m_elloader = null;         // loader package.Classname
    private boolean m_elenabled;      // true if enabled; false if disabled
    List<String> m_elAuthUsers;       // authorized users
    List<String> m_elAuthGroups;      // authorized groups

    BackendTarget m_target = BackendTarget.NATIVE_EE_JNI;
    PrintStream m_compilerDebugPrintStream = null;
    boolean m_securityEnabled = false;
    final Map<String, ProcInfoData> m_procInfoOverrides = new HashMap<String, ProcInfoData>();
    final ClusterConfiguration cluster_config = new ClusterConfiguration();

    private String m_snapshotPath = null;
    private int m_snapshotRetain = 0;
    private String m_snapshotPrefix = null;
    private String m_snapshotFrequency = null;

	

    public VoltProjectBuilder(String project_name) {
        this.project_name = project_name;
    }

    public String getProjectName() {
        return project_name;
    }
    
    public void addAllDefaults() {
        // does nothing in the base class
    }

    public void addUsers(final UserInfo users[]) {
        for (final UserInfo info : users) {
            final boolean added = m_users.add(info);
            if (!added) {
                assert(added);
            }
        }
    }

    public void addGroups(final GroupInfo groups[]) {
        for (final GroupInfo info : groups) {
            final boolean added = m_groups.add(info);
            if (!added) {
                assert(added);
            }
        }
    }
    
    // -------------------------------------------------------------------
    // DATABASE PARTITIONS
    // -------------------------------------------------------------------
    
    public void clearPartitions() {
        this.cluster_config.clear();
    }
    
    public void addPartition(String hostname, int site_id, int partition_id) {
        this.cluster_config.addPartition(hostname, site_id, partition_id);
    }
    
    // -------------------------------------------------------------------
    // SCHEMA
    // -------------------------------------------------------------------

    public void addSchema(final URL schemaURL) {
        assert(schemaURL != null) :
            "Invalid null schema file for " + this.project_name;
        addSchema(schemaURL.getPath());
    }
    
    public void addSchema(final File schemaFile) {
        assert(schemaFile != null);
        addSchema(schemaFile.getAbsolutePath());
    }

    public void addSchema(String schemaPath) {
        try {
            schemaPath = URLDecoder.decode(schemaPath, "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        assert(m_schemas.contains(schemaPath) == false);
        final File schemaFile = new File(schemaPath);
        assert(schemaFile != null);
        assert(schemaFile.isDirectory() == false);
        // this check below fails in some valid cases (like when the file is in a jar)
        //assert schemaFile.canRead()
        //    : "can't read file: " + schemaPath;

        m_schemas.add(schemaPath);
    }

    // -------------------------------------------------------------------
    // PROCEDURES
    // -------------------------------------------------------------------
    
    protected String getStmtProcedureSQL(String name) {
        for (ProcedureInfo pi : m_procedures) {
            if (pi.name.equals(name)) {
                return (pi.sql);
            }
        }
        return (null);
    }
    
    public void clearProcedures() {
        m_procedures.clear();
        m_procInfoOverrides.clear();
        m_prefetchQueries.clear();
    }
    
    /**
     * Remove all of the Procedures whose name matches the given Pattern
     * @param procNameRegex
     */
    public void removeProcedures(Pattern procNameRegex) {
        Set<ProcedureInfo> toRemove = new HashSet<ProcedureInfo>();
        for (ProcedureInfo procInfo : m_procedures) {
            Matcher m = procNameRegex.matcher(procInfo.name);
            if (m.matches()) {
                toRemove.add(procInfo);
            }
        } // FOR
        for (ProcedureInfo procInfo : toRemove)
            this.removeProcedure(procInfo);
    }
    
    /**
     * Remove a Procedure based on its name
     * @param procName
     */
    public void removeProcedure(String procName) {
        for (ProcedureInfo procInfo : m_procedures) {
            if (procInfo.name.equalsIgnoreCase(procName)) {
                this.removeProcedure(procInfo);
                break;
            }
        } // FOR
    }
    
    /**
     * Removes the given ProcedureInfo
     * @param procInfo
     */
    protected void removeProcedure(ProcedureInfo procInfo) {
        m_procedures.remove(procInfo);
        m_procInfoOverrides.remove(procInfo.name);
        m_prefetchQueries.remove(procInfo.name);
        m_paramMappings.remove(procInfo.name);
        if (LOG.isDebugEnabled())
            LOG.debug("Removed Procedure " + procInfo.name + " from project " + this.project_name.toUpperCase());
    }
    
    /**
     * Provide the path to the ParameterMappingsSet file to use to
     * populate the Catalog after it has been created.
     * @param mappingsFile
     */
    public void addParameterMappings(File mappingsFile) {
        assert(mappingsFile != null) :
            "Invalid ParameterMappingsSet file";
        assert(mappingsFile.exists()) :
            "The ParameterMappingsSet file '" + mappingsFile + "' does not exist";
        m_paramMappingsFile = mappingsFile;
    }

    /**
     * Mark a ProcParameter to be mapped to a StmtParameter
     * @param procedureClass
     * @param procParamIdx
     * @param statementName
     * @param stmtParamIdx
     */
    public void mapParameters(Class<? extends VoltProcedure> procedureClass, int procParamIdx, String statementName, int stmtParamIdx) {
        this.mapParameters(procedureClass.getSimpleName(), procParamIdx, statementName, stmtParamIdx);
    }
    
    /**
     * Mark a ProcParameter to be mapped to a StmtParameter
     * @param procedureName
     * @param procParamIdx
     * @param statementName
     * @param stmtParamIdx
     */
    public void mapParameters(String procedureName, int procParamIdx, String statementName, int stmtParamIdx) {
        Map<Integer, Pair<String, Integer>> m = m_paramMappings.get(procedureName);
        if (m == null) {
            m = new LinkedHashMap<Integer, Pair<String,Integer>>();
            m_paramMappings.put(procedureName, m);
        }
        Pair<String, Integer> stmtPair = Pair.of(statementName, stmtParamIdx);
        m.put(procParamIdx, stmtPair);
    }
    
    // -------------------------------------------------------------------
    // PREFETCHABLE
    // -------------------------------------------------------------------
    
    /**
     * Mark a Statement as prefetchable
     * @param procedureName
     * @param statementName
     */
    public void markStatementPrefetchable(Class<? extends VoltProcedure> procedureClass, String statementName) {
        this.markStatementPrefetchable(procedureClass.getSimpleName(), statementName);
    }

    /**
     * Mark a Statement as prefetchable
     * @param procedureName
     * @param statementName
     */
    public void markStatementPrefetchable(String procedureName, String statementName) {
        Set<String> stmtNames = m_prefetchQueries.get(procedureName);
        if (stmtNames == null) {
            stmtNames = new HashSet<String>();
            m_prefetchQueries.put(procedureName, stmtNames);
        }
        stmtNames.add(statementName);
    }
    
    // -------------------------------------------------------------------
    // EVICTABLE TABLES
    // -------------------------------------------------------------------
    
    /**
     * Mark a table as evictable. When using the anti-caching feature, this means
     * that portions of this table can be moved out to blocks on disk 
     * @param tableName
     */
    public void markTableEvictable(String tableName) {
        m_evictableTables.add(tableName);
    }

    /**
     * Mark a table as evictable. When using the anti-caching feature, this means
     * that portions of this table can be moved out to blocks on disk 
     * @param tableName
     */
    public void markTableBatchEvictable(String tableName) {
        m_batchEvictableTables.add(tableName);
    }

    // -------------------------------------------------------------------
    // DEFERRABLE STATEMENTS
    // -------------------------------------------------------------------
    
    /**
     * Mark a Statement as deferrable
     * @param procedureName
     * @param statementName
     */
    public void markStatementDeferrable(Class<? extends VoltProcedure> procedureClass, String statementName) {
        this.markStatementDeferrable(procedureClass.getSimpleName(), statementName);
    }

    /**
     * Mark a Statement as deferrable
     * @param procedureName
     * @param statementName
     */
    public void markStatementDeferrable(String procedureName, String statementName) {
        Set<String> stmtNames = m_deferQueries.get(procedureName);
        if (stmtNames == null) {
            stmtNames = new HashSet<String>();
            m_deferQueries.put(procedureName, stmtNames);
        }
        stmtNames.add(statementName);
    }
    
    // -------------------------------------------------------------------
    // SINGLE-STATEMENT PROCEDURES
    // -------------------------------------------------------------------
    
    /**
     * Create a single statement procedure that only has one query
     * The input parameters to the SQL statement will be automatically passed 
     * from the input parameters to the procedure.
     * @param procedureName
     * @param sql
     */
    public void addStmtProcedure(String procedureName, String sql) {
        addStmtProcedure(procedureName, sql, null);
    }

    public void addStmtProcedure(String name, String sql, String partitionInfo) {
        addProcedures(new ProcedureInfo(new String[0], new String[0], name, sql, partitionInfo));
    }
    
    public void addProcedure(final Class<?> procedure) {
        final ArrayList<ProcedureInfo> procArray = new ArrayList<ProcedureInfo>();
        procArray.add(new ProcedureInfo(new String[0], new String[0], procedure));
        addProcedures(procArray);
    }

    public void addProcedures(final Class<?>... procedures) {
        if (procedures != null && procedures.length > 0) {
            final ArrayList<ProcedureInfo> procArray = new ArrayList<ProcedureInfo>();
            for (final Class<?> procedure : procedures)
                procArray.add(new ProcedureInfo(new String[0], new String[0], procedure));
            addProcedures(procArray);
        }
    }
    
    /*
     * List of users and groups permitted to invoke the procedure
     */
    public void addProcedures(final ProcedureInfo... procedures) {
        final ArrayList<ProcedureInfo> procArray = new ArrayList<ProcedureInfo>();
        for (final ProcedureInfo procedure : procedures)
            procArray.add(procedure);
        addProcedures(procArray);
    }

    public void addProcedures(final Iterable<ProcedureInfo> procedures) {
        // check for duplicates and existings
        final Set<ProcedureInfo> newProcs = new HashSet<ProcedureInfo>();
        for (final ProcedureInfo procInfo : procedures) {
            assert(newProcs.contains(procInfo) == false);
            if (m_procedures.contains(procInfo)) {
                LOG.warn(String.format("Skipping duplicate procedure '%s' for %s",
                         procInfo.name, this.project_name));
            }
            newProcs.add(procInfo);
        } // FOR

        // add the procs
        for (final ProcedureInfo procedure : newProcs) {
            m_procedures.add(procedure);
        }
    }
    
    public void addPartitionInfo(final String tableName, final String partitionColumnName) {
        assert (m_partitionInfos.containsKey(tableName) == false);
        m_partitionInfos.put(tableName, partitionColumnName);
    }            

    public void addSupplementalClasses(final Class<?>... supplementals) {
        final ArrayList<Class<?>> suppArray = new ArrayList<Class<?>>();
        for (final Class<?> supplemental : supplementals)
            suppArray.add(supplemental);
        addSupplementalClasses(suppArray);
    }

    public void addSupplementalClasses(final Iterable<Class<?>> supplementals) {
        // check for duplicates and existings
        final HashSet<Class<?>> newSupps = new HashSet<Class<?>>();
        for (final Class<?> supplemental : supplementals) {
            assert(newSupps.contains(supplemental) == false);
            assert(m_supplementals.contains(supplemental) == false);
            newSupps.add(supplemental);
        }

        // add the supplemental classes
        for (final Class<?> supplemental : supplementals)
            m_supplementals.add(supplemental);
    }

    // -------------------------------------------------------------------
    // TABLE PARTITIONS
    // -------------------------------------------------------------------
    
    public void addTablePartitionInfo(Table catalog_tbl, Column catalog_col) {
        assert(catalog_col != null) : "Unexpected null partition column for " + catalog_tbl;
        
        // TODO: Support special columns
        if (catalog_col instanceof VerticalPartitionColumn) {
            catalog_col = ((VerticalPartitionColumn)catalog_col).getHorizontalColumn();
        }
        if (catalog_col instanceof MultiColumn) {
            catalog_col = ((MultiColumn)catalog_col).get(0);
        }
        this.addTablePartitionInfo(catalog_tbl.getName(), catalog_col.getName());
    }
    
    public void addTablePartitionInfo(final String tableName, final String partitionColumnName) {
        assert(m_partitionInfos.containsKey(tableName) == false) :
            String.format("Already contains table partitioning info for '%s': %s",
                          tableName, m_partitionInfos.get(tableName));
        m_partitionInfos.put(tableName, partitionColumnName);
    }
    
    // -------------------------------------------------------------------
    // REPLICATED SECONDARY INDEXES
    // -------------------------------------------------------------------
    
    public void removeReplicatedSecondaryIndexes() {
        m_replicatedSecondaryIndexes.clear();
    }
    
    public void addReplicatedSecondaryIndex(final String tableName, final String...partitionColumnNames) {
        this.addReplicatedSecondaryIndexInfo(tableName, true, partitionColumnNames);
    }
    
    public void addReplicatedSecondaryIndexInfo(final String tableName, final boolean createIndex, final String...partitionColumnNames) {
        ArrayList<String> columns = new ArrayList<String>();
        for (String col : partitionColumnNames) {
            columns.add(col);
        }
        this.addReplicatedSecondaryIndexInfo(tableName, createIndex, columns);
    }
    
    public void addReplicatedSecondaryIndexInfo(final String tableName, final boolean createIndex, final Collection<String> partitionColumnNames) {
        assert(m_replicatedSecondaryIndexes.containsKey(tableName) == false);
        m_replicatedSecondaryIndexes.put(tableName, Pair.of(createIndex, partitionColumnNames));
    }
    
    public void enableReplicatedSecondaryIndexes(boolean val) { 
        m_replicatedSecondaryIndexesEnabled = val;
    }

    public void setSecurityEnabled(final boolean enabled) {
        m_securityEnabled = enabled;
    }

    public void setSnapshotSettings(
            String frequency,
            int retain,
            String path,
            String prefix) {
        assert(frequency != null);
        assert(path != null);
        assert(prefix != null);
        m_snapshotFrequency = frequency;
        m_snapshotRetain = retain;
        m_snapshotPath = path;
        m_snapshotPrefix = prefix;
    }


    public void addELT(final String loader, boolean enabled,
            List<String> users, List<String> groups) {
        m_elloader = loader;
        m_elenabled = enabled;
        m_elAuthUsers = users;
        m_elAuthGroups = groups;
    }

    public void addELTTable(String name, boolean exportonly) {
        ELTTableInfo info = new ELTTableInfo(name, exportonly);
        m_eltTables.add(info);
    }

    public void setCompilerDebugPrintStream(final PrintStream out) {
        m_compilerDebugPrintStream = out;
    }

    /**
     * Override the procedure annotation with the specified values for a
     * specified procedure.
     *
     * @param procName The name of the procedure to override the annotation.
     * @param info The values to use instead of the annotation.
     */
    public void overrideProcInfoForProcedure(final String procName, final ProcInfoData info) {
        assert(procName != null);
        assert(info != null);
        m_procInfoOverrides.put(procName, info);
    }

    public boolean compile(final String jarPath) {
        return compile(jarPath, 1, 1, 0, "localhost");
    }

    public boolean compile(final File jarPath, final int sitesPerHost, final int replication) {
        return compile(jarPath.getAbsolutePath(), sitesPerHost, 1, replication, "localhost");
    }
    
    public boolean compile(final String jarPath, final int sitesPerHost, final int replication) {
        return compile(jarPath, sitesPerHost, 1, replication, "localhost");
    }

    public boolean compile(final String jarPath, final int sitesPerHost, final int hostCount,
                           final int replication, final String leaderAddress)
    {
        VoltCompiler compiler = new VoltCompiler();
        if (m_replicatedSecondaryIndexesEnabled) {
            compiler.enableVerticalPartitionOptimizations();
        }
        return compile(compiler, jarPath, sitesPerHost, hostCount, replication,
                       leaderAddress);
    }

    public boolean compile(final VoltCompiler compiler, final String jarPath,
                           final int sitesPerHost, final int hostCount,
                           final int replication, final String leaderAddress)
    {
        assert(jarPath != null);
        assert(sitesPerHost >= 1);
        assert(hostCount >= 1);
        assert(leaderAddress != null);

        // this stuff could all be converted to org.voltdb.compiler.projectfile.*
        // jaxb objects and (WE ARE!) marshaled to XML. Just needs some elbow grease.

        DocumentBuilderFactory docFactory;
        DocumentBuilder docBuilder;
        Document doc;
        try {
            docFactory = DocumentBuilderFactory.newInstance();
            docBuilder = docFactory.newDocumentBuilder();
            doc = docBuilder.newDocument();
        }
        catch (final ParserConfigurationException e) {
            e.printStackTrace();
            return false;
        }

        // <project>
        final Element project = doc.createElement("project");
        doc.appendChild(project);

        // <security>
        final Element security = doc.createElement("security");
        security.setAttribute("enabled", Boolean.valueOf(m_securityEnabled).toString());
        project.appendChild(security);

        // <database>
        final Element database = doc.createElement("database");
        database.setAttribute("name", "database");
        database.setAttribute("project", this.project_name);
        project.appendChild(database);
        buildDatabaseElement(doc, database);

        // boilerplate to write this DOM object to file.
        StreamResult result;
        try {
            final Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            result = new StreamResult(new StringWriter());
            final DOMSource domSource = new DOMSource(doc);
            transformer.transform(domSource, result);
        }
        catch (final TransformerConfigurationException e) {
            e.printStackTrace();
            return false;
        }
        catch (final TransformerFactoryConfigurationError e) {
            e.printStackTrace();
            return false;
        }
        catch (final TransformerException e) {
            e.printStackTrace();
            return false;
        }

//        String xml = result.getWriter().toString();
//        System.out.println(xml);

        final File projectFile = writeStringToTempFile(result.getWriter().toString());
        final String projectPath = projectFile.getPath();
        LOG.debug("PROJECT XML: " + projectPath);
        
        ClusterConfig cc = (this.cluster_config.isEmpty() ? 
                                new ClusterConfig(hostCount, sitesPerHost, replication, leaderAddress) :
                                this.cluster_config);
        final boolean success = compiler.compile(projectPath,
                                           cc,
                                           jarPath,
                                           m_compilerDebugPrintStream,
                                           m_procInfoOverrides);
        
        // HACK: If we have a ParameterMappingsSet that we need to apply
        // either from a file or a fixed mappings, then we have 
        // to load the catalog into this JVM, apply the mappings, and then
        // update the jar file with the new catalog
        if (m_paramMappingsFile != null || m_paramMappings.isEmpty() == false) {
            File jarFile = new File(jarPath);
            Catalog catalog = CatalogUtil.loadCatalogFromJar(jarFile);
            assert(catalog != null);
            Database catalog_db = CatalogUtil.getDatabase(catalog);
            
            this.applyParameterMappings(catalog_db);
            
            // Construct a List of prefetchable Statements
            this.applyPrefetchableFlags(catalog_db);
            
            // Write it out!
            try {
                CatalogUtil.updateCatalogInJar(jarFile, catalog, m_paramMappingsFile);
            } catch (Exception ex) {
                String msg = "Failed to updated Catalog in jar file '" + jarPath + "'";
                throw new RuntimeException(msg, ex);
            }
        }
        
        return success;
    }
    
    private void applyParameterMappings(Database catalog_db) {
        ParameterMappingsSet mappings = new ParameterMappingsSet();        
        
        // Load ParameterMappingSet from file
        if (m_paramMappingsFile != null) {
            try {
                mappings.load(m_paramMappingsFile, catalog_db);
            } catch (IOException ex) {
                String msg = "Failed to load ParameterMappingsSet file '" + m_paramMappingsFile + "'";
                throw new RuntimeException(msg, ex);
            }
        }
        // Build ParameterMappingSet from user-provided inputs
        else {
            for (String procName : m_paramMappings.keySet()) {
                Procedure catalog_proc = catalog_db.getProcedures().getIgnoreCase(procName);
                assert(catalog_proc != null) :
                    "Invalid Procedure name for ParameterMappings '" + procName + "'";
                for (Integer procParamIdx : m_paramMappings.get(procName).keySet()) {
                    ProcParameter catalog_procParam = catalog_proc.getParameters().get(procParamIdx.intValue());
                    assert(catalog_procParam != null) :
                        "Invalid ProcParameter for '" + procName + "' at offset " + procParamIdx;
                    Pair<String, Integer> stmtPair = m_paramMappings.get(procName).get(procParamIdx);
                    assert(stmtPair != null);
                    
                    Statement catalog_stmt = catalog_proc.getStatements().getIgnoreCase(stmtPair.getFirst());
                    assert(catalog_stmt != null) :
                        "Invalid Statement name '" + stmtPair.getFirst() + "' for ParameterMappings " +
                		"for Procedure '" + procName + "'";
                    StmtParameter catalog_stmtParam = catalog_stmt.getParameters().get(stmtPair.getSecond().intValue());
                    assert(catalog_stmtParam != null) :
                        "Invalid StmtParameter for '" + catalog_stmt.fullName() + "' at offset " + stmtPair.getSecond();
                    
                    // HACK: This assumes that the ProcParameter is not an array
                    // and that we want to map the first invocation of the Statement
                    // directly to the ProcParameter.
                    ParameterMapping pm = new ParameterMapping(catalog_stmt,
                                                               0,
                                                               catalog_stmtParam,
                                                               catalog_procParam,
                                                               0,
                                                               1.0);
                    mappings.add(pm);
                } // FOR (ProcParameter)
            } // FOR (Procedure)
        }
        
        // Apply it!
        ParametersUtil.applyParameterMappings(catalog_db, mappings);
    }

    private void applyPrefetchableFlags(Database catalog_db) {
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            boolean proc_prefetchable = false;
            for (Statement statement : catalog_proc.getStatements()) {
                boolean stmt_prefetchable = true;
                for (StmtParameter stmtParam : statement.getParameters()) {
                    if (stmtParam.getProcparameter() == null) {
                        stmt_prefetchable = false;
                        break;
                    }
                } // FOR (StmtParameter)
                if (stmt_prefetchable) {
                    statement.setPrefetchable(true);
                    proc_prefetchable = true;
                }
            } // FOR (Statement)
            if (proc_prefetchable) {
                catalog_proc.setPrefetchable(true);
            }
        } // FOR (Procedure)
    }
    
    private void buildDatabaseElement(Document doc, final Element database) {

        // /project/database/users
        final Element users = doc.createElement("users");
        database.appendChild(users);

        // users/user
        if (m_users.isEmpty()) {
            final Element user = doc.createElement("user");
            user.setAttribute("name", "default");
            user.setAttribute("groups", "default");
            user.setAttribute("password", "");
            user.setAttribute("sysproc", "true");
            user.setAttribute("adhoc", "true");
            users.appendChild(user);
        }
        else {
            for (final UserInfo info : m_users) {
                final Element user = doc.createElement("user");
                user.setAttribute("name", info.name);
                user.setAttribute("password", info.password);
                user.setAttribute("sysproc", info.sysproc ? "true" : "false");
                user.setAttribute("adhoc", info.adhoc ? "true" : "false");
                // build up user/@groups. This attribute must be redesigned
                if (info.groups.length > 0) {
                    final StringBuilder groups = new StringBuilder();
                    for (final String group : info.groups) {
                        if (groups.length() > 0)
                            groups.append(",");
                        groups.append(group);
                    }
                    user.setAttribute("groups", groups.toString());
                }
                users.appendChild(user);
            }
        }

        // /project/database/groups
        final Element groups = doc.createElement("groups");
        database.appendChild(groups);

        // groups/group
        if (m_groups.isEmpty()) {
            final Element group = doc.createElement("group");
            group.setAttribute("name", "default");
            group.setAttribute("sysproc", "true");
            group.setAttribute("adhoc", "true");
            groups.appendChild(group);
        }
        else {
            for (final GroupInfo info : m_groups) {
                final Element group = doc.createElement("group");
                group.setAttribute("name", info.name);
                group.setAttribute("sysproc", info.sysproc ? "true" : "false");
                group.setAttribute("adhoc", info.adhoc ? "true" : "false");
                groups.appendChild(group);
            }
        }

        // /project/database/schemas
        final Element schemas = doc.createElement("schemas");
        database.appendChild(schemas);

        // schemas/schema
        for (final String schemaPath : m_schemas) {
            final Element schema = doc.createElement("schema");
            schema.setAttribute("path", schemaPath);
            schemas.appendChild(schema);
        }

        // /project/database/procedures
        final Element procedures = doc.createElement("procedures");
        database.appendChild(procedures);

        // procedures/procedure
        for (final ProcedureInfo procedure : m_procedures) {
            if (procedure.cls == null)
                continue;
            assert(procedure.sql == null);

            final Element proc = doc.createElement("procedure");
            proc.setAttribute("class", procedure.cls.getName());
            // build up @users. This attribute should be redesigned
            if (procedure.users.length > 0) {
                final StringBuilder userattr = new StringBuilder();
                for (final String user : procedure.users) {
                    if (userattr.length() > 0)
                        userattr.append(",");
                    userattr.append(user);
                }
                proc.setAttribute("users", userattr.toString());
            }
            // build up @groups. This attribute should be redesigned
            if (procedure.groups.length > 0) {
                final StringBuilder groupattr = new StringBuilder();
                for (final String group : procedure.groups) {
                    if (groupattr.length() > 0)
                        groupattr.append(",");
                    groupattr.append(group);
                }
                proc.setAttribute("groups", groupattr.toString());
            }
            
            // HACK: Prefetchable Statements
            if (m_prefetchQueries.containsKey(procedure.cls.getSimpleName())) {
                Collection<String> stmtNames = m_prefetchQueries.get(procedure.cls.getSimpleName());
                proc.setAttribute("prefetchable", StringUtil.join(",", stmtNames));
            }
            // HACK: Deferrable Statements
            if (m_deferQueries.containsKey(procedure.cls.getSimpleName())) {
                Collection<String> stmtNames = m_deferQueries.get(procedure.cls.getSimpleName());
                proc.setAttribute("deferrable", StringUtil.join(",", stmtNames));
            }
            
            procedures.appendChild(proc);
        }

        // procedures/procedures (that are stmtprocedures)
        for (final ProcedureInfo procedure : m_procedures) {
            if (procedure.sql == null)
                continue;
            assert(procedure.cls == null);

            final Element proc = doc.createElement("procedure");
            proc.setAttribute("class", procedure.name);
            if (procedure.partitionInfo != null);
                proc.setAttribute("partitioninfo", procedure.partitionInfo);
            // build up @users. This attribute should be redesigned
            if (procedure.users.length > 0) {
                final StringBuilder userattr = new StringBuilder();
                for (final String user : procedure.users) {
                    if (userattr.length() > 0)
                        userattr.append(",");
                    userattr.append(user);
                }
                proc.setAttribute("users", userattr.toString());
            }
            // build up @groups. This attribute should be redesigned
            if (procedure.groups.length > 0) {
                final StringBuilder groupattr = new StringBuilder();
                for (final String group : procedure.groups) {
                    if (groupattr.length() > 0)
                        groupattr.append(",");
                    groupattr.append(group);
                }
                proc.setAttribute("groups", groupattr.toString());
            }

            final Element sql = doc.createElement("sql");
            proc.appendChild(sql);

            final Text sqltext = doc.createTextNode(procedure.sql);
            sql.appendChild(sqltext);

            procedures.appendChild(proc);
        }

        if (m_partitionInfos.size() > 0) {
            // /project/database/partitions
            final Element partitions = doc.createElement("partitions");
            database.appendChild(partitions);

            // partitions/table
            for (final Entry<String, String> partitionInfo : m_partitionInfos.entrySet()) {
                final Element table = doc.createElement("partition");
                table.setAttribute("table", partitionInfo.getKey());
                table.setAttribute("column", partitionInfo.getValue());
                partitions.appendChild(table);
            }
        }

        // Evictable Tables
        if (m_evictableTables.isEmpty() == false) {
            final Element evictables = doc.createElement("evictables");
            database.appendChild(evictables);
            
            // Table entries
            for (String tableName : m_evictableTables) {
                final Element table = doc.createElement("evictable");
                table.setAttribute("table", tableName);
                evictables.appendChild(table);
            }
        }
        // BatchEvictable Tables
        if (m_batchEvictableTables.isEmpty() == false) {
            final Element batchevictables = doc.createElement("batchevictables");
            database.appendChild(batchevictables);
            
            // Table entries
            for (String tableName : m_batchEvictableTables) {
                final Element table = doc.createElement("evictable");
                table.setAttribute("table", tableName);
                batchevictables.appendChild(table);
            }
        }        
        // Vertical Partitions
        if (m_replicatedSecondaryIndexes.size() > 0) {
            // /project/database/partitions
            final Element verticalpartitions = doc.createElement("verticalpartitions");
            database.appendChild(verticalpartitions);

            // partitions/table
            for (String tableName : m_replicatedSecondaryIndexes.keySet()) {
                Pair<Boolean, Collection<String>> p = m_replicatedSecondaryIndexes.get(tableName);
                Boolean createIndex = p.getFirst();
                Collection<String> columnNames = p.getSecond(); 
                
                final Element vp = doc.createElement("verticalpartition");
                vp.setAttribute("table", tableName);
                vp.setAttribute("indexed", createIndex.toString());
                for (final String columnName : columnNames) {
                    final Element column = doc.createElement("column");
                    column.setTextContent(columnName);
                    vp.appendChild(column);
                } // FOR (cols)
                verticalpartitions.appendChild(vp);
            } // FOR (tables)
        }

        // /project/database/classdependencies
        final Element classdeps = doc.createElement("classdependencies");
        database.appendChild(classdeps);

        // classdependency
        for (final Class<?> supplemental : m_supplementals) {
            final Element supp= doc.createElement("classdependency");
            supp.setAttribute("class", supplemental.getName());
            classdeps.appendChild(supp);
        }

        // project/database/exports
        if (m_elloader != null) {
            final Element exports = doc.createElement("exports");
            database.appendChild(exports);

            final Element conn = doc.createElement("connector");
            conn.setAttribute("class", m_elloader);
            conn.setAttribute("enabled", m_elenabled ? "true" : "false");

            // turn list into stupid comma separated attribute list
            String usersattr = "";
            if (m_elAuthUsers != null) {
                for (String s : m_elAuthUsers) {
                    if (usersattr.isEmpty()) {
                        usersattr += s;
                    }
                    else {
                        usersattr += "," + s;
                    }
                }
                conn.setAttribute("users", usersattr);
            }

            // turn list into stupid comma separated attribute list
            String groupsattr = "";
            if (m_elAuthGroups != null) {
                for (String s : m_elAuthGroups) {
                    if (groupsattr.isEmpty()) {
                        groupsattr += s;
                    }
                    else {
                        groupsattr += "," + s;
                    }
                }
                conn.setAttribute("groups", groupsattr);
            }

            exports.appendChild(conn);

            if (m_eltTables.size() > 0) {
                final Element tables = doc.createElement("tables");
                conn.appendChild(tables);

                for (ELTTableInfo info : m_eltTables) {
                    final Element table = doc.createElement("table");
                    table.setAttribute("name", info.m_tablename);
                    table.setAttribute("exportonly", info.m_export_only ? "true" : "false");
                    tables.appendChild(table);
                }
            }
        }

        if (m_snapshotPath != null) {
            final Element snapshot = doc.createElement("snapshot");
            snapshot.setAttribute("frequency", m_snapshotFrequency);
            snapshot.setAttribute("path", m_snapshotPath);
            snapshot.setAttribute("prefix", m_snapshotPrefix);
            snapshot.setAttribute("retain", Integer.toString(m_snapshotRetain));
            database.appendChild(snapshot);
        }
    }

    /**
     * Utility method to take a string and put it in a file. This is used by
     * this class to write the project file to a temp file, but it also used
     * by some other tests.
     *
     * @param content The content of the file to create.
     * @return A reference to the file created or null on failure.
     */
    public static File writeStringToTempFile(final String content) {
        return FileUtil.writeStringToTempFile(content, "project", true);
    }
}
