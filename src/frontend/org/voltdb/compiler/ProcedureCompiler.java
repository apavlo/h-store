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

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.hsqldb.HSQLInterface;
import org.voltdb.ProcInfo;
import org.voltdb.ProcInfoData;
import org.voltdb.SQLStmt;
import org.voltdb.VoltMapReduceProcedure;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.User;
import org.voltdb.catalog.UserRef;
import org.voltdb.compiler.VoltCompiler.ProcedureDescriptor;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.hstore.HStoreConstants;
import edu.brown.interfaces.Deferrable;
import edu.brown.interfaces.Prefetchable;
import edu.brown.utils.ClassUtil;

/**
 * Compiles stored procedures into a given catalog, invoking the
 * StatementCompiler as needed.
 */
public abstract class ProcedureCompiler {

    static void compile(VoltCompiler compiler, HSQLInterface hsql, DatabaseEstimates estimates, Catalog catalog, Database db, ProcedureDescriptor procedureDescriptor)
            throws VoltCompiler.VoltCompilerException {

        assert (compiler != null);
        assert (hsql != null);
        assert (estimates != null);

        if (procedureDescriptor.m_singleStmt == null)
            compileJavaProcedure(compiler, hsql, estimates, catalog, db, procedureDescriptor);
        else
            compileSingleStmtProcedure(compiler, hsql, estimates, catalog, db, procedureDescriptor);
    }

    static void compileJavaProcedure(VoltCompiler compiler,
                                     HSQLInterface hsql,
                                     DatabaseEstimates estimates,
                                     Catalog catalog,
                                     Database db,
                                     ProcedureDescriptor procedureDescriptor)
            throws VoltCompiler.VoltCompilerException {

        final String className = procedureDescriptor.m_className;

        // Load the class given the class name
        Class<?> procClass = null;
        try {
            procClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            String msg = "Cannot load class for procedure: " + className;
            throw compiler.new VoltCompilerException(msg);
        }

        // get the short name of the class (no package)
        String[] parts = className.split("\\.");
        String shortName = parts[parts.length - 1];

        // add an entry to the catalog
        final Procedure procedure = db.getProcedures().add(shortName);
        procedure.setId(compiler.getNextProcedureId());
        for (String userName : procedureDescriptor.m_authUsers) {
            final User user = db.getUsers().get(userName);
            if (user == null) {
                throw compiler.new VoltCompilerException("Procedure " + className + " has a user " + userName + " that does not exist");
            }
            final UserRef userRef = procedure.getAuthusers().add(userName);
            userRef.setUser(user);
        }
        for (String groupName : procedureDescriptor.m_authGroups) {
            final Group group = db.getGroups().get(groupName);
            if (group == null) {
                throw compiler.new VoltCompilerException("Procedure " + className + " has a group " + groupName + " that does not exist");
            }
            final GroupRef groupRef = procedure.getAuthgroups().add(groupName);
            groupRef.setGroup(group);
        }
        procedure.setClassname(className);
        // sysprocs don't use the procedure compiler
        procedure.setSystemproc(false);
        procedure.setHasjava(true);

        // get the annotation
        // first try to get one that has been passed from the compiler
        ProcInfoData info = compiler.getProcInfoOverride(shortName);
        // then check for the usual one in the class itself
        // and create a ProcInfo.Data instance for it
        if (info == null) {
            info = new ProcInfoData();
            ProcInfo annotationInfo = procClass.getAnnotation(ProcInfo.class);
            if (annotationInfo != null) {
                info.partitionInfo = annotationInfo.partitionInfo();
                info.partitionParam = annotationInfo.partitionParam();
                info.singlePartition = annotationInfo.singlePartition();
                info.mapInputQuery = annotationInfo.mapInputQuery();
                // info.mapEmitTable = annotationInfo.mapEmitTable();
                info.reduceInputQuery = annotationInfo.reduceInputQuery();
                // info.reduceEmitTable = annotationInfo.reduceEmitTable();
            }
        }
        assert (info != null);

        VoltProcedure procInstance = null;
        try {
            procInstance = (VoltProcedure) procClass.newInstance();
        } catch (InstantiationException e1) {
            e1.printStackTrace();
        } catch (IllegalAccessException e1) {
            e1.printStackTrace();
        }

        // MapReduce!
        if (ClassUtil.getSuperClasses(procClass).contains(VoltMapReduceProcedure.class)) {
            procedure.setMapreduce(true);

            // The Map input query is required
            // The Reduce input query is optional
            if (info.mapInputQuery == null || info.mapInputQuery.isEmpty()) {
                String msg = "Procedure: " + shortName + " must include a mapInputQuery";
                throw compiler.new VoltCompilerException(msg);
            }

            Database catalog_db = CatalogUtil.getDatabase(procedure);
            VoltMapReduceProcedure<?> mrInstance = (VoltMapReduceProcedure<?>) procInstance;

            // Initialize the MapOutput table
            // Create an invocation of the VoltMapProcedure so that we can grab
            // the MapOutput's schema
            VoltTable.ColumnInfo[] schema = mrInstance.getMapOutputSchema();
            String tableMapOutput = "MAP_" + procedure.getName();
            Table catalog_tbl = catalog_db.getTables().add(tableMapOutput);
            assert (catalog_tbl != null);
            for (int i = 0; i < schema.length; i++) {
                Column catalog_col = catalog_tbl.getColumns().add(schema[i].getName());
                catalog_col.setIndex(i);
                catalog_col.setNullable(i > 0);
                catalog_col.setType(schema[i].getType().getValue());
                if (i == 0)
                    catalog_tbl.setPartitioncolumn(catalog_col);
            } // FOR
            catalog_tbl.setMapreduce(true);
            catalog_tbl.setIsreplicated(false);

            // Initialize the reduceOutput table
            VoltTable.ColumnInfo[] schema_reduceOutput = mrInstance.getReduceOutputSchema();
            String tableReduceOutput = "REDUCE_" + procedure.getName();
            catalog_tbl = catalog_db.getTables().add(tableReduceOutput);
            assert (catalog_tbl != null);
            for (int i = 0; i < schema_reduceOutput.length; i++) {
                Column catalog_col = catalog_tbl.getColumns().add(schema_reduceOutput[i].getName());
                catalog_col.setIndex(i);
                catalog_col.setNullable(i > 0);
                catalog_col.setType(schema_reduceOutput[i].getType().getValue());
                if (i == 0)
                    catalog_tbl.setPartitioncolumn(catalog_col);
            } // FOR
            catalog_tbl.setMapreduce(true);
            catalog_tbl.setIsreplicated(false);

            // Initialize the Procedure catalog object
            procedure.setMapinputquery(info.mapInputQuery);
            procedure.setMapemittable(tableMapOutput);
            procedure.setReduceemittable(tableReduceOutput);
            procedure.setReduceinputquery(info.reduceInputQuery);
        }

        // track if there are any writer stmts
        boolean procHasWriteStmts = false;

        // iterate through the fields and deal with
        Field[] fields = procClass.getFields();
        for (Field f : fields) {
            if (f.getType() == SQLStmt.class) {
                // String fieldName = f.getName();
                SQLStmt stmt = null;

                try {
                    stmt = (SQLStmt) f.get(procInstance);
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }

                // add the statement to the catalog
                Statement catalogStmt = procedure.getStatements().add(f.getName());
                
                // compile the statement
                try {
                    StatementCompiler.compile(compiler, hsql, catalog, db, estimates, catalogStmt, stmt.getText(), info.singlePartition);
                } catch (VoltCompiler.VoltCompilerException e) {
                    e.printStackTrace();
                    String msg = shortName + "." + f.getName() + ": " + e.getMessage();
                    throw compiler.new VoltCompilerException(msg);
                }

                // If this Field has a Prefetchable annotation or the Statement was 
                // identified as prefetchable in the project XML, then we will want to
                // set the "prefetchable" flag in the catalog for the Statement + Procedure
                if (f.getAnnotation(Prefetchable.class) != null ||
                    procedureDescriptor.m_prefetchable.contains(catalogStmt.getName())) {
                    catalogStmt.setPrefetchable(true);
                    procedure.setPrefetchable(true);
                }
                // If this Field has a Deferrable annotation or the Statement was 
                // identified as deferrable in the project XML, then we will want to
                // set the "deferrable" flag in the catalog for the Statement + Procedure
                if (f.getAnnotation(Deferrable.class) != null) {
                    catalogStmt.setDeferrable(true);
                    procedure.setDeferrable(true);
                }

                // if a single stmt is not read only, then the proc is not read
                // only
                if (catalogStmt.getReadonly() == false)
                    procHasWriteStmts = true;
            }
        }

        // set the read onlyness of a proc
        procedure.setReadonly(procHasWriteStmts == false);

        Class<?>[] paramTypes = populateProcedureParameters(compiler, procClass, procedure);

        // parse the procinfo
        procedure.setSinglepartition(info.singlePartition);
        if (info.partitionInfo != null && info.partitionInfo.isEmpty() == false) {
            parsePartitionInfo(compiler, db, procedure, info.partitionInfo);
            if (procedure.getPartitionparameter() >= paramTypes.length) {
                String msg = "PartitionInfo parameter not a valid parameter for procedure: " + procedure.getClassname();
                throw compiler.new VoltCompilerException(msg);
            }

            // check the type of partition parameter meets our high standards
            Class<?> partitionType = paramTypes[procedure.getPartitionparameter()];
            Class<?>[] validPartitionClzzes = { Long.class, Integer.class, Short.class, Byte.class, long.class, int.class, short.class, byte.class, String.class };
            boolean found = false;
            for (Class<?> candidate : validPartitionClzzes) {
                if (partitionType == candidate)
                    found = true;
            }
            // assume on of the two tests above passes and one fails
            if (!found) {
                String msg = "PartitionInfo parameter must be a String or Number for procedure: " + procedure.getClassname();
                throw compiler.new VoltCompilerException(msg);
            }
        } else {
            procedure.setPartitionparameter(NullProcParameter.PARAM_IDX);
        }
        
        // ProcInfo.partitionParam overrides everything else
        if (info.partitionParam != -1) {
            if (info.partitionParam >= paramTypes.length || info.partitionParam < 0) {
                String msg = "PartitionInfo 'partitionParam' not a valid parameter for procedure: " + procedure.getClassname();
                throw compiler.new VoltCompilerException(msg);
            }
            procedure.setPartitionparameter(info.partitionParam);
        }

        // put the compiled code for this procedure into the jarfile
        // VoltCompiler.addClassToJar(procClass, compiler);
    }

    static Class<?>[] populateProcedureParameters(VoltCompiler compiler, Class<?> procClass, Procedure procedure) throws VoltCompiler.VoltCompilerException {

        final String[] parts = procedure.getClassname().split("\\.");
        final String shortName = parts[parts.length - 1];

        // find the run() method and get the params
        Method procMethod = null;
        Method[] methods = procClass.getMethods();

        // DONE(xin): Check to make sure that the queries defined in the the
        // mapInputQuery and the reduceInputQuery
        // exist in the procedure
        // DONE(xin): Check to make sure that the database includes the
        // map/reduce output tables

        // Database catalog_db =
        // edu.brown.catalog.CatalogUtil.getDatabase(procedure);
        // FIXME catalog_db.getTables().get(procedure.getMapemittable());

        boolean isMapReduce = procedure.getMapreduce();
        Statement mapStatement = null;
        if (isMapReduce) {
            String mapInputQuery = procedure.getMapinputquery();
            mapStatement = procedure.getStatements().get(mapInputQuery);
            if (mapStatement == null) {
                String msg = "Procedure: " + shortName + " uses undefined mapInputQuery '" + mapInputQuery + "'";
                throw compiler.new VoltCompilerException(msg);
            }

            String reduceInputQuery = procedure.getReduceinputquery();
            Statement reduceStatement = null;
            if (reduceInputQuery != null && reduceInputQuery.isEmpty() == false) {
                reduceStatement = procedure.getStatements().get(reduceInputQuery);
                if (reduceStatement == null) {
                    String msg = "Procedure: " + shortName + " uses undefined reduceInputQuery '" + reduceInputQuery + "'";
                    throw compiler.new VoltCompilerException(msg);
                }
            }
        }

        for (final Method m : methods) {
            String name = m.getName();
            if (name.equals("run")) {
                // if not null, then we've got more than one run method
                if (procMethod != null) {
                    String msg = "Procedure: " + shortName + " has multiple run(...) methods. ";
                    msg += "Only a single run(...) method is supported.";
                    throw compiler.new VoltCompilerException(msg);
                }
                // found it!
                procMethod = m;
            }

        }
        // check if there is run method

        if (procMethod == null) {
            String msg = "Procedure: " + shortName + " has no run(...) method.";
            throw compiler.new VoltCompilerException(msg);
        }

        if ((procMethod.getReturnType() != VoltTable[].class) && (procMethod.getReturnType() != VoltTable.class) && (procMethod.getReturnType() != long.class)
                && (procMethod.getReturnType() != Long.class)) {

            String msg = "Procedure: " + shortName + " has run(...) method that doesn't return long, Long, VoltTable or VoltTable[].";
            throw compiler.new VoltCompilerException(msg);
        }

        CatalogMap<ProcParameter> params = procedure.getParameters(); // procedure
                                                                      // parameters
        Class<?>[] paramTypes = null;

        // Set procedure parameter types from its run method parameters
        if (isMapReduce == false) {
            paramTypes = procMethod.getParameterTypes();// run method parameters
            for (int i = 0; i < paramTypes.length; i++) {
                Class<?> cls = paramTypes[i];
                ProcParameter param = params.add(String.valueOf(i));
                param.setIndex(i);

                // handle the case where the param is an array
                if (cls.isArray()) {
                    param.setIsarray(true);
                    cls = cls.getComponentType();
                } else
                    param.setIsarray(false);

                VoltType type;
                try {
                    type = VoltType.typeFromClass(cls);
                } catch (RuntimeException e) {
                    // handle the case where the type is invalid
                    String msg = "Procedure: " + shortName + " has a parameter with invalid type: ";
                    msg += cls.getSimpleName();
                    throw compiler.new VoltCompilerException(msg);
                }
                param.setType(type.getValue());
            } // FOR
        }
        // The input parameters to the MapInputQuery are the input parameters
        // for the Procedure
        else {
            paramTypes = new Class<?>[mapStatement.getParameters().size()];
            for (int i = 0; i < paramTypes.length; i++) {
                StmtParameter catalog_stmt_param = mapStatement.getParameters().get(i);
                assert (catalog_stmt_param != null);
                VoltType vtype = VoltType.get(catalog_stmt_param.getJavatype());
                paramTypes[i] = vtype.classFromType();

                ProcParameter catalog_proc_param = procedure.getParameters().add(catalog_stmt_param.getName());
                catalog_proc_param.setIndex(i);
                catalog_proc_param.setIsarray(false); // One day...
                catalog_proc_param.setType(vtype.getValue());
            } // FOR
        }
        return (paramTypes);
    }

    static void compileSingleStmtProcedure(VoltCompiler compiler, HSQLInterface hsql, DatabaseEstimates estimates, Catalog catalog, Database db, ProcedureDescriptor procedureDescriptor)
            throws VoltCompiler.VoltCompilerException {

        final String className = procedureDescriptor.m_className;
        if (className.indexOf('@') != -1) {
            throw compiler.new VoltCompilerException("User procedure names can't contain \"@\".");
        }

        // get the short name of the class (no package)
        String[] parts = className.split("\\.");
        String shortName = parts[parts.length - 1];

        // add an entry to the catalog
        final Procedure procedure = db.getProcedures().add(shortName);
        procedure.setId(compiler.getNextProcedureId());
        for (String userName : procedureDescriptor.m_authUsers) {
            final User user = db.getUsers().get(userName);
            if (user == null) {
                throw compiler.new VoltCompilerException("Procedure " + className + " has a user " + userName + " that does not exist");
            }
            final UserRef userRef = procedure.getAuthusers().add(userName);
            userRef.setUser(user);
        }
        for (String groupName : procedureDescriptor.m_authGroups) {
            final Group group = db.getGroups().get(groupName);
            if (group == null) {
                throw compiler.new VoltCompilerException("Procedure " + className + " has a group " + groupName + " that does not exist");
            }
            final GroupRef groupRef = procedure.getAuthgroups().add(groupName);
            groupRef.setGroup(group);
        }
        procedure.setClassname(className);
        // sysprocs don't use the procedure compiler
        procedure.setSystemproc(false);
        procedure.setHasjava(false);

        // get the annotation
        // first try to get one that has been passed from the compiler
        ProcInfoData info = compiler.getProcInfoOverride(shortName);
        // then check for the usual one in the class itself
        // and create a ProcInfo.Data instance for it
        if (info == null) {
            info = new ProcInfoData();
            if (procedureDescriptor.m_partitionString != null) {
                info.partitionInfo = procedureDescriptor.m_partitionString;
                info.singlePartition = true;
            }
        }
        assert (info != null);

        // ADD THE STATEMENT

        // add the statement to the catalog
        Statement catalogStmt = procedure.getStatements().add(HStoreConstants.ANON_STMT_NAME);

        // compile the statement
        StatementCompiler.compile(compiler, hsql, catalog, db, estimates, catalogStmt, procedureDescriptor.m_singleStmt, info.singlePartition);

        // if the single stmt is not read only, then the proc is not read only
        boolean procHasWriteStmts = (catalogStmt.getReadonly() == false);

        // set the read onlyness of a proc
        procedure.setReadonly(procHasWriteStmts == false);

        // set procedure parameter types
        CatalogMap<ProcParameter> params = procedure.getParameters();
        CatalogMap<StmtParameter> stmtParams = catalogStmt.getParameters();

        // set the procedure parameter types from the statement parameter types
        int i = 0;
        for (StmtParameter stmtParam : CatalogUtil.getSortedCatalogItems(stmtParams, "index")) {
            // name each parameter "param1", "param2", etc...
            ProcParameter procParam = params.add("param" + String.valueOf(i));
            procParam.setIndex(stmtParam.getIndex());
            procParam.setIsarray(false);
            procParam.setType(stmtParam.getJavatype());
            i++;
        }

        // parse the procinfo
        procedure.setSinglepartition(info.singlePartition);
        if (info.singlePartition) {
            parsePartitionInfo(compiler, db, procedure, info.partitionInfo);
            if (procedure.getPartitionparameter() >= params.size()) {
                String msg = "PartitionInfo parameter not a valid parameter for procedure: " + procedure.getClassname();
                throw compiler.new VoltCompilerException(msg);
            }
        }
    }

    /**
     * Determine which parameter is the partition indicator
     */
    static void parsePartitionInfo(VoltCompiler compiler, Database db, Procedure procedure, String info) throws VoltCompilerException {

        // assert(procedure.getSinglepartition() == true);

        // check this isn't empty
        if (info.length() == 0) {
            String msg = "Missing or Truncated PartitionInfo in attribute for procedure: " + procedure.getClassname();
            throw compiler.new VoltCompilerException(msg);
        }

        // split on the colon
        String[] parts = info.split(":");

        // if the colon doesn't split well, we have a problem
        if (parts.length != 2) {
            String msg = "Possibly invalid PartitionInfo in attribute for procedure: " + procedure.getClassname();
            throw compiler.new VoltCompilerException(msg);
        }

        // relabel the parts for code readability
        String columnInfo = parts[0].trim();
        int paramIndex = Integer.parseInt(parts[1].trim());

        int paramCount = procedure.getParameters().size();
        if ((paramIndex < 0) || (paramIndex >= paramCount)) {
            String msg = "PartitionInfo specifies invalid column for procedure: " + procedure.getClassname();
            throw compiler.new VoltCompilerException(msg);
        }

        // locate the parameter
        procedure.setPartitionparameter(paramIndex);

        // split the columninfo
        parts = columnInfo.split("\\.");
        if (parts.length != 2) {
            String msg = "Possibly invalid PartitionInfo in attribute for procedure: " + procedure.getClassname();
            throw compiler.new VoltCompilerException(msg);
        }

        // relabel the parts for code readability
        String tableName = parts[0].trim();
        String columnName = parts[1].trim();

        // locate the partition column
        CatalogMap<Table> tables = db.getTables();
        for (Table table : tables) {
            if (table.getTypeName().equalsIgnoreCase(tableName)) {
                CatalogMap<Column> columns = table.getColumns();
                for (Column column : columns) {
                    if (column.getTypeName().equalsIgnoreCase(columnName)) {
                        procedure.setPartitioncolumn(column);
                        return;
                    }
                }
            }
        }

        String msg = "Unable to locate partition column in PartitionInfo for procedure: " + procedure.getClassname();
        throw compiler.new VoltCompilerException(msg);
    }
}
