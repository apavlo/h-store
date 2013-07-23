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

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

/**
 * <p>A simple wrapper of a parameterized SQL statement. VoltDB uses this instead of
 * a Java String type for performance reasons and to cache statement meta-data like
 * result schema, etc..</p>
 *
 * <p>SQLStmts are used exclusively in subclasses of {@link VoltProcedure}</p>
 *
 * @see VoltProcedure
 */
public class SQLStmt {
    final String sqlText;
    int hashCode;
    byte statementParamJavaTypes[];
    int numStatementParamJavaTypes;
    long fragGUIDs[];
    int numFragGUIDs;
    Statement catStmt;

    /**
     * Construct a SQLStmt instance from a SQL statement.
     *
     * @param sqlText Valid VoltDB complient SQL with question marks as parameter
     * place holders.
     */
    public SQLStmt(String sqlText) {
        this.sqlText = sqlText;
        this.computeHashCode();
    }
    
    public SQLStmt(Statement catalog_stmt) {
        this(catalog_stmt, (catalog_stmt.getHas_singlesited() ? catalog_stmt.getMs_fragments() : catalog_stmt.getFragments()));
    }
    
    public SQLStmt(Statement catalog_stmt, CatalogMap<PlanFragment> fragments) {
        this.sqlText = catalog_stmt.getSqltext();
        this.catStmt = catalog_stmt;

        this.numFragGUIDs = fragments.size();
        this.fragGUIDs = new long[this.numFragGUIDs];
        int i = 0;
        for (PlanFragment frag : fragments) {
            this.fragGUIDs[i++] = Integer.parseInt(frag.getName());
        }
        
        this.numStatementParamJavaTypes = catalog_stmt.getParameters().size();
        this.statementParamJavaTypes = new byte[this.numStatementParamJavaTypes];
        for (i = 0; i < this.numStatementParamJavaTypes; i++) {
            this.statementParamJavaTypes[i] = (byte)this.catStmt.getParameters().get(i).getJavatype();
        } // FOR
        this.computeHashCode();
    }
    
    protected void computeHashCode() {
        if (this.catStmt != null) {
            this.hashCode = this.catStmt.hashCode();
        } else {
            this.hashCode = this.sqlText.hashCode();
        }
    }

    /**
     * Get the text of the SQL statement represented.
     *
     * @return String containing the text of the SQL statement represented.
     */
    public String getText() {
        return sqlText;
    }

    public Statement getStatement() {
        return (this.catStmt);
    }
    
    /**
     * Get the Procedure catalog object for this Statement
     * @return
     */
    public final Procedure getProcedure() {
        return (this.catStmt != null ? (Procedure)this.catStmt.getParent() : null);
    }
    
    @Override
    public int hashCode() {
        return (this.hashCode);
    }
    
    @Override
    public String toString() {
        return (catStmt != null ? catStmt.fullName() : super.toString());
    }
}
