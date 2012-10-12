/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

/**
 * A pair of Statements that have a conflict
 */
public class ConflictPair extends CatalogType {

    CatalogMap<TableRef> m_tables;
    boolean m_alwaysConflicting;
    int m_conflictType;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("statement0", null);
        m_fields.put("statement1", null);
        m_tables = new CatalogMap<TableRef>(catalog, this, path + "/" + "tables", TableRef.class);
        m_childCollections.put("tables", m_tables);
        m_fields.put("alwaysConflicting", m_alwaysConflicting);
        m_fields.put("conflictType", m_conflictType);
    }

    public void update() {
        m_alwaysConflicting = (Boolean) m_fields.get("alwaysConflicting");
        m_conflictType = (Integer) m_fields.get("conflictType");
    }

    /** GETTER: The source Statement */
    public Statement getStatement0() {
        Object o = getField("statement0");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Statement retval = (Statement) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("statement0", retval);
            return retval;
        }
        return (Statement) o;
    }

    /** GETTER: The destination Statement */
    public Statement getStatement1() {
        Object o = getField("statement1");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Statement retval = (Statement) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("statement1", retval);
            return retval;
        }
        return (Statement) o;
    }

    /** GETTER: The list of tables that caused this conflict */
    public CatalogMap<TableRef> getTables() {
        return m_tables;
    }

    /** GETTER: If true, then this ConflictPair will always cause a conflict */
    public boolean getAlwaysconflicting() {
        return m_alwaysConflicting;
    }

    /** GETTER: Type of conflict (ConflictType) */
    public int getConflicttype() {
        return m_conflictType;
    }

    /** SETTER: The source Statement */
    public void setStatement0(Statement value) {
        m_fields.put("statement0", value);
    }

    /** SETTER: The destination Statement */
    public void setStatement1(Statement value) {
        m_fields.put("statement1", value);
    }

    /** SETTER: If true, then this ConflictPair will always cause a conflict */
    public void setAlwaysconflicting(boolean value) {
        m_alwaysConflicting = value; m_fields.put("alwaysConflicting", value);
    }

    /** SETTER: Type of conflict (ConflictType) */
    public void setConflicttype(int value) {
        m_conflictType = value; m_fields.put("conflictType", value);
    }

}
