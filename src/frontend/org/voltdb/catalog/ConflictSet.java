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
 * A set of conflicts with another procedures
 */
public class ConflictSet extends CatalogType {

    CatalogMap<ConflictPair> m_readWriteConflicts;
    CatalogMap<ConflictPair> m_writeWriteConflicts;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("procedure", null);
        m_readWriteConflicts = new CatalogMap<ConflictPair>(catalog, this, path + "/" + "readWriteConflicts", ConflictPair.class);
        m_childCollections.put("readWriteConflicts", m_readWriteConflicts);
        m_writeWriteConflicts = new CatalogMap<ConflictPair>(catalog, this, path + "/" + "writeWriteConflicts", ConflictPair.class);
        m_childCollections.put("writeWriteConflicts", m_writeWriteConflicts);
    }

    public void update() {
    }

    /** GETTER: The other procedure that this conflict set is for */
    public Procedure getProcedure() {
        Object o = getField("procedure");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Procedure retval = (Procedure) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("procedure", retval);
            return retval;
        }
        return (Procedure) o;
    }

    /** GETTER: ConflictPairs that the parent Procedure has a read-write conflict with the target procedure */
    public CatalogMap<ConflictPair> getReadwriteconflicts() {
        return m_readWriteConflicts;
    }

    /** GETTER: ConflictPairs that the parent Procedure has a write-write conflict with the target procedure */
    public CatalogMap<ConflictPair> getWritewriteconflicts() {
        return m_writeWriteConflicts;
    }

    /** SETTER: The other procedure that this conflict set is for */
    public void setProcedure(Procedure value) {
        m_fields.put("procedure", value);
    }

}
