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
 * A stream (relation) in the database
 */
public class Stream extends CatalogType {

    CatalogMap<Column> m_columns;
    CatalogMap<Index> m_indexes;
    CatalogMap<Trigger> m_triggers;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_columns = new CatalogMap<Column>(catalog, this, path + "/" + "columns", Column.class);
        m_childCollections.put("columns", m_columns);
        m_indexes = new CatalogMap<Index>(catalog, this, path + "/" + "indexes", Index.class);
        m_childCollections.put("indexes", m_indexes);
        m_triggers = new CatalogMap<Trigger>(catalog, this, path + "/" + "triggers", Trigger.class);
        m_childCollections.put("triggers", m_triggers);
        m_fields.put("partitioncolumn", null);
    }

    public void update() {
    }

    /** GETTER: The set of columns in the stream */
    public CatalogMap<Column> getColumns() {
        return m_columns;
    }

    /** GETTER: The set of indexes on the columns in the stream */
    public CatalogMap<Index> getIndexes() {
        return m_indexes;
    }

    /** GETTER: The set of triggers for this stream */
    public CatalogMap<Trigger> getTriggers() {
        return m_triggers;
    }

    /** GETTER: On which column is the stream horizontally partitioned */
    public Column getPartitioncolumn() {
        Object o = getField("partitioncolumn");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Column retval = (Column) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("partitioncolumn", retval);
            return retval;
        }
        return (Column) o;
    }

    /** SETTER: On which column is the stream horizontally partitioned */
    public void setPartitioncolumn(Column value) {
        m_fields.put("partitioncolumn", value);
    }

}
