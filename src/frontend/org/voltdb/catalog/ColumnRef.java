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
 * A reference to a table column
 */
public class ColumnRef extends CatalogType {

    int m_index;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("index", m_index);
        m_fields.put("column", null);
    }

    public void update() {
        m_index = (Integer) m_fields.get("index");
    }

    /** GETTER: The index within the set */
    public int getIndex() {
        return m_index;
    }

    /** GETTER: The table column being referenced */
    public Column getColumn() {
        Object o = getField("column");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Column retval = (Column) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("column", retval);
            return retval;
        }
        return (Column) o;
    }

    /** SETTER: The index within the set */
    public void setIndex(int value) {
        m_index = value; m_fields.put("index", value);
    }

    /** SETTER: The table column being referenced */
    public void setColumn(Column value) {
        m_fields.put("column", value);
    }

}
