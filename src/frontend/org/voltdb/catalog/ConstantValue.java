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
 * What John Hugg doesn't want me to have
 */
public class ConstantValue extends CatalogType {

    String m_value = new String();
    boolean m_is_null;
    int m_type;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("value", m_value);
        m_fields.put("is_null", m_is_null);
        m_fields.put("type", m_type);
    }

    public void update() {
        m_value = (String) m_fields.get("value");
        m_is_null = (Boolean) m_fields.get("is_null");
        m_type = (Integer) m_fields.get("type");
    }

    /** GETTER: A string representation of the value */
    public String getValue() {
        return m_value;
    }

    /** GETTER: Whether the value is null */
    public boolean getIs_null() {
        return m_is_null;
    }

    /** GETTER: The type of the value (int/double/date/etc) */
    public int getType() {
        return m_type;
    }

    /** SETTER: A string representation of the value */
    public void setValue(String value) {
        m_value = value; m_fields.put("value", value);
    }

    /** SETTER: Whether the value is null */
    public void setIs_null(boolean value) {
        m_is_null = value; m_fields.put("is_null", value);
    }

    /** SETTER: The type of the value (int/double/date/etc) */
    public void setType(int value) {
        m_type = value; m_fields.put("type", value);
    }

}
