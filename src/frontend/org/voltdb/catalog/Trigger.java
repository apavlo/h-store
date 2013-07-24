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
 * Trigger objects on a stream, with a statement attached
 */
public class Trigger extends CatalogType {

    int m_id;
    int m_triggerType;
    boolean m_forEach;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("id", m_id);
        m_fields.put("sourceStream", null);
        m_fields.put("triggerType", m_triggerType);
        m_fields.put("forEach", m_forEach);
        m_fields.put("stmt", null);
    }

    public void update() {
        m_id = (Integer) m_fields.get("id");
        m_triggerType = (Integer) m_fields.get("triggerType");
        m_forEach = (Boolean) m_fields.get("forEach");
    }

    /** GETTER: Unique identifier for this Trigger. Allows for faster look-ups */
    public int getId() {
        return m_id;
    }

    /** GETTER: Stream on which the trigger is placed. */
    public Stream getSourcestream() {
        Object o = getField("sourceStream");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Stream retval = (Stream) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("sourceStream", retval);
            return retval;
        }
        return (Stream) o;
    }

    /** GETTER: Insert / Update / Delete */
    public int getTriggertype() {
        return m_triggerType;
    }

    /** GETTER: Is this for each tuple, or each statement */
    public boolean getForeach() {
        return m_forEach;
    }

    /** GETTER: What to execute when this trigger is activated"			 */
    public Statement getStmt() {
        Object o = getField("stmt");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Statement retval = (Statement) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("stmt", retval);
            return retval;
        }
        return (Statement) o;
    }

    /** SETTER: Unique identifier for this Trigger. Allows for faster look-ups */
    public void setId(int value) {
        m_id = value; m_fields.put("id", value);
    }

    /** SETTER: Stream on which the trigger is placed. */
    public void setSourcestream(Stream value) {
        m_fields.put("sourceStream", value);
    }

    /** SETTER: Insert / Update / Delete */
    public void setTriggertype(int value) {
        m_triggerType = value; m_fields.put("triggerType", value);
    }

    /** SETTER: Is this for each tuple, or each statement */
    public void setForeach(boolean value) {
        m_forEach = value; m_fields.put("forEach", value);
    }

    /** SETTER: What to execute when this trigger is activated"			 */
    public void setStmt(Statement value) {
        m_fields.put("stmt", value);
    }

}
