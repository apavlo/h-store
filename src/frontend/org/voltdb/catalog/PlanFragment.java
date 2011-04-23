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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

/**
 * Instructions to the executor to execute part of an execution plan
 */
public class PlanFragment extends CatalogType {

    int m_id;
    boolean m_hasdependencies;
    boolean m_multipartition;
    boolean m_readonly;
    String m_plannodetree = new String();
    boolean m_nontransactional;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        this.addField("id", m_id);
        this.addField("hasdependencies", m_hasdependencies);
        this.addField("multipartition", m_multipartition);
        this.addField("readonly", m_readonly);
        this.addField("plannodetree", m_plannodetree);
        this.addField("nontransactional", m_nontransactional);
    }

    void update() {
        m_id = (Integer) m_fields.get("id");
        m_hasdependencies = (Boolean) m_fields.get("hasdependencies");
        m_multipartition = (Boolean) m_fields.get("multipartition");
        m_readonly = (Boolean) m_fields.get("readonly");
        m_plannodetree = (String) m_fields.get("plannodetree");
        m_nontransactional = (Boolean) m_fields.get("nontransactional");
    }

    /** GETTER: Unique Id for this PlanFragment */
    public int getId() {
        return m_id;
    }

    /** GETTER: Dependencies must be received before this plan fragment can execute */
    public boolean getHasdependencies() {
        return m_hasdependencies;
    }

    /** GETTER: Should this plan fragment be sent to all partitions */
    public boolean getMultipartition() {
        return m_multipartition;
    }

    /** GETTER: Whether this PlanFragment is read only */
    public boolean getReadonly() {
        return m_readonly;
    }

    /** GETTER: A serialized representation of the plan-graph/plan-pipeline */
    public String getPlannodetree() {
        return m_plannodetree;
    }

    /** GETTER: True if this fragment doesn't read from or write to any persistent tables */
    public boolean getNontransactional() {
        return m_nontransactional;
    }

    /** SETTER: Unique Id for this PlanFragment */
    public void setId(int value) {
        m_id = value; m_fields.put("id", value);
    }

    /** SETTER: Dependencies must be received before this plan fragment can execute */
    public void setHasdependencies(boolean value) {
        m_hasdependencies = value; m_fields.put("hasdependencies", value);
    }

    /** SETTER: Should this plan fragment be sent to all partitions */
    public void setMultipartition(boolean value) {
        m_multipartition = value; m_fields.put("multipartition", value);
    }

    /** SETTER: Whether this PlanFragment is read only */
    public void setReadonly(boolean value) {
        m_readonly = value; m_fields.put("readonly", value);
    }

    /** SETTER: A serialized representation of the plan-graph/plan-pipeline */
    public void setPlannodetree(String value) {
        m_plannodetree = value; m_fields.put("plannodetree", value);
    }

    /** SETTER: True if this fragment doesn't read from or write to any persistent tables */
    public void setNontransactional(boolean value) {
        m_nontransactional = value; m_fields.put("nontransactional", value);
    }

}
