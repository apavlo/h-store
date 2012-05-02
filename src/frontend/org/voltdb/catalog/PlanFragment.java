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
 * Instructions to the executor to execute part of an execution plan
 */
public class PlanFragment extends CatalogType {

    int m_id;
    boolean m_hasdependencies;
    boolean m_multipartition;
    boolean m_readonly;
    String m_plannodetree = new String();
    boolean m_nontransactional;
    boolean m_fastaggregate;
    boolean m_fastcombine;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("id", m_id);
        m_fields.put("hasdependencies", m_hasdependencies);
        m_fields.put("multipartition", m_multipartition);
        m_fields.put("readonly", m_readonly);
        m_fields.put("plannodetree", m_plannodetree);
        m_fields.put("nontransactional", m_nontransactional);
        m_fields.put("fastaggregate", m_fastaggregate);
        m_fields.put("fastcombine", m_fastcombine);
    }

    public void update() {
        m_id = (Integer) m_fields.get("id");
        m_hasdependencies = (Boolean) m_fields.get("hasdependencies");
        m_multipartition = (Boolean) m_fields.get("multipartition");
        m_readonly = (Boolean) m_fields.get("readonly");
        m_plannodetree = (String) m_fields.get("plannodetree");
        m_nontransactional = (Boolean) m_fields.get("nontransactional");
        m_fastaggregate = (Boolean) m_fields.get("fastaggregate");
        m_fastcombine = (Boolean) m_fields.get("fastcombine");
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

    /** GETTER: Whether this PlanFragment is an aggregate that can be executed in Java */
    public boolean getFastaggregate() {
        return m_fastaggregate;
    }

    /** GETTER: Whether this PlanFragment just combines its input tables and therefore can be executed in Java */
    public boolean getFastcombine() {
        return m_fastcombine;
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

    /** SETTER: Whether this PlanFragment is an aggregate that can be executed in Java */
    public void setFastaggregate(boolean value) {
        m_fastaggregate = value; m_fields.put("fastaggregate", value);
    }

    /** SETTER: Whether this PlanFragment just combines its input tables and therefore can be executed in Java */
    public void setFastcombine(boolean value) {
        m_fastcombine = value; m_fields.put("fastcombine", value);
    }

}
