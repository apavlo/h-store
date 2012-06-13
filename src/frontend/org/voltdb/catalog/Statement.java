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
 * A parameterized SQL statement embedded in a stored procedure
 */
public class Statement extends CatalogType {

    int m_id;
    String m_sqltext = new String();
    int m_querytype;
    boolean m_readonly;
    boolean m_singlepartition;
    boolean m_replicatedtabledml;
    boolean m_replicatedonly;
    boolean m_batched;
    boolean m_secondaryindex;
    boolean m_prefetchable;
    boolean m_deferrable;
    int m_paramnum;
    CatalogMap<StmtParameter> m_parameters;
    CatalogMap<Column> m_output_columns;
    boolean m_has_singlesited;
    CatalogMap<PlanFragment> m_fragments;
    String m_exptree = new String();
    String m_fullplan = new String();
    boolean m_has_multisited;
    CatalogMap<PlanFragment> m_ms_fragments;
    String m_ms_exptree = new String();
    String m_ms_fullplan = new String();
    int m_cost;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("id", m_id);
        m_fields.put("sqltext", m_sqltext);
        m_fields.put("querytype", m_querytype);
        m_fields.put("readonly", m_readonly);
        m_fields.put("singlepartition", m_singlepartition);
        m_fields.put("replicatedtabledml", m_replicatedtabledml);
        m_fields.put("replicatedonly", m_replicatedonly);
        m_fields.put("batched", m_batched);
        m_fields.put("secondaryindex", m_secondaryindex);
        m_fields.put("prefetchable", m_prefetchable);
        m_fields.put("deferrable", m_deferrable);
        m_fields.put("paramnum", m_paramnum);
        m_parameters = new CatalogMap<StmtParameter>(catalog, this, path + "/" + "parameters", StmtParameter.class);
        m_childCollections.put("parameters", m_parameters);
        m_output_columns = new CatalogMap<Column>(catalog, this, path + "/" + "output_columns", Column.class);
        m_childCollections.put("output_columns", m_output_columns);
        m_fields.put("has_singlesited", m_has_singlesited);
        m_fragments = new CatalogMap<PlanFragment>(catalog, this, path + "/" + "fragments", PlanFragment.class);
        m_childCollections.put("fragments", m_fragments);
        m_fields.put("exptree", m_exptree);
        m_fields.put("fullplan", m_fullplan);
        m_fields.put("has_multisited", m_has_multisited);
        m_ms_fragments = new CatalogMap<PlanFragment>(catalog, this, path + "/" + "ms_fragments", PlanFragment.class);
        m_childCollections.put("ms_fragments", m_ms_fragments);
        m_fields.put("ms_exptree", m_ms_exptree);
        m_fields.put("ms_fullplan", m_ms_fullplan);
        m_fields.put("cost", m_cost);
    }

    public void update() {
        m_id = (Integer) m_fields.get("id");
        m_sqltext = (String) m_fields.get("sqltext");
        m_querytype = (Integer) m_fields.get("querytype");
        m_readonly = (Boolean) m_fields.get("readonly");
        m_singlepartition = (Boolean) m_fields.get("singlepartition");
        m_replicatedtabledml = (Boolean) m_fields.get("replicatedtabledml");
        m_replicatedonly = (Boolean) m_fields.get("replicatedonly");
        m_batched = (Boolean) m_fields.get("batched");
        m_secondaryindex = (Boolean) m_fields.get("secondaryindex");
        m_prefetchable = (Boolean) m_fields.get("prefetchable");
        m_deferrable = (Boolean) m_fields.get("deferrable");
        m_paramnum = (Integer) m_fields.get("paramnum");
        m_has_singlesited = (Boolean) m_fields.get("has_singlesited");
        m_exptree = (String) m_fields.get("exptree");
        m_fullplan = (String) m_fields.get("fullplan");
        m_has_multisited = (Boolean) m_fields.get("has_multisited");
        m_ms_exptree = (String) m_fields.get("ms_exptree");
        m_ms_fullplan = (String) m_fields.get("ms_fullplan");
        m_cost = (Integer) m_fields.get("cost");
    }

    /** GETTER: Unique identifier for this Procedure. Allows for faster look-ups */
    public int getId() {
        return m_id;
    }

    /** GETTER: The text of the sql statement */
    public String getSqltext() {
        return m_sqltext;
    }

    public int getQuerytype() {
        return m_querytype;
    }

    /** GETTER: Can the statement modify any data? */
    public boolean getReadonly() {
        return m_readonly;
    }

    /** GETTER: Does the statement only use data on one partition? */
    public boolean getSinglepartition() {
        return m_singlepartition;
    }

    /** GETTER: Should the result of this statememt be divided by partition count before returned */
    public boolean getReplicatedtabledml() {
        return m_replicatedtabledml;
    }

    /** GETTER: Does this statement only access replicated tables? */
    public boolean getReplicatedonly() {
        return m_replicatedonly;
    }

    public boolean getBatched() {
        return m_batched;
    }

    public boolean getSecondaryindex() {
        return m_secondaryindex;
    }

    /** GETTER: Whether this query should be examined for pre-fetching if Procedure is being executed as a distributed transaction */
    public boolean getPrefetchable() {
        return m_prefetchable;
    }

    /** GETTER: Whether this query does not need to executed immediately in this transaction */
    public boolean getDeferrable() {
        return m_deferrable;
    }

    public int getParamnum() {
        return m_paramnum;
    }

    /** GETTER: The set of parameters to this SQL statement */
    public CatalogMap<StmtParameter> getParameters() {
        return m_parameters;
    }

    /** GETTER: The set of columns in the output table */
    public CatalogMap<Column> getOutput_columns() {
        return m_output_columns;
    }

    /** GETTER: Whether this statement has a single-sited query plan */
    public boolean getHas_singlesited() {
        return m_has_singlesited;
    }

    /** GETTER: The set of plan fragments used to execute this statement */
    public CatalogMap<PlanFragment> getFragments() {
        return m_fragments;
    }

    /** GETTER: A serialized representation of the original expression tree */
    public String getExptree() {
        return m_exptree;
    }

    /** GETTER: A serialized representation of the un-fragmented plan */
    public String getFullplan() {
        return m_fullplan;
    }

    /** GETTER: Whether this statement has a multi-sited query plan */
    public boolean getHas_multisited() {
        return m_has_multisited;
    }

    /** GETTER: The set of multi-sited plan fragments used to execute this statement */
    public CatalogMap<PlanFragment> getMs_fragments() {
        return m_ms_fragments;
    }

    /** GETTER: A serialized representation of the multi-sited query plan */
    public String getMs_exptree() {
        return m_ms_exptree;
    }

    /** GETTER: A serialized representation of the multi-sited query plan */
    public String getMs_fullplan() {
        return m_ms_fullplan;
    }

    /** GETTER: The cost of this plan measured in arbitrary units */
    public int getCost() {
        return m_cost;
    }

    /** SETTER: Unique identifier for this Procedure. Allows for faster look-ups */
    public void setId(int value) {
        m_id = value; m_fields.put("id", value);
    }

    /** SETTER: The text of the sql statement */
    public void setSqltext(String value) {
        m_sqltext = value; m_fields.put("sqltext", value);
    }

    public void setQuerytype(int value) {
        m_querytype = value; m_fields.put("querytype", value);
    }

    /** SETTER: Can the statement modify any data? */
    public void setReadonly(boolean value) {
        m_readonly = value; m_fields.put("readonly", value);
    }

    /** SETTER: Does the statement only use data on one partition? */
    public void setSinglepartition(boolean value) {
        m_singlepartition = value; m_fields.put("singlepartition", value);
    }

    /** SETTER: Should the result of this statememt be divided by partition count before returned */
    public void setReplicatedtabledml(boolean value) {
        m_replicatedtabledml = value; m_fields.put("replicatedtabledml", value);
    }

    /** SETTER: Does this statement only access replicated tables? */
    public void setReplicatedonly(boolean value) {
        m_replicatedonly = value; m_fields.put("replicatedonly", value);
    }

    public void setBatched(boolean value) {
        m_batched = value; m_fields.put("batched", value);
    }

    public void setSecondaryindex(boolean value) {
        m_secondaryindex = value; m_fields.put("secondaryindex", value);
    }

    /** SETTER: Whether this query should be examined for pre-fetching if Procedure is being executed as a distributed transaction */
    public void setPrefetchable(boolean value) {
        m_prefetchable = value; m_fields.put("prefetchable", value);
    }

    /** SETTER: Whether this query does not need to executed immediately in this transaction */
    public void setDeferrable(boolean value) {
        m_deferrable = value; m_fields.put("deferrable", value);
    }

    public void setParamnum(int value) {
        m_paramnum = value; m_fields.put("paramnum", value);
    }

    /** SETTER: Whether this statement has a single-sited query plan */
    public void setHas_singlesited(boolean value) {
        m_has_singlesited = value; m_fields.put("has_singlesited", value);
    }

    /** SETTER: A serialized representation of the original expression tree */
    public void setExptree(String value) {
        m_exptree = value; m_fields.put("exptree", value);
    }

    /** SETTER: A serialized representation of the un-fragmented plan */
    public void setFullplan(String value) {
        m_fullplan = value; m_fields.put("fullplan", value);
    }

    /** SETTER: Whether this statement has a multi-sited query plan */
    public void setHas_multisited(boolean value) {
        m_has_multisited = value; m_fields.put("has_multisited", value);
    }

    /** SETTER: A serialized representation of the multi-sited query plan */
    public void setMs_exptree(String value) {
        m_ms_exptree = value; m_fields.put("ms_exptree", value);
    }

    /** SETTER: A serialized representation of the multi-sited query plan */
    public void setMs_fullplan(String value) {
        m_ms_fullplan = value; m_fields.put("ms_fullplan", value);
    }

    /** SETTER: The cost of this plan measured in arbitrary units */
    public void setCost(int value) {
        m_cost = value; m_fields.put("cost", value);
    }

}
