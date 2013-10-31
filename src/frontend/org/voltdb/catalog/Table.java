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
 * A table (relation) in the database
 */
public class Table extends CatalogType {

    CatalogMap<Column> m_columns;
    CatalogMap<Index> m_indexes;
    CatalogMap<Constraint> m_constraints;
    CatalogMap<Trigger> m_triggers;
    CatalogMap<ProcedureRef> m_triggerProcedures;
    boolean m_isreplicated;
    int m_estimatedtuplecount;
    CatalogMap<MaterializedViewInfo> m_views;
    boolean m_systable;
    boolean m_mapreduce;
    boolean m_evictable;
    boolean m_isStream;
    boolean m_isWindow;
    boolean m_isRows;
    String m_streamName = new String();
    int m_size;
    int m_slide;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_columns = new CatalogMap<Column>(catalog, this, path + "/" + "columns", Column.class);
        m_childCollections.put("columns", m_columns);
        m_indexes = new CatalogMap<Index>(catalog, this, path + "/" + "indexes", Index.class);
        m_childCollections.put("indexes", m_indexes);
        m_constraints = new CatalogMap<Constraint>(catalog, this, path + "/" + "constraints", Constraint.class);
        m_childCollections.put("constraints", m_constraints);
        m_triggers = new CatalogMap<Trigger>(catalog, this, path + "/" + "triggers", Trigger.class);
        m_childCollections.put("triggers", m_triggers);
        m_triggerProcedures = new CatalogMap<ProcedureRef>(catalog, this, path + "/" + "triggerProcedures", ProcedureRef.class);
        m_childCollections.put("triggerProcedures", m_triggerProcedures);
        m_fields.put("isreplicated", m_isreplicated);
        m_fields.put("partitioncolumn", null);
        m_fields.put("estimatedtuplecount", m_estimatedtuplecount);
        m_views = new CatalogMap<MaterializedViewInfo>(catalog, this, path + "/" + "views", MaterializedViewInfo.class);
        m_childCollections.put("views", m_views);
        m_fields.put("materializer", null);
        m_fields.put("systable", m_systable);
        m_fields.put("mapreduce", m_mapreduce);
        m_fields.put("evictable", m_evictable);
        m_fields.put("isStream", m_isStream);
        m_fields.put("isWindow", m_isWindow);
        m_fields.put("isRows", m_isRows);
        m_fields.put("streamName", m_streamName);
        m_fields.put("size", m_size);
        m_fields.put("slide", m_slide);
    }

    public void update() {
        m_isreplicated = (Boolean) m_fields.get("isreplicated");
        m_estimatedtuplecount = (Integer) m_fields.get("estimatedtuplecount");
        m_systable = (Boolean) m_fields.get("systable");
        m_mapreduce = (Boolean) m_fields.get("mapreduce");
        m_evictable = (Boolean) m_fields.get("evictable");
        m_isStream = (Boolean) m_fields.get("isStream");
        m_isWindow = (Boolean) m_fields.get("isWindow");
        m_isRows = (Boolean) m_fields.get("isRows");
        m_streamName = (String) m_fields.get("streamName");
        m_size = (Integer) m_fields.get("size");
        m_slide = (Integer) m_fields.get("slide");
    }

    /** GETTER: The set of columns in the table */
    public CatalogMap<Column> getColumns() {
        return m_columns;
    }

    /** GETTER: The set of indexes on the columns in the table */
    public CatalogMap<Index> getIndexes() {
        return m_indexes;
    }

    /** GETTER: The set of constraints on the table */
    public CatalogMap<Constraint> getConstraints() {
        return m_constraints;
    }

    /** GETTER: The set of triggers for this table */
    public CatalogMap<Trigger> getTriggers() {
        return m_triggers;
    }

    /** GETTER: The set of frontend trigger procedures for this table"  */
    public CatalogMap<ProcedureRef> getTriggerprocedures() {
        return m_triggerProcedures;
    }

    /** GETTER: Is the table replicated? */
    public boolean getIsreplicated() {
        return m_isreplicated;
    }

    /** GETTER: On which column is the table horizontally partitioned */
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

    /** GETTER: A rough estimate of the number of tuples in the table; used for planning */
    public int getEstimatedtuplecount() {
        return m_estimatedtuplecount;
    }

    /** GETTER: Information about materialized views based on this table's content */
    public CatalogMap<MaterializedViewInfo> getViews() {
        return m_views;
    }

    /** GETTER: If this is a materialized view, this field stores the source table */
    public Table getMaterializer() {
        Object o = getField("materializer");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Table retval = (Table) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("materializer", retval);
            return retval;
        }
        return (Table) o;
    }

    /** GETTER: Is this table an internal system table? */
    public boolean getSystable() {
        return m_systable;
    }

    /** GETTER: Is this table a MapReduce transaction table? */
    public boolean getMapreduce() {
        return m_mapreduce;
    }

    /** GETTER: Can contents of this table be evicted by the anti-cache? */
    public boolean getEvictable() {
        return m_evictable;
    }

    /** GETTER: Is this table a Stream table? */
    public boolean getIsstream() {
        return m_isStream;
    }

    /** GETTER: Is this table a Window for Stream? */
    public boolean getIswindow() {
        return m_isWindow;
    }

    /** GETTER: Is this is a row based window or time based? */
    public boolean getIsrows() {
        return m_isRows;
    }

    /** GETTER: The window related stream name */
    public String getStreamname() {
        return m_streamName;
    }

    /** GETTER: The window size */
    public int getSize() {
        return m_size;
    }

    /** GETTER: The window slide"  */
    public int getSlide() {
        return m_slide;
    }

    /** SETTER: Is the table replicated? */
    public void setIsreplicated(boolean value) {
        m_isreplicated = value; m_fields.put("isreplicated", value);
    }

    /** SETTER: On which column is the table horizontally partitioned */
    public void setPartitioncolumn(Column value) {
        m_fields.put("partitioncolumn", value);
    }

    /** SETTER: A rough estimate of the number of tuples in the table; used for planning */
    public void setEstimatedtuplecount(int value) {
        m_estimatedtuplecount = value; m_fields.put("estimatedtuplecount", value);
    }

    /** SETTER: If this is a materialized view, this field stores the source table */
    public void setMaterializer(Table value) {
        m_fields.put("materializer", value);
    }

    /** SETTER: Is this table an internal system table? */
    public void setSystable(boolean value) {
        m_systable = value; m_fields.put("systable", value);
    }

    /** SETTER: Is this table a MapReduce transaction table? */
    public void setMapreduce(boolean value) {
        m_mapreduce = value; m_fields.put("mapreduce", value);
    }

    /** SETTER: Can contents of this table be evicted by the anti-cache? */
    public void setEvictable(boolean value) {
        m_evictable = value; m_fields.put("evictable", value);
    }

    /** SETTER: Is this table a Stream table? */
    public void setIsstream(boolean value) {
        m_isStream = value; m_fields.put("isStream", value);
    }

    /** SETTER: Is this table a Window for Stream? */
    public void setIswindow(boolean value) {
        m_isWindow = value; m_fields.put("isWindow", value);
    }

    /** SETTER: Is this is a row based window or time based? */
    public void setIsrows(boolean value) {
        m_isRows = value; m_fields.put("isRows", value);
    }

    /** SETTER: The window related stream name */
    public void setStreamname(String value) {
        m_streamName = value; m_fields.put("streamName", value);
    }

    /** SETTER: The window size */
    public void setSize(int value) {
        m_size = value; m_fields.put("size", value);
    }

    /** SETTER: The window slide"  */
    public void setSlide(int value) {
        m_slide = value; m_fields.put("slide", value);
    }

}
