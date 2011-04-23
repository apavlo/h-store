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
 * A logical, replicate-able partition
 */
public class Partition extends CatalogType {

    int m_id;
    int m_dtxn_port;
    int m_engine_port;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        this.addField("id", m_id);
        this.addField("dtxn_port", m_dtxn_port);
        this.addField("engine_port", m_engine_port);
    }

    void update() {
        m_id = (Integer) m_fields.get("id");
        m_dtxn_port = (Integer) m_fields.get("dtxn_port");
        m_engine_port = (Integer) m_fields.get("engine_port");
    }

    /** GETTER: Partition id */
    public int getId() {
        return m_id;
    }

    /** GETTER: Port used for DTXN.Coordinator to communicate to the ProtoEngine */
    public int getDtxn_port() {
        return m_dtxn_port;
    }

    /** GETTER: Port used for HStoreSite to communicate to the ProtoEngine */
    public int getEngine_port() {
        return m_engine_port;
    }

    /** SETTER: Partition id */
    public void setId(int value) {
        m_id = value; m_fields.put("id", value);
    }

    /** SETTER: Port used for DTXN.Coordinator to communicate to the ProtoEngine */
    public void setDtxn_port(int value) {
        m_dtxn_port = value; m_fields.put("dtxn_port", value);
    }

    /** SETTER: Port used for HStoreSite to communicate to the ProtoEngine */
    public void setEngine_port(int value) {
        m_engine_port = value; m_fields.put("engine_port", value);
    }

}
