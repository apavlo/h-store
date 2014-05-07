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
 * A single host participating in the cluster
 */
public class Host extends CatalogType {

    int m_id;
    String m_ipaddr = new String();
    int m_num_cpus;
    int m_corespercpu;
    int m_threadspercore;
    int m_memory;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("id", m_id);
        m_fields.put("ipaddr", m_ipaddr);
        m_fields.put("num_cpus", m_num_cpus);
        m_fields.put("corespercpu", m_corespercpu);
        m_fields.put("threadspercore", m_threadspercore);
        m_fields.put("memory", m_memory);
    }

    public void update() {
        m_id = (Integer) m_fields.get("id");
        m_ipaddr = (String) m_fields.get("ipaddr");
        m_num_cpus = (Integer) m_fields.get("num_cpus");
        m_corespercpu = (Integer) m_fields.get("corespercpu");
        m_threadspercore = (Integer) m_fields.get("threadspercore");
        m_memory = (Integer) m_fields.get("memory");
    }

    /** GETTER: Unique host id */
    public int getId() {
        return m_id;
    }

    /** GETTER: The ip address or hostname of the host */
    public String getIpaddr() {
        return m_ipaddr;
    }

    /** GETTER: The max number of cpus on this host */
    public int getNum_cpus() {
        return m_num_cpus;
    }

    /** GETTER: The number of cores per CPU on this host */
    public int getCorespercpu() {
        return m_corespercpu;
    }

    /** GETTER: The number of threads per cores on this host */
    public int getThreadspercore() {
        return m_threadspercore;
    }

    /** GETTER: The amount of memory in bytes that this host has */
    public int getMemory() {
        return m_memory;
    }

    /** SETTER: Unique host id */
    public void setId(int value) {
        m_id = value; m_fields.put("id", value);
    }

    /** SETTER: The ip address or hostname of the host */
    public void setIpaddr(String value) {
        m_ipaddr = value; m_fields.put("ipaddr", value);
    }

    /** SETTER: The max number of cpus on this host */
    public void setNum_cpus(int value) {
        m_num_cpus = value; m_fields.put("num_cpus", value);
    }

    /** SETTER: The number of cores per CPU on this host */
    public void setCorespercpu(int value) {
        m_corespercpu = value; m_fields.put("corespercpu", value);
    }

    /** SETTER: The number of threads per cores on this host */
    public void setThreadspercore(int value) {
        m_threadspercore = value; m_fields.put("threadspercore", value);
    }

    /** SETTER: The amount of memory in bytes that this host has */
    public void setMemory(int value) {
        m_memory = value; m_fields.put("memory", value);
    }

}
