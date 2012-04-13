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

#include <cassert>
#include "host.h"
#include "catalog.h"
#include "hardwarecpu.h"

using namespace catalog;
using namespace std;

Host::Host(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_cpus(catalog, this, path + "/" + "cpus")
{
    CatalogValue value;
    m_fields["id"] = value;
    m_fields["ipaddr"] = value;
    m_fields["num_cpus"] = value;
    m_fields["corespercpu"] = value;
    m_fields["threadspercore"] = value;
    m_fields["memory"] = value;
    m_childCollections["cpus"] = &m_cpus;
}

void Host::update() {
    m_id = m_fields["id"].intValue;
    m_ipaddr = m_fields["ipaddr"].strValue.c_str();
    m_num_cpus = m_fields["num_cpus"].intValue;
    m_corespercpu = m_fields["corespercpu"].intValue;
    m_threadspercore = m_fields["threadspercore"].intValue;
    m_memory = m_fields["memory"].intValue;
}

CatalogType * Host::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("cpus") == 0) {
        CatalogType *exists = m_cpus.get(childName);
        if (exists)
            return NULL;
        return m_cpus.add(childName);
    }
    return NULL;
}

CatalogType * Host::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("cpus") == 0)
        return m_cpus.get(childName);
    return NULL;
}

void Host::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("cpus") == 0)
        return m_cpus.remove(childName);
}

int32_t Host::id() const {
    return m_id;
}

const string & Host::ipaddr() const {
    return m_ipaddr;
}

int32_t Host::num_cpus() const {
    return m_num_cpus;
}

int32_t Host::corespercpu() const {
    return m_corespercpu;
}

int32_t Host::threadspercore() const {
    return m_threadspercore;
}

int32_t Host::memory() const {
    return m_memory;
}

const CatalogMap<HardwareCPU> & Host::cpus() const {
    return m_cpus;
}

