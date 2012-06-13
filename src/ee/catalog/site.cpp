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

#include <cassert>
#include "site.h"
#include "catalog.h"
#include "host.h"
#include "partition.h"

using namespace catalog;
using namespace std;

Site::Site(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_partitions(catalog, this, path + "/" + "partitions")
{
    CatalogValue value;
    m_fields["id"] = value;
    m_fields["host"] = value;
    m_childCollections["partitions"] = &m_partitions;
    m_fields["isUp"] = value;
    m_fields["messenger_port"] = value;
    m_fields["proc_port"] = value;
}

Site::~Site() {
    std::map<std::string, Partition*>::const_iterator partition_iter = m_partitions.begin();
    while (partition_iter != m_partitions.end()) {
        delete partition_iter->second;
        partition_iter++;
    }
    m_partitions.clear();

}

void Site::update() {
    m_id = m_fields["id"].intValue;
    m_host = m_fields["host"].typeValue;
    m_isUp = m_fields["isUp"].intValue;
    m_messenger_port = m_fields["messenger_port"].intValue;
    m_proc_port = m_fields["proc_port"].intValue;
}

CatalogType * Site::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("partitions") == 0) {
        CatalogType *exists = m_partitions.get(childName);
        if (exists)
            return NULL;
        return m_partitions.add(childName);
    }
    return NULL;
}

CatalogType * Site::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("partitions") == 0)
        return m_partitions.get(childName);
    return NULL;
}

bool Site::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("partitions") == 0) {
        return m_partitions.remove(childName);
    }
    return false;
}

int32_t Site::id() const {
    return m_id;
}

const Host * Site::host() const {
    return dynamic_cast<Host*>(m_host);
}

const CatalogMap<Partition> & Site::partitions() const {
    return m_partitions;
}

bool Site::isUp() const {
    return m_isUp;
}

int32_t Site::messenger_port() const {
    return m_messenger_port;
}

int32_t Site::proc_port() const {
    return m_proc_port;
}

