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
#include "hardwarethread.h"
#include "catalog.h"
#include "partition.h"

using namespace catalog;
using namespace std;

HardwareThread::HardwareThread(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_partitions(catalog, this, path + "/" + "partitions")
{
    CatalogValue value;
    m_childCollections["partitions"] = &m_partitions;
}

HardwareThread::~HardwareThread() {
    std::map<std::string, Partition*>::const_iterator partition_iter = m_partitions.begin();
    while (partition_iter != m_partitions.end()) {
        delete partition_iter->second;
        partition_iter++;
    }
    m_partitions.clear();

}

void HardwareThread::update() {
}

CatalogType * HardwareThread::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("partitions") == 0) {
        CatalogType *exists = m_partitions.get(childName);
        if (exists)
            return NULL;
        return m_partitions.add(childName);
    }
    return NULL;
}

CatalogType * HardwareThread::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("partitions") == 0)
        return m_partitions.get(childName);
    return NULL;
}

bool HardwareThread::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("partitions") == 0) {
        return m_partitions.remove(childName);
    }
    return false;
}

const CatalogMap<Partition> & HardwareThread::partitions() const {
    return m_partitions;
}

