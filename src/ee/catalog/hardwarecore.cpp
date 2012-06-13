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
#include "hardwarecore.h"
#include "catalog.h"
#include "hardwarethread.h"

using namespace catalog;
using namespace std;

HardwareCore::HardwareCore(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_threads(catalog, this, path + "/" + "threads")
{
    CatalogValue value;
    m_childCollections["threads"] = &m_threads;
}

HardwareCore::~HardwareCore() {
    std::map<std::string, HardwareThread*>::const_iterator hardwarethread_iter = m_threads.begin();
    while (hardwarethread_iter != m_threads.end()) {
        delete hardwarethread_iter->second;
        hardwarethread_iter++;
    }
    m_threads.clear();

}

void HardwareCore::update() {
}

CatalogType * HardwareCore::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("threads") == 0) {
        CatalogType *exists = m_threads.get(childName);
        if (exists)
            return NULL;
        return m_threads.add(childName);
    }
    return NULL;
}

CatalogType * HardwareCore::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("threads") == 0)
        return m_threads.get(childName);
    return NULL;
}

bool HardwareCore::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("threads") == 0) {
        return m_threads.remove(childName);
    }
    return false;
}

const CatalogMap<HardwareThread> & HardwareCore::threads() const {
    return m_threads;
}

