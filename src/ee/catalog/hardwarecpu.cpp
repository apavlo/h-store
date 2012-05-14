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
#include "hardwarecpu.h"
#include "catalog.h"
#include "hardwarecore.h"

using namespace catalog;
using namespace std;

HardwareCPU::HardwareCPU(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_cores(catalog, this, path + "/" + "cores")
{
    CatalogValue value;
    m_childCollections["cores"] = &m_cores;
}

HardwareCPU::~HardwareCPU() {
    std::map<std::string, HardwareCore*>::const_iterator hardwarecore_iter = m_cores.begin();
    while (hardwarecore_iter != m_cores.end()) {
        delete hardwarecore_iter->second;
        hardwarecore_iter++;
    }
    m_cores.clear();

}

void HardwareCPU::update() {
}

CatalogType * HardwareCPU::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("cores") == 0) {
        CatalogType *exists = m_cores.get(childName);
        if (exists)
            return NULL;
        return m_cores.add(childName);
    }
    return NULL;
}

CatalogType * HardwareCPU::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("cores") == 0)
        return m_cores.get(childName);
    return NULL;
}

bool HardwareCPU::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("cores") == 0) {
        return m_cores.remove(childName);
    }
    return false;
}

const CatalogMap<HardwareCore> & HardwareCPU::cores() const {
    return m_cores;
}

