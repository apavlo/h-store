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
#include "partition.h"
#include "catalog.h"

using namespace catalog;
using namespace std;

Partition::Partition(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["id"] = value;
    m_fields["dtxn_port"] = value;
    m_fields["engine_port"] = value;
}

void Partition::update() {
    m_id = m_fields["id"].intValue;
    m_dtxn_port = m_fields["dtxn_port"].intValue;
    m_engine_port = m_fields["engine_port"].intValue;
}

CatalogType * Partition::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * Partition::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

void Partition::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
}

int32_t Partition::id() const {
    return m_id;
}

int32_t Partition::dtxn_port() const {
    return m_dtxn_port;
}

int32_t Partition::engine_port() const {
    return m_engine_port;
}

