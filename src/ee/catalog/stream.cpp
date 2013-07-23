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
#include "stream.h"
#include "catalog.h"
#include "index.h"
#include "column.h"

using namespace catalog;
using namespace std;

Stream::Stream(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_columns(catalog, this, path + "/" + "columns"), m_indexes(catalog, this, path + "/" + "indexes")
{
    CatalogValue value;
    m_childCollections["columns"] = &m_columns;
    m_childCollections["indexes"] = &m_indexes;
    m_fields["partitioncolumn"] = value;
}

Stream::~Stream() {
    std::map<std::string, Column*>::const_iterator column_iter = m_columns.begin();
    while (column_iter != m_columns.end()) {
        delete column_iter->second;
        column_iter++;
    }
    m_columns.clear();

    std::map<std::string, Index*>::const_iterator index_iter = m_indexes.begin();
    while (index_iter != m_indexes.end()) {
        delete index_iter->second;
        index_iter++;
    }
    m_indexes.clear();

}

void Stream::update() {
    m_partitioncolumn = m_fields["partitioncolumn"].typeValue;
}

CatalogType * Stream::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("columns") == 0) {
        CatalogType *exists = m_columns.get(childName);
        if (exists)
            return NULL;
        return m_columns.add(childName);
    }
    if (collectionName.compare("indexes") == 0) {
        CatalogType *exists = m_indexes.get(childName);
        if (exists)
            return NULL;
        return m_indexes.add(childName);
    }
    return NULL;
}

CatalogType * Stream::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("columns") == 0)
        return m_columns.get(childName);
    if (collectionName.compare("indexes") == 0)
        return m_indexes.get(childName);
    return NULL;
}

bool Stream::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("columns") == 0) {
        return m_columns.remove(childName);
    }
    if (collectionName.compare("indexes") == 0) {
        return m_indexes.remove(childName);
    }
    return false;
}

const CatalogMap<Column> & Stream::columns() const {
    return m_columns;
}

const CatalogMap<Index> & Stream::indexes() const {
    return m_indexes;
}

const Column * Stream::partitioncolumn() const {
    return dynamic_cast<Column*>(m_partitioncolumn);
}

