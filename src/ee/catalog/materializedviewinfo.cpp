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
#include "materializedviewinfo.h"
#include "catalog.h"
#include "table.h"
#include "columnref.h"

using namespace catalog;
using namespace std;

MaterializedViewInfo::MaterializedViewInfo(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_groupbycols(catalog, this, path + "/" + "groupbycols")
{
    CatalogValue value;
    m_fields["dest"] = value;
    m_childCollections["groupbycols"] = &m_groupbycols;
    m_fields["predicate"] = value;
    m_fields["verticalpartition"] = value;
    m_fields["sqltext"] = value;
}

MaterializedViewInfo::~MaterializedViewInfo() {
    std::map<std::string, ColumnRef*>::const_iterator columnref_iter = m_groupbycols.begin();
    while (columnref_iter != m_groupbycols.end()) {
        delete columnref_iter->second;
        columnref_iter++;
    }
    m_groupbycols.clear();

}

void MaterializedViewInfo::update() {
    m_dest = m_fields["dest"].typeValue;
    m_predicate = m_fields["predicate"].strValue.c_str();
    m_verticalpartition = m_fields["verticalpartition"].intValue;
    m_sqltext = m_fields["sqltext"].strValue.c_str();
}

CatalogType * MaterializedViewInfo::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("groupbycols") == 0) {
        CatalogType *exists = m_groupbycols.get(childName);
        if (exists)
            return NULL;
        return m_groupbycols.add(childName);
    }
    return NULL;
}

CatalogType * MaterializedViewInfo::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("groupbycols") == 0)
        return m_groupbycols.get(childName);
    return NULL;
}

bool MaterializedViewInfo::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("groupbycols") == 0) {
        return m_groupbycols.remove(childName);
    }
    return false;
}

const Table * MaterializedViewInfo::dest() const {
    return dynamic_cast<Table*>(m_dest);
}

const CatalogMap<ColumnRef> & MaterializedViewInfo::groupbycols() const {
    return m_groupbycols;
}

const string & MaterializedViewInfo::predicate() const {
    return m_predicate;
}

bool MaterializedViewInfo::verticalpartition() const {
    return m_verticalpartition;
}

const string & MaterializedViewInfo::sqltext() const {
    return m_sqltext;
}

