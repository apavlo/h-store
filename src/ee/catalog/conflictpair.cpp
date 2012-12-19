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
#include "conflictpair.h"
#include "catalog.h"
#include "statement.h"
#include "tableref.h"

using namespace catalog;
using namespace std;

ConflictPair::ConflictPair(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_tables(catalog, this, path + "/" + "tables")
{
    CatalogValue value;
    m_fields["statement0"] = value;
    m_fields["statement1"] = value;
    m_childCollections["tables"] = &m_tables;
    m_fields["alwaysConflicting"] = value;
    m_fields["conflictType"] = value;
}

ConflictPair::~ConflictPair() {
    std::map<std::string, TableRef*>::const_iterator tableref_iter = m_tables.begin();
    while (tableref_iter != m_tables.end()) {
        delete tableref_iter->second;
        tableref_iter++;
    }
    m_tables.clear();

}

void ConflictPair::update() {
    m_statement0 = m_fields["statement0"].typeValue;
    m_statement1 = m_fields["statement1"].typeValue;
    m_alwaysConflicting = m_fields["alwaysConflicting"].intValue;
    m_conflictType = m_fields["conflictType"].intValue;
}

CatalogType * ConflictPair::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("tables") == 0) {
        CatalogType *exists = m_tables.get(childName);
        if (exists)
            return NULL;
        return m_tables.add(childName);
    }
    return NULL;
}

CatalogType * ConflictPair::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("tables") == 0)
        return m_tables.get(childName);
    return NULL;
}

bool ConflictPair::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("tables") == 0) {
        return m_tables.remove(childName);
    }
    return false;
}

const Statement * ConflictPair::statement0() const {
    return dynamic_cast<Statement*>(m_statement0);
}

const Statement * ConflictPair::statement1() const {
    return dynamic_cast<Statement*>(m_statement1);
}

const CatalogMap<TableRef> & ConflictPair::tables() const {
    return m_tables;
}

bool ConflictPair::alwaysConflicting() const {
    return m_alwaysConflicting;
}

int32_t ConflictPair::conflictType() const {
    return m_conflictType;
}

