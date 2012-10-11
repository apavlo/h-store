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
#include "conflictset.h"
#include "catalog.h"
#include "procedure.h"
#include "conflictpair.h"

using namespace catalog;
using namespace std;

ConflictSet::ConflictSet(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_readWriteConflicts(catalog, this, path + "/" + "readWriteConflicts"), m_writeWriteConflicts(catalog, this, path + "/" + "writeWriteConflicts")
{
    CatalogValue value;
    m_fields["procedure"] = value;
    m_childCollections["readWriteConflicts"] = &m_readWriteConflicts;
    m_childCollections["writeWriteConflicts"] = &m_writeWriteConflicts;
}

ConflictSet::~ConflictSet() {
    std::map<std::string, ConflictPair*>::const_iterator conflictpair_iter = m_readWriteConflicts.begin();
    while (conflictpair_iter != m_readWriteConflicts.end()) {
        delete conflictpair_iter->second;
        conflictpair_iter++;
    }
    m_readWriteConflicts.clear();

    conflictpair_iter = m_writeWriteConflicts.begin();
    while (conflictpair_iter != m_writeWriteConflicts.end()) {
        delete conflictpair_iter->second;
        conflictpair_iter++;
    }
    m_writeWriteConflicts.clear();

}

void ConflictSet::update() {
    m_procedure = m_fields["procedure"].typeValue;
}

CatalogType * ConflictSet::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("readWriteConflicts") == 0) {
        CatalogType *exists = m_readWriteConflicts.get(childName);
        if (exists)
            return NULL;
        return m_readWriteConflicts.add(childName);
    }
    if (collectionName.compare("writeWriteConflicts") == 0) {
        CatalogType *exists = m_writeWriteConflicts.get(childName);
        if (exists)
            return NULL;
        return m_writeWriteConflicts.add(childName);
    }
    return NULL;
}

CatalogType * ConflictSet::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("readWriteConflicts") == 0)
        return m_readWriteConflicts.get(childName);
    if (collectionName.compare("writeWriteConflicts") == 0)
        return m_writeWriteConflicts.get(childName);
    return NULL;
}

bool ConflictSet::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("readWriteConflicts") == 0) {
        return m_readWriteConflicts.remove(childName);
    }
    if (collectionName.compare("writeWriteConflicts") == 0) {
        return m_writeWriteConflicts.remove(childName);
    }
    return false;
}

const Procedure * ConflictSet::procedure() const {
    return dynamic_cast<Procedure*>(m_procedure);
}

const CatalogMap<ConflictPair> & ConflictSet::readWriteConflicts() const {
    return m_readWriteConflicts;
}

const CatalogMap<ConflictPair> & ConflictSet::writeWriteConflicts() const {
    return m_writeWriteConflicts;
}

