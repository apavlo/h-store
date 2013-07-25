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
#include "trigger.h"
#include "catalog.h"
#include "statement.h"
#include "table.h"

using namespace catalog;
using namespace std;

Trigger::Trigger(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["id"] = value;
    m_fields["sourceTable"] = value;
    m_fields["triggerType"] = value;
    m_fields["forEach"] = value;
    m_fields["stmt"] = value;
}

Trigger::~Trigger() {
}

void Trigger::update() {
    m_id = m_fields["id"].intValue;
    m_sourceTable = m_fields["sourceTable"].typeValue;
    m_triggerType = m_fields["triggerType"].intValue;
    m_forEach = m_fields["forEach"].intValue;
    m_stmt = m_fields["stmt"].typeValue;
}

CatalogType * Trigger::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * Trigger::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool Trigger::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

int32_t Trigger::id() const {
    return m_id;
}

const Table * Trigger::sourceTable() const {
    return dynamic_cast<Table*>(m_sourceTable);
}

int32_t Trigger::triggerType() const {
    return m_triggerType;
}

bool Trigger::forEach() const {
    return m_forEach;
}

const Statement * Trigger::stmt() const {
    return dynamic_cast<Statement*>(m_stmt);
}

