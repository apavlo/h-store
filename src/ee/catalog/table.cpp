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
#include "table.h"
#include "catalog.h"
#include "index.h"
#include "trigger.h"
#include "column.h"
#include "materializedviewinfo.h"
#include "constraint.h"
#include "table.h"
#include "procedureref.h"

using namespace catalog;
using namespace std;

Table::Table(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_columns(catalog, this, path + "/" + "columns"), m_indexes(catalog, this, path + "/" + "indexes"), m_constraints(catalog, this, path + "/" + "constraints"), m_triggers(catalog, this, path + "/" + "triggers"), m_triggerProcedures(catalog, this, path + "/" + "triggerProcedures"), m_views(catalog, this, path + "/" + "views")
{
    CatalogValue value;
    m_childCollections["columns"] = &m_columns;
    m_childCollections["indexes"] = &m_indexes;
    m_childCollections["constraints"] = &m_constraints;
    m_childCollections["triggers"] = &m_triggers;
    m_childCollections["triggerProcedures"] = &m_triggerProcedures;
    m_fields["isreplicated"] = value;
    m_fields["partitioncolumn"] = value;
    m_fields["estimatedtuplecount"] = value;
    m_childCollections["views"] = &m_views;
    m_fields["materializer"] = value;
    m_fields["systable"] = value;
    m_fields["mapreduce"] = value;
    m_fields["evictable"] = value;
    m_fields["isStream"] = value;
    m_fields["isWindow"] = value;
    m_fields["isRows"] = value;
    m_fields["streamName"] = value;
    m_fields["size"] = value;
    m_fields["slide"] = value;
}

Table::~Table() {
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

    std::map<std::string, Constraint*>::const_iterator constraint_iter = m_constraints.begin();
    while (constraint_iter != m_constraints.end()) {
        delete constraint_iter->second;
        constraint_iter++;
    }
    m_constraints.clear();

    std::map<std::string, Trigger*>::const_iterator trigger_iter = m_triggers.begin();
    while (trigger_iter != m_triggers.end()) {
        delete trigger_iter->second;
        trigger_iter++;
    }
    m_triggers.clear();

    std::map<std::string, ProcedureRef*>::const_iterator procedureref_iter = m_triggerProcedures.begin();
    while (procedureref_iter != m_triggerProcedures.end()) {
        delete procedureref_iter->second;
        procedureref_iter++;
    }
    m_triggerProcedures.clear();

    std::map<std::string, MaterializedViewInfo*>::const_iterator materializedviewinfo_iter = m_views.begin();
    while (materializedviewinfo_iter != m_views.end()) {
        delete materializedviewinfo_iter->second;
        materializedviewinfo_iter++;
    }
    m_views.clear();

}

void Table::update() {
    m_isreplicated = m_fields["isreplicated"].intValue;
    m_partitioncolumn = m_fields["partitioncolumn"].typeValue;
    m_estimatedtuplecount = m_fields["estimatedtuplecount"].intValue;
    m_materializer = m_fields["materializer"].typeValue;
    m_systable = m_fields["systable"].intValue;
    m_mapreduce = m_fields["mapreduce"].intValue;
    m_evictable = m_fields["evictable"].intValue;
    m_isStream = m_fields["isStream"].intValue;
    m_isWindow = m_fields["isWindow"].intValue;
    m_isRows = m_fields["isRows"].intValue;
    m_streamName = m_fields["streamName"].strValue.c_str();
    m_size = m_fields["size"].intValue;
    m_slide = m_fields["slide"].intValue;
}

CatalogType * Table::addChild(const std::string &collectionName, const std::string &childName) {
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
    if (collectionName.compare("constraints") == 0) {
        CatalogType *exists = m_constraints.get(childName);
        if (exists)
            return NULL;
        return m_constraints.add(childName);
    }
    if (collectionName.compare("triggers") == 0) {
        CatalogType *exists = m_triggers.get(childName);
        if (exists)
            return NULL;
        return m_triggers.add(childName);
    }
    if (collectionName.compare("triggerProcedures") == 0) {
        CatalogType *exists = m_triggerProcedures.get(childName);
        if (exists)
            return NULL;
        return m_triggerProcedures.add(childName);
    }
    if (collectionName.compare("views") == 0) {
        CatalogType *exists = m_views.get(childName);
        if (exists)
            return NULL;
        return m_views.add(childName);
    }
    return NULL;
}

CatalogType * Table::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("columns") == 0)
        return m_columns.get(childName);
    if (collectionName.compare("indexes") == 0)
        return m_indexes.get(childName);
    if (collectionName.compare("constraints") == 0)
        return m_constraints.get(childName);
    if (collectionName.compare("triggers") == 0)
        return m_triggers.get(childName);
    if (collectionName.compare("triggerProcedures") == 0)
        return m_triggerProcedures.get(childName);
    if (collectionName.compare("views") == 0)
        return m_views.get(childName);
    return NULL;
}

bool Table::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("columns") == 0) {
        return m_columns.remove(childName);
    }
    if (collectionName.compare("indexes") == 0) {
        return m_indexes.remove(childName);
    }
    if (collectionName.compare("constraints") == 0) {
        return m_constraints.remove(childName);
    }
    if (collectionName.compare("triggers") == 0) {
        return m_triggers.remove(childName);
    }
    if (collectionName.compare("triggerProcedures") == 0) {
        return m_triggerProcedures.remove(childName);
    }
    if (collectionName.compare("views") == 0) {
        return m_views.remove(childName);
    }
    return false;
}

const CatalogMap<Column> & Table::columns() const {
    return m_columns;
}

const CatalogMap<Index> & Table::indexes() const {
    return m_indexes;
}

const CatalogMap<Constraint> & Table::constraints() const {
    return m_constraints;
}

const CatalogMap<Trigger> & Table::triggers() const {
    return m_triggers;
}

const CatalogMap<ProcedureRef> & Table::triggerProcedures() const {
    return m_triggerProcedures;
}

bool Table::isreplicated() const {
    return m_isreplicated;
}

const Column * Table::partitioncolumn() const {
    return dynamic_cast<Column*>(m_partitioncolumn);
}

int32_t Table::estimatedtuplecount() const {
    return m_estimatedtuplecount;
}

const CatalogMap<MaterializedViewInfo> & Table::views() const {
    return m_views;
}

const Table * Table::materializer() const {
    return dynamic_cast<Table*>(m_materializer);
}

bool Table::systable() const {
    return m_systable;
}

bool Table::mapreduce() const {
    return m_mapreduce;
}

bool Table::evictable() const {
    return m_evictable;
}

bool Table::isStream() const {
    return m_isStream;
}

bool Table::isWindow() const {
    return m_isWindow;
}

bool Table::isRows() const {
    return m_isRows;
}

const string & Table::streamName() const {
    return m_streamName;
}

int32_t Table::size() const {
    return m_size;
}

int32_t Table::slide() const {
    return m_slide;
}

