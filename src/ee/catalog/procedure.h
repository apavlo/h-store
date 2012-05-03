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

#ifndef CATALOG_PROCEDURE_H_
#define CATALOG_PROCEDURE_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class UserRef;
class GroupRef;
class Table;
class Column;
class AuthProgram;
class Statement;
class ProcParameter;
/**
 * A stored procedure (transaction) in the system
 */
class Procedure : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Procedure>;

protected:
    Procedure(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_id;
    std::string m_classname;
    CatalogMap<UserRef> m_authUsers;
    CatalogMap<GroupRef> m_authGroups;
    bool m_readonly;
    bool m_singlepartition;
    bool m_everysite;
    bool m_systemproc;
    bool m_mapreduce;
    bool m_prefetchable;
    bool m_deferrable;
    std::string m_mapInputQuery;
    std::string m_mapEmitTable;
    std::string m_reduceInputQuery;
    std::string m_reduceEmitTable;
    bool m_hasjava;
    CatalogType* m_partitiontable;
    CatalogType* m_partitioncolumn;
    int32_t m_partitionparameter;
    CatalogMap<AuthProgram> m_authPrograms;
    CatalogMap<Statement> m_statements;
    CatalogMap<ProcParameter> m_parameters;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Procedure();

    /** GETTER: Unique identifier for this Procedure. Allows for faster look-ups */
    int32_t id() const;
    /** GETTER: The full class name for the Java class for this procedure */
    const std::string & classname() const;
    /** GETTER: Users authorized to invoke this procedure */
    const CatalogMap<UserRef> & authUsers() const;
    /** GETTER: Groups authorized to invoke this procedure */
    const CatalogMap<GroupRef> & authGroups() const;
    /** GETTER: Can the stored procedure modify data */
    bool readonly() const;
    /** GETTER: Does the stored procedure need data on more than one partition? */
    bool singlepartition() const;
    /** GETTER: Does the stored procedure as a single procedure txn at every site? */
    bool everysite() const;
    /** GETTER: Is this procedure an internal system procedure? */
    bool systemproc() const;
    /** GETTER: Is this procedure a Map/Reduce procedure? */
    bool mapreduce() const;
    /** GETTER: Does this Procedure have Statements can be pre-fetched for distributed transactions? */
    bool prefetchable() const;
    /** GETTER: Does this Procedure have at least one deferrable Statement? */
    bool deferrable() const;
    /** GETTER: The name of the query that gets executed and fed into the Map function */
    const std::string & mapInputQuery() const;
    /** GETTER: The name of the table that the Map function will store data in */
    const std::string & mapEmitTable() const;
    /** GETTER: The name of the query that gets executed and fed into the Reduce function */
    const std::string & reduceInputQuery() const;
    /** GETTER: The name of the table that the Reduce function will store data in */
    const std::string & reduceEmitTable() const;
    /** GETTER: Is this a full java stored procedure or is it just a single stmt? */
    bool hasjava() const;
    /** GETTER: Which table contains the partition column for this procedure? */
    const Table * partitiontable() const;
    /** GETTER: Which column in the partitioned table is this procedure mapped on? */
    const Column * partitioncolumn() const;
    /** GETTER: Which parameter identifies the partition column? */
    int32_t partitionparameter() const;
    /** GETTER: The set of authorized programs for this procedure (users) */
    const CatalogMap<AuthProgram> & authPrograms() const;
    /** GETTER: The set of SQL statements this procedure may call */
    const CatalogMap<Statement> & statements() const;
    /** GETTER: The set of parameters to this stored procedure */
    const CatalogMap<ProcParameter> & parameters() const;
};

} // namespace catalog

#endif //  CATALOG_PROCEDURE_H_
