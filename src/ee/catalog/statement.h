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

#ifndef CATALOG_STATEMENT_H_
#define CATALOG_STATEMENT_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class StmtParameter;
class Column;
class PlanFragment;
/**
 * A parameterized SQL statement embedded in a stored procedure
 */
class Statement : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Statement>;

protected:
    Statement(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_id;
    std::string m_sqltext;
    int32_t m_querytype;
    bool m_readonly;
    bool m_singlepartition;
    bool m_replicatedtabledml;
    bool m_replicatedonly;
    bool m_batched;
    bool m_secondaryindex;
    bool m_prefetchable;
    bool m_deferrable;
    int32_t m_paramnum;
    CatalogMap<StmtParameter> m_parameters;
    CatalogMap<Column> m_output_columns;
    bool m_has_singlesited;
    CatalogMap<PlanFragment> m_fragments;
    std::string m_exptree;
    std::string m_fullplan;
    bool m_has_multisited;
    CatalogMap<PlanFragment> m_ms_fragments;
    std::string m_ms_exptree;
    std::string m_ms_fullplan;
    int32_t m_cost;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Statement();

    /** GETTER: Unique identifier for this Procedure. Allows for faster look-ups */
    int32_t id() const;
    /** GETTER: The text of the sql statement */
    const std::string & sqltext() const;
    int32_t querytype() const;
    /** GETTER: Can the statement modify any data? */
    bool readonly() const;
    /** GETTER: Does the statement only use data on one partition? */
    bool singlepartition() const;
    /** GETTER: Should the result of this statememt be divided by partition count before returned */
    bool replicatedtabledml() const;
    /** GETTER: Does this statement only access replicated tables? */
    bool replicatedonly() const;
    bool batched() const;
    bool secondaryindex() const;
    /** GETTER: Whether this query should be examined for pre-fetching if Procedure is being executed as a distributed transaction */
    bool prefetchable() const;
    /** GETTER: Whether this query does not need to executed immediately in this transaction */
    bool deferrable() const;
    int32_t paramnum() const;
    /** GETTER: The set of parameters to this SQL statement */
    const CatalogMap<StmtParameter> & parameters() const;
    /** GETTER: The set of columns in the output table */
    const CatalogMap<Column> & output_columns() const;
    /** GETTER: Whether this statement has a single-sited query plan */
    bool has_singlesited() const;
    /** GETTER: The set of plan fragments used to execute this statement */
    const CatalogMap<PlanFragment> & fragments() const;
    /** GETTER: A serialized representation of the original expression tree */
    const std::string & exptree() const;
    /** GETTER: A serialized representation of the un-fragmented plan */
    const std::string & fullplan() const;
    /** GETTER: Whether this statement has a multi-sited query plan */
    bool has_multisited() const;
    /** GETTER: The set of multi-sited plan fragments used to execute this statement */
    const CatalogMap<PlanFragment> & ms_fragments() const;
    /** GETTER: A serialized representation of the multi-sited query plan */
    const std::string & ms_exptree() const;
    /** GETTER: A serialized representation of the multi-sited query plan */
    const std::string & ms_fullplan() const;
    /** GETTER: The cost of this plan measured in arbitrary units */
    int32_t cost() const;
};

} // namespace catalog

#endif //  CATALOG_STATEMENT_H_
