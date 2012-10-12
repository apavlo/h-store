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

#ifndef CATALOG_CONFLICTPAIR_H_
#define CATALOG_CONFLICTPAIR_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class Statement;
class TableRef;
/**
 * A pair of Statements that have a conflict
 */
class ConflictPair : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<ConflictPair>;

protected:
    ConflictPair(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogType* m_statement0;
    CatalogType* m_statement1;
    CatalogMap<TableRef> m_tables;
    bool m_alwaysConflicting;
    int32_t m_conflictType;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~ConflictPair();

    /** GETTER: The source Statement */
    const Statement * statement0() const;
    /** GETTER: The destination Statement */
    const Statement * statement1() const;
    /** GETTER: The list of tables that caused this conflict */
    const CatalogMap<TableRef> & tables() const;
    /** GETTER: If true, then this ConflictPair will always cause a conflict */
    bool alwaysConflicting() const;
    /** GETTER: Type of conflict (ConflictType) */
    int32_t conflictType() const;
};

} // namespace catalog

#endif //  CATALOG_CONFLICTPAIR_H_
