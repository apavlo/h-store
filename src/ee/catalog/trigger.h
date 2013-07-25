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

#ifndef CATALOG_TRIGGER_H_
#define CATALOG_TRIGGER_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class Table;
class Statement;
/**
 * Trigger objects on a table, with a statement attached
 */
class Trigger : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Trigger>;

protected:
    Trigger(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_id;
    CatalogType* m_sourceTable;
    int32_t m_triggerType;
    bool m_forEach;
    CatalogType* m_stmt;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Trigger();

    /** GETTER: Unique identifier for this Trigger. Allows for faster look-ups */
    int32_t id() const;
    /** GETTER: Table on which the trigger is placed. */
    const Table * sourceTable() const;
    /** GETTER: Insert / Update / Delete */
    int32_t triggerType() const;
    /** GETTER: Is this for each tuple, or each statement */
    bool forEach() const;
    /** GETTER: What to execute when this trigger is activated"			 */
    const Statement * stmt() const;
};

} // namespace catalog

#endif //  CATALOG_TRIGGER_H_
