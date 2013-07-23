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

#ifndef CATALOG_STREAM_H_
#define CATALOG_STREAM_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class Column;
class Index;
/**
 * A stream (relation) in the database
 */
class Stream : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Stream>;

protected:
    Stream(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogMap<Column> m_columns;
    CatalogMap<Index> m_indexes;
    CatalogType* m_partitioncolumn;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Stream();

    /** GETTER: The set of columns in the stream */
    const CatalogMap<Column> & columns() const;
    /** GETTER: The set of indexes on the columns in the stream */
    const CatalogMap<Index> & indexes() const;
    /** GETTER: On which column is the stream horizontally partitioned */
    const Column * partitioncolumn() const;
};

} // namespace catalog

#endif //  CATALOG_STREAM_H_
