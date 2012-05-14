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

#ifndef CATALOG_HARDWARETHREAD_H_
#define CATALOG_HARDWARETHREAD_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class Partition;
/**
 * A single hardware thread that is part of a core
 */
class HardwareThread : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<HardwareThread>;

protected:
    HardwareThread(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogMap<Partition> m_partitions;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~HardwareThread();

    /** GETTER: The partitions assigned to this hardware thread */
    const CatalogMap<Partition> & partitions() const;
};

} // namespace catalog

#endif //  CATALOG_HARDWARETHREAD_H_
