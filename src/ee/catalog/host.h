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

#ifndef CATALOG_HOST_H_
#define CATALOG_HOST_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

/**
 * A single host participating in the cluster
 */
class Host : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Host>;

protected:
    Host(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_id;
    std::string m_ipaddr;
    int32_t m_num_cpus;
    int32_t m_corespercpu;
    int32_t m_threadspercore;
    int32_t m_memory;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Host();

    /** GETTER: Unique host id */
    int32_t id() const;
    /** GETTER: The ip address or hostname of the host */
    const std::string & ipaddr() const;
    /** GETTER: The max number of cpus on this host */
    int32_t num_cpus() const;
    /** GETTER: The number of cores per CPU on this host */
    int32_t corespercpu() const;
    /** GETTER: The number of threads per cores on this host */
    int32_t threadspercore() const;
    /** GETTER: The amount of memory in bytes that this host has */
    int32_t memory() const;
};

} // namespace catalog

#endif //  CATALOG_HOST_H_
