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
#include "streamref.h"
#include "catalog.h"
#include "stream.h"

using namespace catalog;
using namespace std;

StreamRef::StreamRef(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["stream"] = value;
}

StreamRef::~StreamRef() {
}

void StreamRef::update() {
    m_stream = m_fields["stream"].typeValue;
}

CatalogType * StreamRef::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * StreamRef::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool StreamRef::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

const Stream * StreamRef::stream() const {
    return dynamic_cast<Stream*>(m_stream);
}

