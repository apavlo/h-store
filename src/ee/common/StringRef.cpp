/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

#include "StringRef.h"

#include "Pool.hpp"

using namespace voltdb;
using namespace std;

StringRef*
StringRef::create(size_t size, Pool* dataPool)
{
    StringRef* retval;
    if (dataPool != NULL)
    {
        retval =
            new(dataPool->allocate(sizeof(StringRef))) StringRef(size, dataPool);
    }
    else
    {
        retval = new StringRef(size);
    }
    return retval;
}

void
StringRef::destroy(StringRef* sref)
{
    delete sref;
}

StringRef::StringRef(size_t size)
{
    m_size = size + sizeof(StringRef*);
    m_tempPool = false;
    m_stringPtr = new char[m_size];
    //printf("m_stringPtr: %p\n", m_stringPtr);
    setBackPtr();
}

StringRef::StringRef(std::size_t size, Pool* dataPool)
{
    m_tempPool = true;
    m_stringPtr =
        reinterpret_cast<char*>(dataPool->allocate(size + sizeof(StringRef*)));
    setBackPtr();
}

StringRef::~StringRef()
{
    if (!m_tempPool)
    {
        delete[] m_stringPtr;
    }
}

char*
StringRef::get()
{
    return m_stringPtr + sizeof(StringRef*);
}

const char*
StringRef::get() const
{
    return m_stringPtr + sizeof(StringRef*);
}

void
StringRef::updateStringLocation(void* location)
{
    m_stringPtr = reinterpret_cast<char*>(location);
}

void
StringRef::setBackPtr()
{
    StringRef** backptr = reinterpret_cast<StringRef**>(m_stringPtr);
    *backptr = this;
}
