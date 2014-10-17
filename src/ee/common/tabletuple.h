/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef HSTORETABLETUPLE_H
#define HSTORETABLETUPLE_H

#include "common/common.h"
#include "common/TupleSchema.h"
#include "common/ValuePeeker.hpp"
#include "common/FatalException.hpp"
#include "common/ExportSerializeIo.h"
#include <ostream>

#include <iostream>

class CopyOnWriteTest_TestTableTupleFlags;
class TableTupleTest_MarkAsEvicted;

namespace voltdb {
    
/*
 
 The tuple header is of the following structures:
 
 (a). No anti-caching
 -----------------------------------
 |  flags (1 byte)  |  tuple data  |
 -----------------------------------
 
 (b). Anti-Caching with single-linked list
 ---------------------------------------------------------------
 |  flags (1 byte)  |  "next" tuple id (4 bytes) | tuple data  |
 ---------------------------------------------------------------
 
 (c). Anti-Caching with double-linked list
 ---------------------------------------------------------------------------------------------------
 |  flags (1 byte)  |  "previous" tuple id  (4 bytes)  |  "next" tuple id (4 bytes)  | tuple data  |
 ---------------------------------------------------------------------------------------------------
 
 (d). Anti-Caching with timestamps
 ----------------------------------------------------------
 |  flags (1 byte)  |  time stamp (4 bytes) | tuple data  |
 ----------------------------------------------------------

 */

#ifdef ANTICACHE
    #ifdef ANTICACHE_TIMESTAMPS
    	#define TUPLE_HEADER_SIZE 5
	#else
    	#ifdef ANTICACHE_REVERSIBLE_LRU
        	#define TUPLE_HEADER_SIZE 9
    	#else
        	#define TUPLE_HEADER_SIZE 5
    	#endif
	#endif
#else
    #define TUPLE_HEADER_SIZE 1
#endif

#define DELETED_MASK 1
#define DIRTY_MASK 2
#define MIGRATED_MASK 4
#define EVICTED_MASK 8

class TableColumn;

class TableTuple {
    friend class TableFactory;
    friend class Table;
    friend class TempTable;
    friend class EvictedTable;
    friend class PersistentTable;
    friend class PersistentTableUndoDeleteAction;
    friend class PersistentTableUndoUpdateAction;
    friend class CopyOnWriteIterator;
    friend class CopyOnWriteContext;
    friend class ::CopyOnWriteTest_TestTableTupleFlags;
    friend class ::TableTupleTest_MarkAsEvicted;
    template<std::size_t keySize> friend class IntsKey;
    template<std::size_t keySize> friend class GenericKey;

public:
    /** Initialize a tuple unassociated with a table (bad idea... dangerous) */
    explicit TableTuple();

    /** Setup the tuple given a table */
    TableTuple(const TableTuple &rhs);

    /** Setup the tuple given a schema */
    TableTuple(const TupleSchema *schema);

    /** Setup the tuple given the specified data location and schema **/
    TableTuple(char *data, const voltdb::TupleSchema *schema);

    /** Assignment operator */
    TableTuple& operator=(const TableTuple &rhs);

    /**
     * Set the tuple to point toward a given address in a table's
     * backing store
     */
    inline void move(void *address) {
        assert(m_schema);
        m_data = reinterpret_cast<char*> (address);
    }

    inline void moveNoHeader(void *address) {
        assert(m_schema);
        // isActive() and all the other methods expect a header
        m_data = reinterpret_cast<char*> (address) - TUPLE_HEADER_SIZE;
    }

    // Used to wrap read only tuples in indexing code. TODO Remove
    // constedeness from indexing code so this cast isn't necessary.
    inline void moveToReadOnlyTuple(const void *address) {
        assert(m_schema);
        assert(address);
        //Necessary to move the pointer back TUPLE_HEADER_SIZE
        // artificially because Tuples used as keys for indexes do not
        // have the header.
        m_data = reinterpret_cast<char*>(const_cast<void*>(address)) - TUPLE_HEADER_SIZE;
    }

    /** Get the address of this tuple in the table's backing store */
    inline char* address() const {
        return m_data;
    }

    /** Return the number of columns in this tuple */
    inline int sizeInValues() const {
        return m_schema->columnCount();
    }

    /**
        Determine the maximum number of bytes when serialized for Export.
        Excludes the bytes required by the row header (which includes
        the null bit indicators) and ignores the width of metadata cols.
    */
    size_t maxExportSerializationSize() const {
        size_t bytes = 0;
        int cols = sizeInValues();
        for (int i = 0; i < cols; ++i) {
            switch (getType(i)) {
              case VALUE_TYPE_TINYINT:
              case VALUE_TYPE_SMALLINT:
              case VALUE_TYPE_INTEGER:
              case VALUE_TYPE_BIGINT:
              case VALUE_TYPE_TIMESTAMP:
              case VALUE_TYPE_DOUBLE:
                bytes += sizeof (int64_t);
                break;

              case VALUE_TYPE_DECIMAL:
                // decimals serialized in ascii as
                // 32 bits of length + max prec digits + radix pt + sign
                bytes += sizeof (int32_t) + NValue::kMaxDecPrec + 1 + 1;
                break;

              case VALUE_TYPE_VARCHAR:
              case VALUE_TYPE_VARBINARY:
                  // 32 bit length preceding value and
                  // actual character data without null string terminator.
                  if (!getNValue(i).isNull())
                  {
                      bytes += (sizeof (int32_t) +
                                ValuePeeker::peekObjectLength(getNValue(i)));
                  }
                break;
              default:
                // let caller handle this error
                throwFatalException("Unknown ValueType found during Export serialization.");
                return (size_t)0;
            }
        }
        return bytes;
    }

    // Return the amount of memory allocated for non-inlined objects
    size_t getNonInlinedMemorySize() const
    {
        size_t bytes = 0;
        int cols = sizeInValues();
        // fast-path for no inlined cols
        if (m_schema->getUninlinedObjectColumnCount() != 0)
        {
            for (int i = 0; i < cols; ++i)
            {
                // peekObjectLength is unhappy with non-varchar
            	if ((getType(i) == VALUE_TYPE_VARCHAR || (getType(i) == VALUE_TYPE_VARBINARY))  &&
                    !m_schema->columnIsInlined(i))
                {
                    if (!getNValue(i).isNull())
                    {
                        bytes += (sizeof(int32_t) +
                                  ValuePeeker::
                                  peekObjectLength(getNValue(i)));
                    }
                }
            }
        }
        return bytes;
    }

    void setNValue(const int idx, voltdb::NValue value);

    /*
     * Version of setSlimValue that will allocate space to copy
     * strings that can't be inlined rather then copying the
     * pointer. Used when setting a SlimValue that will go into
     * permanent storage in a persistent table.  It is also possible
     * to provide NULL for stringPool in which case the strings will
     * be allocated on the heap.
     */
    void setNValueAllocateForObjectCopies(const int idx, voltdb::NValue value,
                                             Pool *dataPool);

    /** How long is a tuple? */
    inline int tupleLength() const {
        return m_schema->tupleLength() + TUPLE_HEADER_SIZE;
    }

    /** Is the tuple deleted or active? */
    inline bool isActive() const {
        return (*(reinterpret_cast<const char*> (m_data)) & DELETED_MASK) == 0 ? true : false;
    }

    /** Is the tuple deleted or active? */
    inline bool isDirty() const {
        return (*(reinterpret_cast<const char*> (m_data)) & DIRTY_MASK) == 0 ? false : true;
    }

    inline bool isEvicted() const {
        return (*(reinterpret_cast<const char*> (m_data)) & EVICTED_MASK) == 0 ? false : true;
    }

    /** Is the column value null? */
    inline bool isNull(const int idx) const {
        return getNValue(idx).isNull();
    }

    inline bool isNullTuple() const {
        return m_data == NULL;
    }

    /** Get the type of a particular column in the tuple */
    inline ValueType getType(int idx) const {
        return m_schema->columnType(idx);
    }
    
#ifdef ANTICACHE
	#ifndef ANTICACHE_TIMESTAMPS
    	inline uint32_t getNextTupleInChain() {
        	uint32_t tuple_id = 0;
	        memcpy(&tuple_id, m_data+TUPLE_HEADER_SIZE-4, 4);
   	     
    	    return tuple_id; 
    	}
    
	    inline void setNextTupleInChain(uint32_t next) {
    	    memcpy(m_data+TUPLE_HEADER_SIZE-4, &next, 4);

    	}
    
    	inline uint32_t getPreviousTupleInChain() {
        	uint32_t tuple_id = 0;
        	memcpy(&tuple_id, m_data+TUPLE_HEADER_SIZE-8, 4);
        	return tuple_id;
    	}
    
    	inline void setPreviousTupleInChain(uint32_t prev) {
        	memcpy(m_data+TUPLE_HEADER_SIZE-8, &prev, 4);
    	}

	#else
    	inline uint32_t getTimeStamp() {
        	uint32_t time_stamp = 0;
	        memcpy(&time_stamp, m_data+TUPLE_HEADER_SIZE-4, 4);
   	     
    	    return time_stamp; 
    	}

        static uint64_t rdtsc() {
            uint32_t lo, hi;
            __asm__ __volatile__ ("rdtsc": "=a" (lo), "=d" (hi));
            return (((uint64_t)hi << 32) | lo);
        }
    
        inline void setTimeStamp() {
            uint32_t current_time = (uint32_t)(rdtsc() >> 32);
            memcpy(m_data+TUPLE_HEADER_SIZE-4, &current_time, 4);
        }

        inline void setColdTimeStamp() {
            uint32_t cold_time = 0;
            memcpy(m_data+TUPLE_HEADER_SIZE-4, &cold_time, 4);
        }
#endif
#endif

        inline uint32_t getTupleID()
        {
            uint32_t tuple_id; 
            memcpy(&tuple_id, m_data+TUPLE_HEADER_SIZE-4, 4);  

            return tuple_id; 
        }

        inline void setTupleID(uint32_t tuple_id)
        {
            memcpy(m_data+TUPLE_HEADER_SIZE-4, &tuple_id, 4); 
        }

        /** Get the value of a specified column (const) */
        //not performant because it has to check the schema to see how to
        //return the SlimValue.
        inline const NValue getNValue(const int idx) const {
            assert(m_schema);
            assert(m_data);
            assert(idx < m_schema->columnCount());

            //assert(isActive());
            const voltdb::ValueType columnType = m_schema->columnType(idx);
            //VOLT_DEBUG("column type: %d\n", (int)columnType);
            const char* dataPtr = getDataPtr(idx);
            const bool isInlined = m_schema->columnIsInlined(idx);
            return NValue::deserializeFromTupleStorage( dataPtr, columnType, isInlined);
        }

        inline const voltdb::TupleSchema* getSchema() const {
            return m_schema;
        }

        /** Print out a human readable description of this tuple */
        std::string debug(const std::string& tableName) const;
        std::string debugNoHeader() const;

        /** Copy values from one tuple into another (uses memcpy) */
        // verify assumptions for copy. do not use at runtime (expensive)
        bool compatibleForCopy(const TableTuple &source);
        void copyForPersistentInsert(const TableTuple &source, Pool *pool = NULL);
        void copyForPersistentUpdate(const TableTuple &source, Pool *pool = NULL);
        void copy(const TableTuple &source);

        /** this does set NULL in addition to clear string count.*/
        void setAllNulls();

        bool equals(const TableTuple &other) const;
        bool equalsNoSchemaCheck(const TableTuple &other) const;

        int compare(const TableTuple &other) const;

        void deserializeFrom(voltdb::SerializeInput &tupleIn, Pool *stringPool);
        void serializeTo(voltdb::SerializeOutput &output);
        void serializeToExport(voltdb::ExportSerializeOutput &io,
                int colOffset, uint8_t *nullArray);

        void serializeWithHeaderTo(voltdb::SerializeOutput &output);
        int64_t deserializeWithHeaderFrom(voltdb::SerializeInput &tupleIn);

        void freeObjectColumns();

        //#ifdef ARIES
        void freeObjectColumnsOfLogTuple();
        //#endif

    size_t hashCode(size_t seed) const;
    size_t hashCode() const;
    inline void setEvictedTrue() {
        *(reinterpret_cast<char*> (m_data)) |= static_cast<char>(EVICTED_MASK);
    }
    inline void setEvictedFalse() {
        *(reinterpret_cast<char*> (m_data)) &= static_cast<char>(~EVICTED_MASK);
    }
    inline void setDeletedFalse() {
        // treat the first "value" as a boolean flag
        *(reinterpret_cast<char*> (m_data)) &= static_cast<char>(~DELETED_MASK);
    }

protected:
        inline void setDeletedTrue() {
            // treat the first "value" as a boolean flag
            *(reinterpret_cast<char*> (m_data)) |= static_cast<char>(DELETED_MASK);
        }
        inline void setDirtyTrue() {
            // treat the first "value" as a boolean flag
            *(reinterpret_cast<char*> (m_data)) |= static_cast<char>(DIRTY_MASK);
        }
        inline void setDirtyFalse() {
            // treat the first "value" as a boolean flag
            *(reinterpret_cast<char*> (m_data)) &= static_cast<char>(~DIRTY_MASK);
        }


        /** The types of the columns in the tuple */
        const TupleSchema *m_schema;

        /**
         * The column data, padded at the front by 8 bytes
         * representing whether the tuple is active or deleted
         */
        char *m_data;
private:
        inline char* getDataPtr(const int idx) {
            assert(m_schema);
            assert(m_data);
            return &m_data[m_schema->columnOffset(idx) + TUPLE_HEADER_SIZE];
        }

        inline const char* getDataPtr(const int idx) const {
            assert(m_schema);
            assert(m_data);
            return &m_data[m_schema->columnOffset(idx) + TUPLE_HEADER_SIZE];
        }
};

inline TableTuple::TableTuple() :
    m_schema(NULL), m_data(NULL) {
    }

inline TableTuple::TableTuple(const TableTuple &rhs) :
    m_schema(rhs.m_schema), m_data(rhs.m_data) {
    }

inline TableTuple::TableTuple(const TupleSchema *schema) :
    m_schema(schema), m_data(NULL) {
        assert (m_schema);
    }

/** Setup the tuple given the specified data location and schema **/
inline TableTuple::TableTuple(char *data, const voltdb::TupleSchema *schema) {
    assert(data);
    assert(schema);
    m_data = data;
    m_schema = schema;
}

inline TableTuple& TableTuple::operator=(const TableTuple &rhs) {
    m_schema = rhs.m_schema;
    m_data = rhs.m_data;
    return *this;
}

/** Copy scalars by value and non-scalars (non-inlined strings, decimals) by
  reference from a slim value in to this tuple. */
inline void TableTuple::setNValue(const int idx, voltdb::NValue value) {
    assert(m_schema);
    assert(m_data);
    const ValueType type = m_schema->columnType(idx);
    value = value.castAs(type);
    const bool isInlined = m_schema->columnIsInlined(idx);
    char *dataPtr = getDataPtr(idx);
    const int32_t columnLength = m_schema->columnLength(idx);
    value.serializeToTupleStorage(dataPtr, isInlined, columnLength);
}

/* Copy strictly by value from slimvalue into this tuple */
inline void TableTuple::setNValueAllocateForObjectCopies(const int idx,
        voltdb::NValue value,
        Pool *dataPool)
{
    assert(m_schema);
    assert(m_data);
    //assert(isActive())
    const ValueType type = m_schema->columnType(idx);
    value = value.castAs(type);
    const bool isInlined = m_schema->columnIsInlined(idx);
    char *dataPtr = getDataPtr(idx);
    const int32_t columnLength = m_schema->columnLength(idx);
    value.serializeToTupleStorageAllocateForObjects(dataPtr, isInlined,
            columnLength, dataPool);
}

/*
 * With a persistent insert the copy should do an allocation for all uninlinable strings
 */
inline void TableTuple::copyForPersistentInsert(const voltdb::TableTuple &source, Pool *pool) {
    assert(m_schema);
    assert(source.m_schema);
    assert(source.m_data);
    assert(m_data);

    const bool allowInlinedObjects = m_schema->allowInlinedObjects();
    const TupleSchema *sourceSchema = source.m_schema;
    const bool oAllowInlinedObjects = sourceSchema->allowInlinedObjects();
    const uint16_t uninlineableObjectColumnCount = m_schema->getUninlinedObjectColumnCount();

#ifndef NDEBUG
    if(!compatibleForCopy(source)) {
        std::ostringstream message;
        message << "src  tuple: " << source.debug("") << std::endl;
        message << "src schema: " << source.m_schema->debug() << std::endl;
        message << "dest schema: " << m_schema->debug() << std::endl;
        throwFatalException( "%s", message.str().c_str());
    }
#endif

    if (allowInlinedObjects == oAllowInlinedObjects) {
        /*
         * The source and target tuple have the same policy WRT to
         * inlining strings. A memcpy can be used to speed the process
         * up for all columns that are not uninlineable strings.
         */
        if (uninlineableObjectColumnCount > 0) {
            // copy the data AND the isActive flag
            ::memcpy(m_data, source.m_data, m_schema->tupleLength() + TUPLE_HEADER_SIZE);
            /*
             * Copy each uninlined string column doing an allocation for string copies.
             */
            for (uint16_t ii = 0; ii < uninlineableObjectColumnCount; ii++) {
                const uint16_t uinlineableObjectColumnIndex =
                    m_schema->getUninlinedObjectColumnInfoIndex(ii);
                setNValueAllocateForObjectCopies(uinlineableObjectColumnIndex,
                        source.getNValue(uinlineableObjectColumnIndex),
                        pool);
            }
            m_data[0] = source.m_data[0];
        } else {
            // copy the data AND the isActive flag
            ::memcpy(m_data, source.m_data, m_schema->tupleLength() + TUPLE_HEADER_SIZE);
        }
    } else {
        // Can't copy the string ptr from the other tuple if the string
        // is inlined into the tuple
        assert(!(!allowInlinedObjects && oAllowInlinedObjects));
        const uint16_t columnCount = m_schema->columnCount();
        for (uint16_t ii = 0; ii < columnCount; ii++) {
            setNValueAllocateForObjectCopies(ii, source.getNValue(ii), pool);
        }

        m_data[0] = source.m_data[0];
    }
}

/*
 * With a persistent update the copy should only do an allocation for
 * a string if the source and destination pointers are different.
 */
inline void TableTuple::copyForPersistentUpdate(const TableTuple &source, Pool *pool) {
    assert(m_schema);
    assert(m_schema == source.m_schema);
    const int columnCount = m_schema->columnCount();
    const uint16_t uninlineableObjectColumnCount = m_schema->getUninlinedObjectColumnCount();
    /*
     * The source and target tuple have the same policy WRT to
     * inlining strings because a TableTuple used for updating a
     * persistent table uses the same schema as the persistent table.
     */
    if (uninlineableObjectColumnCount > 0) {
        uint16_t uninlineableObjectColumnIndex = 0;
        uint16_t nextUninlineableObjectColumnInfoIndex = m_schema->getUninlinedObjectColumnInfoIndex(0);
        /*
         * Copy each column doing an allocation for string
         * copies. Compare the source and target pointer to see if it
         * is changed in this update. If it is changed then free the
         * old string and copy/allocate the new one from the source.
         */
        for (uint16_t ii = 0; ii < columnCount; ii++) {
            if (ii == nextUninlineableObjectColumnInfoIndex) {
                const char *mPtr = *reinterpret_cast<char* const*>(getDataPtr(ii));
                const char *oPtr = *reinterpret_cast<char* const*>(source.getDataPtr(ii));
                if (mPtr != oPtr) {
                    // Make a copy of the input string. Don't need to
                    // delete the old string because that will be done
                    // by the UndoAction for the update.
                    setNValueAllocateForObjectCopies(ii, source.getNValue(ii), pool);
                }
                uninlineableObjectColumnIndex++;
                if (uninlineableObjectColumnIndex < uninlineableObjectColumnCount) {
                    nextUninlineableObjectColumnInfoIndex =
                        m_schema->getUninlinedObjectColumnInfoIndex(uninlineableObjectColumnIndex);
                } else {
                    nextUninlineableObjectColumnInfoIndex = 0;
                }
            } else {
                setNValueAllocateForObjectCopies(ii, source.getNValue(ii), pool);
            }
        }
        m_data[0] = source.m_data[0];
    } else {
        // copy the data AND the isActive flag
        ::memcpy(m_data, source.m_data, m_schema->tupleLength() + TUPLE_HEADER_SIZE);
    }
}

inline void TableTuple::copy(const TableTuple &source) {
    assert(m_schema);
    assert(source.m_schema);
    assert(source.m_data);
    assert(m_data);

    const uint16_t columnCount = m_schema->columnCount();
    const bool allowInlinedObjects = m_schema->allowInlinedObjects();
    const TupleSchema *sourceSchema = source.m_schema;
    const bool oAllowInlinedObjects = sourceSchema->allowInlinedObjects();

#ifndef NDEBUG
    if(!compatibleForCopy(source)) {
        std::ostringstream message;
        message << "src  tuple: " << source.debug("") << std::endl;
        message << "src schema: " << source.m_schema->debug() << std::endl;
        message << "dest schema: " << m_schema->debug() << std::endl;
        throwFatalException("%s", message.str().c_str());
    }
#endif

    if (allowInlinedObjects == oAllowInlinedObjects) {
        // copy the data AND the isActive flag
        ::memcpy(m_data, source.m_data, m_schema->tupleLength() + TUPLE_HEADER_SIZE);
    } else {
        // Can't copy the string ptr from the other tuple if the
        // string is inlined into the tuple
        assert(!(!allowInlinedObjects && oAllowInlinedObjects));
        for (uint16_t ii = 0; ii < columnCount; ii++) {
            setNValue(ii, source.getNValue(ii));
        }
        m_data[0] = source.m_data[0];
    }
}

inline void TableTuple::deserializeFrom(voltdb::SerializeInput &tupleIn, Pool *dataPool) {
    assert(m_schema);
    assert(m_data);

    tupleIn.readInt();
    for (int j = 0; j < m_schema->columnCount(); ++j) {
        const ValueType type = m_schema->columnType(j);
        /**
         * Hack hack. deserializeFrom is only called when we serialize
         * and deserialize tables. The serialization format for
         * Strings/Objects in a serialized table happens to have the
         * same in memory representation as the Strings/Objects in a
         * tabletuple. The goal here is to wrap the serialized
         * representation of the value in an NValue and then serialize
         * that into the tuple from the NValue. This makes it possible
         * to push more value specific functionality out of
         * TableTuple. The memory allocation will be performed when
         * serializing to tuple storage.
         */
        const bool isInlined = m_schema->columnIsInlined(j);
        char *dataPtr = getDataPtr(j);
        const int32_t columnLength = m_schema->columnLength(j);
        NValue::deserializeFrom(tupleIn, type, dataPtr, isInlined, columnLength, dataPool);
    }
}

inline int64_t TableTuple::deserializeWithHeaderFrom(voltdb::SerializeInput &tupleIn) {

    int64_t total_bytes_deserialized = 0;

    assert(m_schema);
    assert(m_data);

    tupleIn.readInt();  // read in the tuple size, discard 
    total_bytes_deserialized+=sizeof(int);

    memcpy(m_data, tupleIn.getRawPointer(TUPLE_HEADER_SIZE), TUPLE_HEADER_SIZE);    
    total_bytes_deserialized += TUPLE_HEADER_SIZE;

    for (int j = 0; j < m_schema->columnCount(); ++j) {
        const ValueType type = m_schema->columnType(j);
        /**
         * Hack hack. deserializeFrom is only called when we serialize
         * and deserialize tables. The serialization format for
         * Strings/Objects in a serialized table happens to have the
         * same in memory representation as the Strings/Objects in a
         * tabletuple. The goal here is to wrap the serialized
         * representation of the value in an NValue and then serialize
         * that into the tuple from the NValue. This makes it possible
         * to push more value specific functionality out of
         * TableTuple. The memory allocation will be performed when
         * serializing to tuple storage.
         */
        const bool isInlined = m_schema->columnIsInlined(j);
        char *dataPtr = getDataPtr(j);
        const int32_t columnLength = m_schema->columnLength(j);
        total_bytes_deserialized+= NValue::deserializeFrom(tupleIn, type, dataPtr, isInlined, columnLength, NULL);
    }
    return total_bytes_deserialized;
    //    for (int j = 0; j < m_schema->columnCount(); ++j) {
    //        ValueType type = m_schema->columnType(j);
    //        
    //        switch (type) {
    //            case VALUE_TYPE_BIGINT:
    //            case VALUE_TYPE_TIMESTAMP:
    //                
    //                *reinterpret_cast<int64_t*>(m_data+total_bytes_deserialized) = tupleIn.readLong();
    //                total_bytes_deserialized += 8;
    //                break;
    //                
    //            case VALUE_TYPE_TINYINT:
    //                
    //                *reinterpret_cast<int8_t*>(m_data+total_bytes_deserialized) = tupleIn.readByte();
    //                total_bytes_deserialized += 1;
    //                break;
    //                
    //            case VALUE_TYPE_SMALLINT:
    //                
    //                *reinterpret_cast<int16_t*>(m_data+total_bytes_deserialized) = tupleIn.readShort();
    //                total_bytes_deserialized += 2;
    //                break;
    //                
    //            case VALUE_TYPE_INTEGER:
    //                
    //                *reinterpret_cast<int32_t*>(m_data+total_bytes_deserialized) = tupleIn.readInt();
    //                total_bytes_deserialized += 4;
    //                break;
    //                
    //            case VALUE_TYPE_DOUBLE:
    //                
    //                *reinterpret_cast<double* >(m_data+total_bytes_deserialized) = tupleIn.readDouble();
    //                total_bytes_deserialized += sizeof(double);
    //                break;
    //                
    //            case VALUE_TYPE_VARCHAR: {
    //                int32_t length = tupleIn.readInt();  // read in the length of this serialized string
    //                
    //                memcpy(m_data+total_bytes_deserialized, &length, 4);
    //                total_bytes_deserialized += 4;
    //
    //                if(!m_schema->columnIsInlined(j))
    //                {
    //                    VOLT_INFO("Dserializing an non in-line string of length %d.", length);
    //                }
    //                
    //                memcpy(m_data+total_bytes_deserialized, tupleIn.getRawPointer(length), length);
    //                total_bytes_deserialized += length;
    //
    //                break;
    //            }
    //            case VALUE_TYPE_DECIMAL: {
    //                int64_t *longStorage = reinterpret_cast<int64_t*>(m_data+total_bytes_deserialized);
    //                
    //                total_bytes_deserialized += 8;
    //
    //                //Reverse order for Java BigDecimal BigEndian
    //                longStorage[1] = tupleIn.readLong();
    //                longStorage[0] = tupleIn.readLong();
    //                break;
    //            }
    //            default:
    //                char message[128];
    //                snprintf(message, 128, "NValue::deserializeFrom() unrecognized type '%d'",
    //                         type);
    //                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
    //                                              message);
    //        }
    //    }
}

inline void TableTuple::serializeWithHeaderTo(voltdb::SerializeOutput &output) {

    assert(m_schema);
    assert(m_data); 

    size_t start = output.position(); 
    output.writeInt(0);  // reserve first 4 bytes for the total tuple size

    output.writeBytes(m_data, TUPLE_HEADER_SIZE);

    for (int j = 0; j < m_schema->columnCount(); ++j) {
        //int fieldStart = output.position();
        NValue value = getNValue(j);
        value.serializeTo(output);
    }

    int32_t serialized_size = static_cast<int32_t>(output.position() - start - sizeof(int32_t));

    // write the length of the tuple
    output.writeIntAt(start, serialized_size);
}


inline void TableTuple::serializeTo(voltdb::SerializeOutput &output) {
    size_t start = output.reserveBytes(4);

    for (int j = 0; j < m_schema->columnCount(); ++j) {
        //int fieldStart = output.position();
        NValue value = getNValue(j);
        value.serializeTo(output);
    }

    // write the length of the tuple
    output.writeIntAt(start, static_cast<int32_t>(output.position() - start - sizeof(int32_t)));
}



inline
    void
TableTuple::serializeToExport(ExportSerializeOutput &io,
        int colOffset, uint8_t *nullArray)
{
    int columnCount = sizeInValues();
    for (int i = 0; i < columnCount; i++) {
        // NULL doesn't produce any bytes for the NValue
        // Handle it here to consolidate manipulation of
        // the nullarray.
        if (isNull(i)) {
            // turn on i'th bit of nullArray
            int byte = (colOffset + i) >> 3;
            int bit = (colOffset + i) % 8;
            int mask = 0x80 >> bit;
            nullArray[byte] = (uint8_t)(nullArray[byte] | mask);
            continue;
        }
        getNValue(i).serializeToExport(io);
    }
}

inline bool TableTuple::equals(const TableTuple &other) const {
    if (!m_schema->equals(other.m_schema)) {
        return false;
    }
    return equalsNoSchemaCheck(other);
}

inline bool TableTuple::equalsNoSchemaCheck(const TableTuple &other) const {
    for (int ii = 0; ii < m_schema->columnCount(); ii++) {
        const NValue lhs = getNValue(ii);
        const NValue rhs = other.getNValue(ii);
        if (lhs.op_notEquals(rhs).isTrue()) {
            return false;
        }
    }
    return true;
}

inline void TableTuple::setAllNulls() {
    assert(m_schema);
    assert(m_data);

    for (int ii = 0; ii < m_schema->columnCount(); ++ii) {
        NValue value = NValue::getNullValue(m_schema->columnType(ii));
        setNValue(ii, value);
    }
}

inline int TableTuple::compare(const TableTuple &other) const {
    const int columnCount = m_schema->columnCount();
    int diff;
    for (int ii = 0; ii < columnCount; ii++) {
        const NValue lhs = getNValue(ii);
        const NValue rhs = other.getNValue(ii);
        diff = lhs.compare(rhs);
        if (diff) {
            return diff;
        }
    }
    return 0;
}

inline size_t TableTuple::hashCode(size_t seed) const {
    const int columnCount = m_schema->columnCount();
    for (int i = 0; i < columnCount; i++) {
        const NValue value = getNValue(i);
        value.hashCombine(seed);
    }
    return seed;
}

inline size_t TableTuple::hashCode() const {
    size_t seed = 0;
    return hashCode(seed);
}

/**
 * Release to the heap any memory allocated for any uninlined columns.
 */
inline void TableTuple::freeObjectColumns() {
    const uint16_t unlinlinedColumnCount = m_schema->getUninlinedObjectColumnCount();
    for (int ii = 0; ii < unlinlinedColumnCount; ii++) {
        getNValue(m_schema->getUninlinedObjectColumnInfoIndex(ii)).free();
    }
}

//#ifdef ARIES

inline void TableTuple::freeObjectColumnsOfLogTuple() {
    const uint16_t unlinlinedColumnCount = m_schema->getUninlinedObjectColumnCount();

    for (int ii = 0; ii < unlinlinedColumnCount; ii++) {
        getNValue(m_schema->getUninlinedObjectColumnInfoIndex(ii)).freeLogTupleVal();
    }
}

//#endif

/**
 * Hasher for use with boost::unordered_map and similar
 */
struct TableTupleHasher : std::unary_function<TableTuple, std::size_t>
{
    /** Generate a 64-bit number for the key value */
    inline size_t operator()(TableTuple tuple) const
    {
        return tuple.hashCode();
    }
};

/**
 * Equality operator for use with boost::unrodered_map and similar
 */
class TableTupleEqualityChecker {
    public:
        inline bool operator()(const TableTuple lhs, const TableTuple rhs) const {
            return lhs.equalsNoSchemaCheck(rhs);
        }
};

}

#endif
