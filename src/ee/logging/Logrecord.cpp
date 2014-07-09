/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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


/*
 * Logrecord.cpp
 *
 *      Author: nirmeshmalviya
 */

#include <iostream>
#include <vector>

#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "Logrecord.h"

#define MAX_TUPLE_PKEY_LEN			1024	// the pkey could be bigger, we assume its not
#define MAX_RAW_TUPLE_LEN			4096	// XXX: basis?
#define MAX_NUM_COL_LEN				256		// A volt table can have 64 columns, so 64*4 actually matches up just fine
#define MAX_TABLENAME_LEN			20		// seems like a decent upperbound

#define MAX_RECORD_TUPDATA_LEN 		(2*MAX_RAW_TUPLE_LEN + MAX_TUPLE_PKEY_LEN + MAX_TABLENAME_LEN + MAX_NUM_COL_LEN + 50)

using namespace voltdb;

LogRecord::LogRecord(double timestamp, Logrec_type_t type, Logrec_category_t category,
		double prevLsn, int64_t xid, int32_t execSiteId, const std::string& tableName,
		TableTuple *primaryKey, int32_t numCols, std::vector<int32_t> *colIndices,
		TableTuple* beforeImage, TableTuple *afterImage)
		: lsn(timestamp),
		type(type),
		category(category),
		prevLsn(prevLsn),
		xid(xid),
		execSiteId(execSiteId),
		tableName(tableName),
		primaryKey(primaryKey),
		numColumnsModified(numCols),
		beforeImage(beforeImage),
		afterImage(afterImage)
{
	if ((this->type == T_UPDATE) && (numColumnsModified > 0)) {
		columnsModified = new int[numColumnsModified];

		std::vector<int32_t>& columnIndices = *colIndices;

		for (int i = 0; i < numColumnsModified; i++) {
			columnsModified[i] = columnIndices[i];
		}
	} else {
		columnsModified = NULL;
	}

	// it's not possible to get DIRECT access to
	// the TableTuple beforeImage's data buffer
	// (not the same as serialized buffer)
	beforeImageData = NULL;

	// same
	afterImageData = NULL;

	schema = initSchema();

	// XXX HACKY
	// This is not arbitrary, rather its based on all the
	// fields in the log record header
	size_t recordDataLen = 80;

	recordDataLen += tableName.capacity();

	if (primaryKey != NULL) {
		recordDataLen += sizeof(int32_t) + primaryKey->maxExportSerializationSize();
	} else {
		recordDataLen += sizeof(intptr_t);	// just a null pointer's size
	}

	if (columnsModified != NULL) {
		recordDataLen += (sizeof(int32_t) * numColumnsModified);
	} else {
		recordDataLen += sizeof(intptr_t);	// just a null pointer's size
	}

	if (beforeImage != NULL) {
		recordDataLen += sizeof(int32_t) + beforeImage->maxExportSerializationSize();
	} else {
		recordDataLen += sizeof(intptr_t);	// just a null pointer's size
	}

	if (afterImage != NULL) {
		recordDataLen += sizeof(int32_t) +  afterImage->maxExportSerializationSize();
	} else {
		recordDataLen += sizeof(intptr_t);	// just a null pointer's size
	}

	recordData = new char[static_cast<int32_t>(recordDataLen)];
	recordTuple = new TableTuple(recordData, schema);

	initRecordTuple();

	isValid = true;
}

LogRecord::LogRecord(ReferenceSerializeInput &input) {
	schema = initSchema();

	recordData = new char[MAX_RECORD_TUPDATA_LEN];
	recordTuple = new TableTuple(recordData, schema);

	recordTuple->deserializeFrom(input, NULL);

    // XXX: must figure how to handle failed deserialization
    bool deserializeSuccess = true;

    if (!deserializeSuccess) {
    	recordTuple = NULL;
    }

    type = T_INVALIDTYPE;
    category = T_BAD_CATEGORY;

    beforeImageData = NULL;
    beforeImage = NULL;

    afterImageData = NULL;
    afterImage = NULL;

	primaryKey = NULL;

	numColumnsModified = -1;
	columnsModified = NULL;

    isValid = false;
}

LogRecord::~LogRecord() {
	if (recordTuple != NULL) {
		// this call to free is very important
		// memory leaks otherwise.
		//XXX: this is way too slow and kills performance
		// by almost a factor of 8 in 8 sited execution
		// Besides, its not inherent to Volt as a Logrecord need not be
		// a TableTuple, its just that way to be able to leverage Volt's
		// machinery. Best to run experiments with this call commented out.
		// Only problem is we can't run for too long due to the memory leak.
		// recordTuple->freeObjectColumns(); //XXX: for now
		//recordTuple->freeObjectColumnsOfLogTuple(); //XXX: don't do this, results in UB

		delete recordTuple;
		recordTuple = NULL;
	}

	if (columnsModified != NULL) {
		delete[] columnsModified;
		columnsModified = NULL;
	}

	assert(recordData != NULL);
	delete[] recordData;
	recordData = NULL;

	TupleSchema::freeTupleSchema(schema);
}

TableTuple* LogRecord::initRecordTuple() {
	recordTuple->setNValue(0, ValueFactory::getDoubleValue(lsn));
	recordTuple->setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(type)));
	recordTuple->setNValue(2, ValueFactory::getTinyIntValue(static_cast<int8_t>(category)));
	recordTuple->setNValue(3, ValueFactory::getDoubleValue(prevLsn));
	recordTuple->setNValue(4, ValueFactory::getBigIntValue(xid));
	recordTuple->setNValue(5, ValueFactory::getIntegerValue(execSiteId));
	recordTuple->setNValue(6, ValueFactory::getStringValue(tableName));

	if (primaryKey != NULL) {
		size_t len = sizeof(int32_t) + primaryKey->maxExportSerializationSize();
		char *pkeyRawData = new char[len];

		FallbackSerializeOutput output;
		output.initializeWithPosition(pkeyRawData, len, 0);

		primaryKey->serializeTo(output);
		recordTuple->setNValue(7,
				ValueFactory::getBinaryValue(
				reinterpret_cast<unsigned char*>(const_cast<char*>(output.data())),
				static_cast<int32_t>(output.position())
				)
		);

		// Safe to delete, ValueFactory copies out the data.
		delete[] pkeyRawData;
		pkeyRawData = NULL;
	} else {
		recordTuple->setNValue(7, ValueFactory::getNullBinaryValue());
	}

	recordTuple->setNValue(8, ValueFactory::getIntegerValue(numColumnsModified));

	if (numColumnsModified > 0) {
		char *colData = new char[numColumnsModified * sizeof(int32_t)];

		FallbackSerializeOutput output;
		output.initializeWithPosition(colData, numColumnsModified * sizeof(int32_t), 0);

		for (int i = 0; i < numColumnsModified; i++) {
			output.writeInt(columnsModified[i]);
		}

		recordTuple->setNValue(9,
				ValueFactory::getBinaryValue(
				reinterpret_cast<unsigned char*>(const_cast<char*>(output.data())),
				static_cast<int32_t>(output.position())
				)
		);

		delete[] colData;
		colData = NULL;
	} else {
		recordTuple->setNValue(9, ValueFactory::getNullBinaryValue());
	}

	// only present for T_UPDATE and T_DELETE, that too in the absence of a primary index
	if (beforeImage != NULL) {
		size_t len = sizeof(int32_t) + beforeImage->maxExportSerializationSize();
		char *beforeRawData = new char[len];

		FallbackSerializeOutput output;
		output.initializeWithPosition(beforeRawData, len, 0);

		beforeImage->serializeTo(output);
		recordTuple->setNValue(10,
				ValueFactory::getBinaryValue(
				reinterpret_cast<unsigned char*>(const_cast<char*>(output.data())),
				static_cast<int32_t>(output.position())
				)
		);

		// Safe to delete beforeData because
		// ValueFactory copies out the data.
		delete[] beforeRawData;
		beforeRawData = NULL;

	} else {
		recordTuple->setNValue(10, ValueFactory::getNullBinaryValue());
	}

	// only present for T_INSERT, T_UPDATE (not for T_BULKLOAD, T_DELETE, T_TRUNCATE)
	if (afterImage != NULL) {
		// This is correct, tupleLength() is not required.
		// On serialization, the first 4 bytes indicate the length
		// and then comes the tuple.
		size_t len = sizeof(int32_t) + afterImage->maxExportSerializationSize();
		char *afterRawData = new char[len];

		FallbackSerializeOutput output;
		output.initializeWithPosition(afterRawData, len, 0);

		afterImage->serializeTo(output);
		recordTuple->setNValue(11,
				ValueFactory::getBinaryValue(
				reinterpret_cast<unsigned char*>(const_cast<char*>(output.data())),
				static_cast<int32_t>(output.position())
				)
		);

		delete[] afterRawData;
		afterRawData = NULL;
	} else {
		recordTuple->setNValue(11, ValueFactory::getNullBinaryValue());
	}

	//debug
	VOLT_DEBUG("Logrecord : %s", recordTuple->debugNoHeader().c_str());

	return recordTuple;
}

void LogRecord::populateFields(const TupleSchema *imageSchema, TableIndex *pkeyIndex) {
	if (isValidRecord()) {
		return; // nothing to be done
	}

	if (recordTuple == NULL) {
		return; // failed deserialization of logrecord
	}

    lsn = ValuePeeker::peekDouble(recordTuple->getNValue(0));
    type = static_cast<Logrec_type_t> (ValuePeeker::peekTinyInt(recordTuple->getNValue(1)));
    category = static_cast<Logrec_category_t> (ValuePeeker::peekTinyInt(recordTuple->getNValue(2)));
    prevLsn = ValuePeeker::peekDouble(recordTuple->getNValue(3));
    xid = ValuePeeker::peekBigInt(recordTuple->getNValue(4));
    execSiteId = ValuePeeker::peekInteger(recordTuple->getNValue(5));

    tableName = std::string(reinterpret_cast<char *>(ValuePeeker::peekObjectValue(recordTuple->getNValue(6))),
    		ValuePeeker::peekObjectLength(recordTuple->getNValue(6)));

    numColumnsModified = ValuePeeker::peekInteger(recordTuple->getNValue(8));

    // Primary key is only populated for updates, don't bother otherwise
	char *pkeyRawData = reinterpret_cast<char*>(ValuePeeker::peekObjectValue(recordTuple->getNValue(7)));

	if (type == T_UPDATE || type == T_DELETE) {
        if (pkeyRawData != NULL) {
        	// so we do have a primary key
			size_t pkeyRawLen = ValuePeeker::peekObjectLength(recordTuple->getNValue(7));
			ReferenceSerializeInput input(pkeyRawData, pkeyRawLen);

			char *pkeyData = new char[MAX_TUPLE_PKEY_LEN];
			primaryKey = new TableTuple(pkeyData, pkeyIndex->getKeySchema());
			primaryKey->deserializeFrom(input, NULL);

			pkeyIndex->moveToKey(primaryKey);
			beforeImage = new TableTuple(pkeyIndex->nextValueAtKey());

			delete[] pkeyData;
			pkeyData = NULL;

			delete primaryKey;
			primaryKey = NULL;
        } else {
        	// the entire before image must exist in absence of a primary key.
            char *beforeRawData = reinterpret_cast<char*>(ValuePeeker::peekObjectValue(recordTuple->getNValue(10)));

			size_t beforeRawLen = ValuePeeker::peekObjectLength(recordTuple->getNValue(10));
			ReferenceSerializeInput input(beforeRawData, beforeRawLen);

			beforeImageData = new char[MAX_RAW_TUPLE_LEN];

			beforeImage = new TableTuple(beforeImageData, imageSchema);
			beforeImage->deserializeFrom(input, NULL);
        }
    } else { // T_INSERT, T_BULKLOAD, T_TRUNCATE
    	beforeImage = NULL;
    }

    char *afterRawData = reinterpret_cast<char*>(ValuePeeker::peekObjectValue(recordTuple->getNValue(11)));

    if (type == T_INSERT || type == T_UPDATE) {
    	size_t afterRawLen = ValuePeeker::peekObjectLength(recordTuple->getNValue(11));

        ReferenceSerializeInput input(afterRawData, afterRawLen);

        afterImageData = new char[MAX_RAW_TUPLE_LEN];
    	afterImage = new TableTuple(afterImageData, imageSchema);

    	if ((type == T_UPDATE) && (numColumnsModified > 0)) {
            afterImage->copy(*beforeImage);

    		char *columnsRawData = reinterpret_cast<char*>(ValuePeeker::peekObjectValue(recordTuple->getNValue(9)));
    		ReferenceSerializeInput colInput(columnsRawData, (numColumnsModified * sizeof(int32_t)));

    		columnsModified = new int[numColumnsModified];

    		for (int i = 0; i < numColumnsModified; i++) {
    			columnsModified[i] = colInput.readInt();
    		}

    	    std::vector<voltdb::ValueType> subColumnTypes;
    	    std::vector<int32_t> subColumnLengths;

    	    // updateexecutor always has the first field as a 64-bit pointer
    	    // must have it here, otherwise garbage will get read back
	        subColumnTypes.push_back(VALUE_TYPE_BIGINT);
	        subColumnLengths.push_back(8);

    	    for (int i = 0; i < numColumnsModified; ++i) {
    	        subColumnTypes.push_back(imageSchema->columnType(columnsModified[i]));
    	        subColumnLengths.push_back(imageSchema->columnLength(columnsModified[i]));
    	    }

    	    std::vector<bool> subColumnAllowNull(numColumnsModified + 1, true);

    	    TupleSchema *subSchema = TupleSchema::createTupleSchema(subColumnTypes, subColumnLengths, subColumnAllowNull, true);
    	    char *subTupData = new char[MAX_RAW_TUPLE_LEN];

    	    TableTuple *subTuple = new TableTuple(subTupData, subSchema);
       		subTuple->deserializeFrom(input, NULL);

    		for (int i = 0; i < numColumnsModified; i++) {
    			// must be i+1 to avoid incorrectly copying the zeroth 64-bit ptr column
    			afterImage->setNValue(columnsModified[i], subTuple->getNValue(i + 1));
    		}

    		delete[] subTupData;
    		subTupData = NULL;

            TupleSchema::freeTupleSchema(subSchema);

    		delete subTuple;
    		subTuple = NULL;
    	} else {
    		// update or insert
    		afterImage->deserializeFrom(input, NULL);
    	}
    } else {	// T_BULKLOAD or T_DELETE or T_TRUNCATE
    	afterImage = NULL;
    }

    isValid = true;
}

std::string& LogRecord::getTableName() {
	if (isValidRecord()) {
		return tableName;
	}

	if (recordTuple == NULL) {
		tableName = "";
		return tableName; // nothing is instantiated
	}

	tableName = std::string(reinterpret_cast<char *>(ValuePeeker::peekObjectValue(recordTuple->getNValue(6))),
	    		ValuePeeker::peekObjectLength(recordTuple->getNValue(6)));
	return tableName;
}

void LogRecord::serializeTo(SerializeOutput &output) {
	recordTuple->serializeTo(output);
}

size_t LogRecord::getEstimatedLength() {
	// max export size does not include the header size which records
	// length of tuple.
	return (sizeof(int32_t) + recordTuple->maxExportSerializationSize());
}

TupleSchema* LogRecord::initSchema() {
    // Time to create schema for a new tuple

    std::vector<ValueType> types;
    std::vector<int32_t> columnLengths;
    std::vector<bool> allowNull;

    // LSN
    types.push_back(VALUE_TYPE_DOUBLE);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
    allowNull.push_back(false);

    // type of record
    types.push_back(VALUE_TYPE_TINYINT);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
    allowNull.push_back(false);

    // record category
    types.push_back(VALUE_TYPE_TINYINT);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
    allowNull.push_back(false);

    // prev-LSN
    types.push_back(VALUE_TYPE_DOUBLE);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
    allowNull.push_back(false);

    // xid
    types.push_back(VALUE_TYPE_BIGINT);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
    allowNull.push_back(false);

    // Exec-site id
    types.push_back(VALUE_TYPE_INTEGER);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
    allowNull.push_back(false);

    // tableName
    // we do not log writes to intermediate temp tuples
    types.push_back(VALUE_TYPE_VARCHAR);
    columnLengths.push_back(MAX_TABLENAME_LEN);
    allowNull.push_back(false);

    // If there is a primary key, its recorded here in case
    // of an update record, is null otherwise
    types.push_back(VALUE_TYPE_VARBINARY);
    columnLengths.push_back(MAX_TUPLE_PKEY_LEN);
    allowNull.push_back(true);

    // No of columns modified
    // -1 if entire tuple (eg insert or when no primary key in case of update)
    types.push_back(VALUE_TYPE_INTEGER);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
    allowNull.push_back(false);

    // Columns modified -- the number is in the previous field
    // Also, a value of -1 in that field is a NULL here.
    types.push_back(VALUE_TYPE_VARBINARY);
    columnLengths.push_back(MAX_NUM_COL_LEN);
    allowNull.push_back(true);

 	// before image
    // NULL for inserts, bulk-loads, and for updates too for the most part.
    // In case there is no primary key/unique index though,
    // the entire before image must be written
    // (eg no primary key)
    types.push_back(VALUE_TYPE_VARBINARY);
    columnLengths.push_back(MAX_RAW_TUPLE_LEN);
    allowNull.push_back(true);			// can be null

 	// after image
    // INSERTS: the entire tuple
    // BULKLOADS: NULL (all the bytes are stored beyond the log record entry)
    // UPDATES: only the relevant columns modified
    // (ie a different TableTuple with an appropriate subset schema)
    types.push_back(VALUE_TYPE_VARBINARY);
    columnLengths.push_back(MAX_RAW_TUPLE_LEN);
    allowNull.push_back(true);		// XXX: can this be null?

    return TupleSchema::createTupleSchema(types, columnLengths,
    		allowNull, true);
}

