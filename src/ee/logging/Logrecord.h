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
 * Logrecord.h
 *
 *      Author: nirmeshmalviya
 */

#ifndef LOGRECORD_H_
#define LOGRECORD_H_

#include "common/tabletuple.h"
#include "common/TupleSchema.h"

#include "indexes/tableindex.h"

#define OFFSET_TO_TXNTYPE		8
#define OFFSET_TO_TXNID			8 + 1 + 1 + 8
#define OFFSET_TO_SITEID		8 + 1 + 1 + 8 + 8

namespace voltdb {

class LogRecord {
public:
	enum Logrec_type_t {
		T_INVALIDTYPE = 0,
		T_INSERT,
		T_UPDATE,
		T_BULKLOAD,
		T_DELETE,
		T_TRUNCATE
	};

	enum Logrec_category_t {
		 T_BAD_CATEGORY = 0,
		 T_FORWARD,
		 T_ROLLBACK
	};

	LogRecord(double timestamp, Logrec_type_t type, Logrec_category_t category,
			double prevLsn, int64_t xid, int32_t execSiteId, const std::string& tableName,
			TableTuple *primaryKey, int32_t numCols, std::vector<int32_t> *colIndices,
			TableTuple* beforeImage, TableTuple *afterImage);

	LogRecord(ReferenceSerializeInput &input); 		// parsing back written out records

	~LogRecord();

	void serializeTo(SerializeOutput &output);
	size_t getEstimatedLength();

	TupleSchema* getRecordSchema();

	inline bool isValidRecord() {
		return isValid;
	}

	std::string& getTableName();
	void populateFields(const TupleSchema *imageSchema, TableIndex *pkeyIndex);

	inline Logrec_type_t getType() {
		return type;
	}

	inline TableTuple* getTupleAfterImage() {
		if (isValid) {
			return afterImage;
		}

		return NULL;
	}

	inline TableTuple* getTupleBeforeImage() {
		if (isValid) {
			return beforeImage;
		}

		return NULL;
	}

	inline TableTuple* getPrimaryKey() {
		if (isValid) {
			return primaryKey;
		}

		return NULL;
	}

    void dellocateBeforeImageData()
    {
    	if (beforeImageData != NULL) {
    		delete[] beforeImageData;
    		beforeImageData = NULL;
    	}
    }

    void dellocateAfterImageData()
    {
    	if (afterImageData != NULL) {
    		delete[] afterImageData;
    		afterImageData = NULL;
    	}
    }

    inline bool hasTrailingLoadData() {
    	// In case of a bulk load of table data,
    	// the before and after images itself do not
    	// contain any tuple binaries, rather the arbitrarily large
    	// binary immediately follows the log record. This must
    	// be taken care of when the entire log is read back.
    	return ((type == T_BULKLOAD) && (category == T_FORWARD));
    }
private:
	LogRecord();	// do not allow empty constructor
	TupleSchema* initSchema();
	TableTuple* initRecordTuple();

	bool isValid;

	double lsn;

	Logrec_type_t type;
	Logrec_category_t category;

	double prevLsn;

	int64_t xid;		// transaction-id
	int32_t execSiteId;			// id of the execution site

	std::string tableName;		// what table are we updating?

	TableTuple *primaryKey;			// what's the primary key

	int32_t numColumnsModified;			// how many columns are being updated
	int32_t* columnsModified;			// ids of columns modified -- for updates

	// data buffer for beforeImage tuple - used when deserializing a log record
	char *beforeImageData;
	TableTuple *beforeImage;

	// data buffer for afterImage tuple -- used when deserializing a log record
	char *afterImageData;
	TableTuple *afterImage;

	TupleSchema *schema; // log record's schema
	char *recordData;		// data for the tuple representing the record

	TableTuple *recordTuple;	 // the actual tuple representing the record
};

}

#endif /* LOGRECORD_H_ */
