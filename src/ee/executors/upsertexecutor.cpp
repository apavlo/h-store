/* This file is part of VoltDB.
* Copyright (C) 2008-2010 VoltDB L.L.C.
*
* This file contains original code and/or modifications of original code.
* Any modifications made by VoltDB L.L.C. are licensed under the following
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

#include "upsertexecutor.h"
#include "common/debuglog.h"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "plannodes/upsertnode.h"
#include "execution/VoltDBEngine.h"
#include "storage/persistenttable.h"
#include "storage/streamedtable.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "storage/tablefactory.h"
#include "storage/temptable.h"
#include "common/types.h"
#include "storage/WindowTable.h"

// added by hawk, 10/4/2013
#include "catalog/catalogmap.h"
#include "catalog/table.h"
#include "catalog/column.h"
// ended by hawk

namespace voltdb {

	UpsertExecutor::~UpsertExecutor() {
		delete [] m_searchKeyBackingStore;
	}

	bool UpsertExecutor::p_init(AbstractPlanNode *abstract_node, const catalog::Database* catalog_db, int* tempTableMemoryInBytes) {
		VOLT_DEBUG("init Upsert Executor");

		m_node = dynamic_cast<UpsertPlanNode*>(abstract_node);
		assert(m_node);
		assert(m_node->getTargetTable());
		assert(m_node->getInputTables().size() == 1);

		// create an output table that currently will contain copies of modified tuples
		m_node->setOutputTable(TableFactory::getCopiedTempTable(
			m_node->databaseId(),
			m_node->getInputTables()[0]->name(),
			m_node->getInputTables()[0],
			tempTableMemoryInBytes));

		// 2013-07-29 - PAVLO
		// Sail yo ho, kids. John Meehan and I were here, and we decided that in
		// order to support INSERT INTO..SELECT statements, we had to let the input
		// tables be any kind of table. This is because sometimes for certain SELECTS
		// you will get a handle to the original PersistentTable and not a TempTable.
		// We don't think that this should cause any problems.
		m_inputTable = m_node->getInputTables()[0];
		if (m_inputTable == NULL) {
			VOLT_ERROR("Missing input table for InsertPlanNode #%d [numInputs=%ld]",
				m_node->getPlanNodeId(), m_node->getInputTables().size());
			// VOLT_ERROR("INPUT TABLE DUMP:\n%s", m_node->getInputTables()[0]->debug().c_str());
			return false;
		}
		assert(m_inputTable);

		// Target table can be StreamedTable or PersistentTable and must not be NULL
		m_targetTable = dynamic_cast<Table*>(m_node->getTargetTable());
		assert(m_targetTable);
		assert((m_targetTable == dynamic_cast<PersistentTable*>(m_targetTable)) ||
			(m_targetTable == dynamic_cast<StreamedTable*>(m_targetTable)));

		m_tuple = TableTuple(m_inputTable->schema());

		// added by hawk, 10/4/2013
		m_targetTuple = TableTuple(m_targetTable->schema());
		// ended by hawk

		// added by hawk, 2013/10/24
		// get the primary key
		m_primarykey_index = m_targetTable->primaryKeyIndex();
		if (m_primarykey_index == NULL)
		{
			VOLT_ERROR("Failed to retreive primarykey index from table '%s'...",
				m_targetTable->name().c_str());
			delete [] m_searchKeyBackingStore;
			return false;
		}
		// get corresponding position of each collumn of primary keys 
		const std::vector<int> columindices = m_primarykey_index->getColumnIndices();
		m_primarykey_size = 0;
		m_primarykey_size = m_primarykey_index->getColumnCount();
		//int m_primarykeys[m_primarykey_size];

		m_primarykeysArrayPtr =
			boost::shared_array<int>(new int[m_primarykey_size]);
		m_primarykeysArray = m_primarykeysArrayPtr.get();

		int iii = 0;
		for(std::vector<int>::const_iterator it = columindices.begin(); it != columindices.end(); ++it){
			m_primarykeysArrayPtr[iii++] = *it;
		}

		// initialize m_searchKey
		m_searchKey = TableTuple(m_primarykey_index->getKeySchema());
		m_searchKeyBackingStore = new char[m_primarykey_index->getKeySchema()->tupleLength()];
		m_searchKey.moveNoHeader(m_searchKeyBackingStore);

		VOLT_DEBUG("Upsert Executor: finishing initializing search section...");

		// ended by hawk

		PersistentTable *persistentTarget = dynamic_cast<PersistentTable*>(m_targetTable);
		m_partitionColumn = -1;
		m_partitionColumnIsString = false;
		if (persistentTarget) {
			m_partitionColumn = persistentTarget->partitionColumn();
			if (m_partitionColumn != -1) {
				if (m_inputTable->schema()->columnType(m_partitionColumn) == voltdb::VALUE_TYPE_VARCHAR) {
					m_partitionColumnIsString = true;
				}
			}
			// TODO: Check if PersistentTable has triggers
			// TODO: If so, then set some flag in InsertExecutor to true
		}
		m_multiPartition = m_node->isMultiPartition();
		VOLT_DEBUG("finishing Upsert Executor initialization...");
		return true;
	}

	bool UpsertExecutor::p_execute(const NValueArray &params, ReadWriteTracker *tracker) {
		VOLT_DEBUG("execute Upsert Executor");

		assert(m_node == dynamic_cast<UpsertPlanNode*>(abstract_node));
		assert(m_node);
		// XXX assert(m_inputTable == dynamic_cast<TempTable*>(m_node->getInputTables()[0]));
		assert(m_inputTable);
		assert(m_targetTable);
		VOLT_DEBUG("INPUT TABLE: %s\n", m_inputTable->debug().c_str());
		VOLT_DEBUG("hawk: OUTPUT TABLE: %s\n", m_targetTable->debug().c_str());
#ifdef DEBUG
		//
		// This should probably just be a warning in the future when we are
		// running in a distributed cluster
		//
		if (m_inputTable->activeTupleCount() == 0) {
			VOLT_ERROR("No tuples were found in our input table '%s'",
				m_inputTable->name().c_str());
			return false;
		}
#endif
		assert (m_inputTable->activeTupleCount() > 0);

		// count the number of successful inserts
		int modifiedTuples = 0;

		Table* outputTable = m_node->getOutputTable();
		assert(outputTable);

		//
		// An insert is quite simple really. We just loop through our m_inputTable
		// and insert any tuple that we find into our m_targetTable. It doesn't get any easier than that!
		//
		assert (m_tuple.sizeInValues() == m_inputTable->columnCount());
		assert(m_targetTuple.sizeInValues() == m_targetTable->columnCount()); // added by hawk, 10/4/2013

		TableIterator iterator(m_inputTable);
		bool beProcessed = false;
		while (iterator.next(m_tuple)) {
			beProcessed = true;
			VOLT_DEBUG("Inserting tuple '%s' into target table '%s'",
				m_tuple.debug(m_targetTable->name()).c_str(), m_targetTable->name().c_str());
			VOLT_TRACE("Target Table %s: %s",
				m_targetTable->name().c_str(), m_targetTable->schema()->debug().c_str());

			// if there is a partition column for the target table
			if (m_partitionColumn != -1) {

				// get the value for the partition column
				NValue value = m_tuple.getNValue(m_partitionColumn);
				bool isLocal = m_engine->isLocalSite(value);

				// if it doesn't map to this site
				if (!isLocal) {
					if (!m_multiPartition) {
						VOLT_ERROR("Mispartitioned Tuple in single-partition plan.");
						return false;
					}

					// don't insert
					continue;
				}
			}

			{
			        VOLT_DEBUG("hawk : the primary key index '%s'", m_primarykey_index->debug().c_str());
				// set new values from the m_tuple
				m_searchKey.setAllNulls();
			        VOLT_DEBUG("hawk : m_primarykey_size : '%d'", m_primarykey_size);
				for (int ctr = 0; ctr < m_primarykey_size; ctr++)
				{
                                        int ipos = m_primarykeysArray[ctr];
			                NValue key_value = m_tuple.getNValue(ipos);
			                VOLT_DEBUG("hawk : ipos - value : '%d' - %s", ipos, key_value.debug().c_str());
					m_searchKey.setNValue( ctr, key_value);
				}
			        VOLT_DEBUG("hawk : the search key is '%s'", m_searchKey.debugNoHeader().c_str());

				// using primary key index to scan the indicate searchkey
				TableTuple tuple(m_targetTable->schema());
				m_primarykey_index->moveToKey(&m_searchKey);
				bool beDelete = false;
				while (	!(tuple = m_primarykey_index->nextValueAtKey()).isNullTuple())
				{
			                VOLT_DEBUG("hawk : found item");

					beDelete = true;
					break;
				}

			        VOLT_DEBUG("hawk : after seaching...");
				// if found the tuple, delete it
				if(beDelete==true)
				{
					// Read/Write Set Tracking
					if (tracker != NULL) {
						//tracker->markTupleWritten(m_targetTable->name(), &m_targetTuple);
						tracker->markTupleWritten(m_targetTable->name(), &tuple);
					}

					VOLT_DEBUG("hawk: begin to delete tuple: %s",
				            tuple.debug(m_targetTable->name()).c_str());

					// Delete from target table
					if (!m_targetTable->deleteTuple(tuple, true)) {
						VOLT_DEBUG("hawk: Failed to delete tuple from table '%s'",
							m_targetTable->name().c_str());
						return false;
					}
					VOLT_DEBUG("hawk: after deleteTuple....... ");
				}
			}

			/*
			{
			// determine if the m_tuple with the same primary key is aready there
			// if exists, delete it, and then render the control to the normal insert
			// if not, just render the control to the normal insert
			TableIndex *m_primarykey_index = m_targetTable->primaryKeyIndex();
			const std::vector<int> m_columindices = m_primarykey_index->getColumnIndices();
			int m_primarykey_size = 0;
			int sizeCols = m_primarykey_index->getColumnCount();
			int primarykeys[sizeCols];
			for(std::vector<int>::const_iterator it = m_columindices.begin(); it != m_columindices.end(); ++it){
			primarykeys[m_primarykey_size++] = *it;
			}
			assert(!(m_primarykey_size-sizeCols));

			TableTuple tuple(m_targetTable->schema());
			TableIterator iterator(m_targetTable);
			VOLT_DEBUG("hawk : scan target table... ");
			while (iterator.next(tuple))
			{
			bool beDelete = true;
			// iterate the primary key related columns
			for(int ii=0;ii<sizeCols;ii++)
			{
			NValue value_source = m_tuple.getNValue(ii);
			VOLT_DEBUG("source tuple '%s'",
			m_tuple.debug(m_targetTable->name()).c_str());
			NValue value_target = tuple.getNValue(ii);
			VOLT_DEBUG("target tuple '%s'",
			tuple.debug(m_targetTable->name()).c_str());

			// determine if they are same
			if(value_source.compare(value_target) != 0)
			{
			beDelete = false;
			break;
			}
			}

			if(beDelete == true)
			{
			// Read/Write Set Tracking
			if (tracker != NULL) {
			//tracker->markTupleWritten(m_targetTable->name(), &m_targetTuple);
			tracker->markTupleWritten(m_targetTable->name(), &tuple);
			}

			VOLT_DEBUG("hawk: after markTupleWritten()....... ");

			// Delete from target table
			if (!m_targetTable->deleteTuple(tuple, true)) {
			VOLT_ERROR("Failed to delete tuple from table '%s'",
			m_targetTable->name().c_str());
			return false;
			}

			break;
			}
			}

			}
			*/
			{
				// try to put the tuple into the target table
				if (!m_targetTable->insertTuple(m_tuple)) {
					VOLT_ERROR("Failed to insert tuple from input table '%s' into"
						" target table '%s'",
						m_inputTable->name().c_str(),
						m_targetTable->name().c_str());
					return false;
				}

				// try to put the tuple into the output table
				if (!outputTable->insertTuple(m_tuple)) {
					VOLT_ERROR("Failed to insert tuple from input table '%s' into"
						" output table '%s'",
						m_inputTable->name().c_str(),
						outputTable->name().c_str());
					return false;
				}

				// successfully inserted
				modifiedTuples++;
			}
			// ended by hawk

		}
		// Check if the target table is persistent, and if hasTriggers flag is true
		// If it is, then iterate through each one and pass in outputTable
		if (beProcessed == true)
		{
			PersistentTable* persistTarget = dynamic_cast<PersistentTable*>(m_targetTable);
			if(persistTarget != NULL && persistTarget->hasTriggers()) {
				std::vector<Trigger*>::iterator trig_iter;

				VOLT_DEBUG( "Start firing triggers of table '%s'", persistTarget->name().c_str());

				for(trig_iter = persistTarget->getTriggers()->begin();
					trig_iter != persistTarget->getTriggers()->end(); trig_iter++) {
						//if statement to make sure the trigger is an insert... breaking
						//if((*trig_iter)->getType() == (unsigned char)TRIGGER_INSERT)
						(*trig_iter)->fire(m_engine, outputTable);
				}
				VOLT_DEBUG( "End firing triggers of table '%s'", persistTarget->name().c_str());
			}
		}


		// add to the planfragments count of modified tuples
		m_engine->m_tuplesModified += modifiedTuples;
		VOLT_DEBUG("Finished inserting %d tuples", modifiedTuples);
		return true;
	}

}
