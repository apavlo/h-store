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

#include <iostream>
#include "seqscanexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "expressions/abstractexpression.h"
#include "plannodes/seqscannode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/limitnode.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"

using namespace voltdb;

bool SeqScanExecutor::p_init(AbstractPlanNode *abstract_node,
                             const catalog::Database* catalog_db,
                             int* tempTableMemoryInBytes) {
    VOLT_TRACE("init SeqScan Executor");

    SeqScanPlanNode* node = dynamic_cast<SeqScanPlanNode*>(abstract_node);
    assert(node);
    assert(node->getTargetTable());

    //
    // NESTED PROJECTION
    //
    if (node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION) != NULL) {
        //std::cout << "Inline node:" << node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION)->debug() << std::endl;
        ProjectionPlanNode* projection_node = static_cast<ProjectionPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));
        assert(projection_node);
        //
        // The internal node will already be initialized for us
        //
        // We just need to use the internal node's output table which
        // has been formatted correctly based on the projection
        // information as our own output table
        //
        assert(projection_node->getOutputTable());
        node->setOutputTable(projection_node->getOutputTable());
    //
    // FULL TABLE SCHEMA
    //
    } else {
        //
        // OPTIMIZATION: If there is no predicate for this SeqScan,
        // then we want to just set our OutputTable pointer to be the
        // pointer of our TargetTable. This prevents us from just
        // reading through the entire TargetTable and copying all of
        // the tuples. We are guarenteed that no Executor will ever
        // modify an input table, so this operation is safe
        //
        if (!this->needsOutputTableClear()) {
            node->setOutputTable(node->getTargetTable());
        //
        // Otherwise create a new temp table that mirrors the
        // TargetTable so that we can just copy the tuples right into
        // it. For now we are always use all of the columns, but in
        // the future we may want to have a projection work right
        // inside of the SeqScan
        //
        } else {
            node->setOutputTable(TableFactory::getCopiedTempTable(node->databaseId(),
                    node->getTargetTable()->name(),
                    node->getTargetTable(),
                    tempTableMemoryInBytes));
        }
    }
    return true;
}

bool SeqScanExecutor::needsOutputTableClear() {
    // clear the temporary output table only when it has a predicate.
    // if it doesn't have a predicate, it's the original persistent table
    // and we don't have to (and must not) clear it.
    SeqScanPlanNode* node = dynamic_cast<SeqScanPlanNode*>(abstract_node);
    assert(node);
    return node->needsOutputTableClear();
}

bool SeqScanExecutor::p_execute(const NValueArray &params, ReadWriteTracker *tracker) {
    SeqScanPlanNode* node = dynamic_cast<SeqScanPlanNode*>(abstract_node);
    assert(node);
    Table* output_table = node->getOutputTable();
    assert(output_table);
    Table* target_table = dynamic_cast<Table*>(node->getTargetTable());
    assert(target_table);
    //cout << "SeqScanExecutor: node id" << node->getPlanNodeId() << endl;
    VOLT_TRACE("Sequential Scanning table :\n %s",
               target_table->debug().c_str());
    VOLT_DEBUG("Sequential Scanning table : %s which has %d active, %d"
               " allocated tuples",
               target_table->name().c_str(),
               (int)target_table->activeTupleCount(),
               (int)target_table->allocatedTupleCount());

    //
    // OPTIMIZATION: NESTED PROJECTION
    //
    // Since we have the input params, we need to call substitute to
    // change any nodes in our expression tree to be ready for the
    // projection operations in execute
    //
    int num_of_columns = (int)output_table->columnCount();
    ProjectionPlanNode* projection_node = dynamic_cast<ProjectionPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));
    if (projection_node != NULL) {
        for (int ctr = 0; ctr < num_of_columns; ctr++) {
            assert(projection_node->getOutputColumnExpressions()[ctr]);
            projection_node->getOutputColumnExpressions()[ctr]->substitute(params);
        }
    }

    //
    // OPTIMIZATION: NESTED LIMIT
    // How nice! We can also cut off our scanning with a nested limit!
    //
    int limit = -1;
    int offset = -1;
    LimitPlanNode* limit_node = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    if (limit_node != NULL) {
        limit_node->getLimitAndOffsetByReference(params, limit, offset);
        if (offset > 0) {
            VOLT_ERROR("Nested Limit Offset is not yet supported for PlanNode"
                       " '%s'", node->debug().c_str());
            return false;
        }
    }

    //
    // OPTIMIZATION:
    //
    // If there is no predicate and no Projection for this SeqScan,
    // then we have already set the node's OutputTable to just point
    // at the TargetTable. Therefore, there is nothing we more we need
    // to do here
    //
    if (node->getPredicate() != NULL || projection_node != NULL ||
        limit_node != NULL)
    {
        //
        // Just walk through the table using our iterator and apply
        // the predicate to each tuple. For each tuple that satisfies
        // our expression, we'll insert them into the output table.
        //
        TableTuple tuple(target_table->schema());
        TableIterator iterator(target_table);
        AbstractExpression *predicate = node->getPredicate();
        VOLT_TRACE("SCAN PREDICATE A:\n%s\n", predicate->debug(true).c_str());

        if (predicate)
        {
            predicate->substitute(params);
            assert(predicate != NULL);
            VOLT_TRACE("SCAN PREDICATE B:\n%s\n",
                       predicate->debug(true).c_str());
        }

        int tuple_ctr = 0;
        while (iterator.next(tuple))
        {
            // Read/Write Set Tracking
            if (tracker != NULL) {
                tracker->markTupleRead(target_table, &tuple);
            }
            
            target_table->updateTupleAccessCount();
            VOLT_TRACE("INPUT TUPLE: %s, %d/%d\n",
                       tuple.debug(target_table->name()).c_str(), tuple_ctr,
                       (int)target_table->activeTupleCount());
            //
            // For each tuple we need to evaluate it against our predicate
            //
            if (predicate == NULL || predicate->eval(&tuple, NULL).isTrue())
            {
                //
                // Nested Projection
                // Project (or replace) values from input tuple
                //
                if (projection_node != NULL)
                {
                    TableTuple &temp_tuple = output_table->tempTuple();
                    for (int ctr = 0; ctr < num_of_columns; ctr++)
                    {
                        NValue value =
                            projection_node->
                          getOutputColumnExpressions()[ctr]->eval(&tuple, NULL);
                        temp_tuple.setNValue(ctr, value);
                    }
                    if (!output_table->insertTuple(temp_tuple))
                    {
                        VOLT_ERROR("Failed to insert tuple from table '%s' into"
                                   " output table '%s'",
                                   target_table->name().c_str(),
                                   output_table->name().c_str());
                        return false;
                    }
                }
                else
                {
                    //
                    // Insert the tuple into our output table
                    //
                    if (!output_table->insertTuple(tuple)) {
                        VOLT_ERROR("Failed to insert tuple from table '%s' into"
                                   " output table '%s'",
                                   target_table->name().c_str(),
                                   output_table->name().c_str());
                        return false;
                    }
                }
                ++tuple_ctr;
                // Check whether we have gone past our limit
                if (limit >= 0 && tuple_ctr >= limit) {
                    break;
                }
            }
        }
    }
    VOLT_TRACE("\n%s\n", output_table->debug().c_str());
    VOLT_TRACE("Finished Seq scanning");

    return true;
}
