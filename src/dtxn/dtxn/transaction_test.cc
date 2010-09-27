// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/transaction.h"
#include "stupidunit/stupidunit.h"

using dtxn::ExecutionEngine;
using dtxn::NoLockTransaction;
using std::string;

TEST(Transaction, SinglePartition) {
    NoLockTransaction txn(this, "txn", true, false);
    // single partition transactions must set last fragment
    EXPECT_DEATH(NoLockTransaction txn2(this, "txn", false, false));

    EXPECT_EQ("txn", txn.last_work_unit());
    EXPECT_EQ(this, txn.external_state());
    EXPECT_FALSE(txn.multiple_partitions());
    EXPECT_TRUE(txn.last_fragment());

    // can't add to an SP txn
    EXPECT_DEATH(txn.addWorkUnit("txn2", false));
    // no result: can't send!
    EXPECT_DEATH(txn.resultSent());

    // get and set the undo pointer
    void** undoptr = txn.undo();
    EXPECT_NE(NULL, undoptr);
    EXPECT_EQ(NULL, *undoptr);
    *undoptr = this;
    EXPECT_EQ(this, *txn.undo());

    // set the status
    EXPECT_EQ(ExecutionEngine::INVALID, txn.last_status());
    EXPECT_DEATH(txn.setWorkUnitStatus(ExecutionEngine::INVALID));
    txn.setWorkUnitStatus(ExecutionEngine::BLOCKED);
    EXPECT_EQ(ExecutionEngine::BLOCKED, txn.last_status());
    // can occur when blocking, re-executing then hitting another lock
    txn.setWorkUnitStatus(ExecutionEngine::BLOCKED);
    EXPECT_EQ(ExecutionEngine::BLOCKED, txn.last_status());
    txn.setWorkUnitStatus(ExecutionEngine::OK);
    EXPECT_DEATH(txn.setWorkUnitStatus(ExecutionEngine::OK));
    EXPECT_EQ(ExecutionEngine::OK, txn.last_status());

    txn.resultSent();
    EXPECT_EQ(ExecutionEngine::OK, txn.last_status());
    EXPECT_DEATH(txn.resultSent());
}

TEST(Transaction, MultiplePartitionsOneShot) {
    // multiple-partition, last fragment
    NoLockTransaction txn(this, "txn", true, true);

    EXPECT_TRUE(txn.multiple_partitions());
    EXPECT_TRUE(txn.last_fragment());

    // can't add: have not sent
    EXPECT_DEATH(txn.addWorkUnit("txn2", false));
    // no result: can't send!
    EXPECT_DEATH(txn.resultSent());

    // set the status
    txn.setWorkUnitStatus(ExecutionEngine::BLOCKED);
    txn.setWorkUnitStatus(ExecutionEngine::BLOCKED);
    txn.setWorkUnitStatus(ExecutionEngine::OK);
    EXPECT_DEATH(txn.setWorkUnitStatus(ExecutionEngine::BLOCKED));
    EXPECT_EQ(ExecutionEngine::OK, txn.last_status());

    txn.resultSent();
    EXPECT_EQ(ExecutionEngine::OK, txn.last_status());
    EXPECT_DEATH(txn.resultSent());

    // can't add: last fragment!
    EXPECT_DEATH(txn.addWorkUnit("txn2", false));
}

TEST(Transaction, MultiplePartitionsMultipleRoundAbort) {
    // multiple-partition
    NoLockTransaction txn(this, "txn", false, true);

    EXPECT_TRUE(txn.multiple_partitions());
    EXPECT_FALSE(txn.last_fragment());

    // can't add: have not sent
    EXPECT_DEATH(txn.addWorkUnit("txn2", false));
    // no result: can't send!
    EXPECT_DEATH(txn.resultSent());

    // send out the first results
    txn.output()->assign("foo");
    txn.setWorkUnitStatus(ExecutionEngine::OK);
    txn.resultSent();
    // be sure we clear the output when starting a new piece of work
    EXPECT_EQ("", *txn.output());

    txn.addWorkUnit("txn2", false);
    EXPECT_EQ("txn2", txn.last_work_unit());
    ASSERT_EQ(2, txn.work_units().size());
    EXPECT_EQ("txn", txn.work_units()[0]);
    EXPECT_EQ("txn2", txn.work_units()[1]);

    // abort this piece
    txn.setWorkUnitStatus(ExecutionEngine::ABORT_USER);
    txn.resultSent();
    EXPECT_EQ(ExecutionEngine::ABORT_USER, txn.last_status());

    // can't add to an aborted transaction
    EXPECT_DEATH(txn.addWorkUnit("txn3", false));
}

TEST(Transaction, MultiplePartitionsMultipleRoundLastFragment) {
    // multiple-partition
    NoLockTransaction txn(this, "txn", false, true);

    // send out the first results
    txn.setWorkUnitStatus(ExecutionEngine::OK);
    txn.resultSent();

    // add and send the second result: this is a last fragment
    txn.addWorkUnit("txn2", true);
    EXPECT_TRUE(txn.last_fragment());
    EXPECT_EQ("txn2", txn.last_work_unit());

    txn.setWorkUnitStatus(ExecutionEngine::OK);
    txn.resultSent();
    EXPECT_EQ(ExecutionEngine::OK, txn.last_status());

    // can't add to a "last fragment" marked transaction
    EXPECT_DEATH(txn.addWorkUnit("txn3", false));
}

TEST(Transaction, ReExecuteSinglePartition) {
    NoLockTransaction txn(NULL, "txn", true, false);

    // Execute a chunk with undo
    *txn.undo() = this;
    EXPECT_DEATH(txn.prepareReExecute());  // have not executed yet
    txn.output()->assign("foo");
    txn.setWorkUnitStatus(ExecutionEngine::OK);

    // Record that we are going to re-execute
    txn.prepareReExecute();
    EXPECT_EQ(NULL, *txn.undo());
    EXPECT_EQ("", *txn.output());
    EXPECT_EQ(ExecutionEngine::INVALID, txn.last_status());

    // If aborted, we can have a NULL undo buffer
    txn.setWorkUnitStatus(ExecutionEngine::ABORT_USER);
    txn.prepareReExecute();

    // Don't permit applying undo after sending results
    txn.setWorkUnitStatus(ExecutionEngine::ABORT_USER);
    txn.resultSent();    
    EXPECT_DEATH(txn.prepareReExecute());
}

TEST(Transaction, ReExecuteMultiPartition) {
    NoLockTransaction txn(NULL, "txn", false, true);

    // Execute a chunk with undo
    *txn.undo() = this;
    EXPECT_DEATH(txn.prepareReExecute());  // have not executed yet
    txn.output()->assign("foo");
    txn.setWorkUnitStatus(ExecutionEngine::OK);

    // Record that we are going to re-execute
    txn.prepareReExecute();
    EXPECT_EQ(NULL, *txn.undo());
    EXPECT_EQ("", *txn.output());
    EXPECT_EQ(ExecutionEngine::INVALID, txn.last_status());

    // Don't permit applying undo after sending results
    txn.setWorkUnitStatus(ExecutionEngine::OK);
    txn.resultSent();
    EXPECT_DEATH(txn.prepareReExecute());
}
 
TEST(Transaction, ReExecuteNullUndo) {
   NoLockTransaction txn(NULL, "txn", false, true);

   // This fragment does not need undoing: it could be read only
   ASSERT_EQ(NULL, *txn.undo());
   EXPECT_DEATH(txn.prepareReExecute());  // have not executed yet
   txn.output()->assign("foo");
   txn.setWorkUnitStatus(ExecutionEngine::OK);

   // Record that we are going to re-execute
   txn.prepareReExecute();
   EXPECT_EQ(NULL, *txn.undo());
   EXPECT_EQ("", *txn.output());
   EXPECT_EQ(ExecutionEngine::INVALID, txn.last_status());
}
 
TEST(Transaction, DeadlockAbort) {
    NoLockTransaction txn(this, "txn", true, false);
    // No output is permitted for deadlock aborts
    txn.output()->assign("foo");
    EXPECT_DEATH(txn.setWorkUnitStatus(ExecutionEngine::ABORT_DEADLOCK));

    txn.output()->clear();
    txn.setWorkUnitStatus(ExecutionEngine::ABORT_DEADLOCK);
    EXPECT_DEATH(txn.setWorkUnitStatus(ExecutionEngine::OK));

    // again output is not permitted
    txn.output()->assign("foo");
    EXPECT_DEATH(txn.resultSent());

    txn.output()->clear();
    txn.resultSent();
    EXPECT_EQ(ExecutionEngine::ABORT_DEADLOCK, txn.last_status());
    EXPECT_DEATH(txn.resultSent());
}

TEST(Transaction, LogEntry) {
    NoLockTransaction txn(this, "txn", true, false);
    EXPECT_EQ(txn.work_units(), txn.log_entry().fragments);
    EXPECT_EQ(txn.multiple_partitions(), txn.log_entry().multiple_partitions);
    EXPECT_EQ(-1, txn.log_entry().decision_log_entry);
    EXPECT_FALSE(txn.log_entry().commit);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
