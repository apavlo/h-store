// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <algorithm>
#include <limits>

#include "base/array.h"
#include "dtxn/mockexecutionengine.h"
#include "dtxn/ordered/orderedscheduler.h"
#include "dtxn/transactionstate.h"
#include "stupidunit/stupidunit.h"

using std::string;
using std::vector;

using namespace dtxn;

class OrderedSchedulerTest : public Test {
public:
    OrderedSchedulerTest() : scheduler_(&engine_), fragment_state_(NULL) {
        scheduler_.set_batching(false);
        engine_.result_ = "out";
        fragment_.multiple_partitions = false;
        fragment_.last_fragment = true;
        fragment_.transaction = "foo";
        fragment_.client_id = 1;
        fragment_.id = 1;
    }

    FragmentState* fragmentArrived(TransactionState* txn) {
        FragmentState* frag = txn->addFragment(fragment_);
        scheduler_.fragmentArrived(frag);
        return frag;
    }

    void orderFragmentNext(FragmentState* frag) {
        order_queue_.clear();
        scheduler_.order(&order_queue_);
        CHECK(order_queue_.size() == 1);
        CHECK(frag == order_queue_[0]);
        order_queue_.clear();
    }

    void addOrderExecuteFragment() {
        addOrderExecuteFragment(&txn_state_, &fragment_state_);
    }

    void addOrderExecuteFragment(TransactionState* txn, FragmentState** fragment) {
        *fragment = fragmentArrived(txn);
        orderFragmentNext(*fragment);
        scheduler_.execute(*fragment);
    }

    MockExecutionEngine engine_;
    OrderedScheduler scheduler_;

    TransactionState txn_state_;
    Fragment fragment_;
    FragmentState* fragment_state_;
    vector<FragmentState*> order_queue_;
};

TEST_F(OrderedSchedulerTest, BadCreate) {
    EXPECT_DEATH(OrderedScheduler(NULL));
}

TEST_F(OrderedSchedulerTest, IdleNothingToDo) {
    // No work queued: idle callback returns false to block on network.
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_FALSE(scheduler_.idle());
}

TEST_F(OrderedSchedulerTest, SinglePartitionCommit) {
    fragment_state_ = fragmentArrived(&txn_state_);

    // Nothing is done yet
    EXPECT_EQ(0, engine_.execute_count_);
    EXPECT_EQ(NULL, scheduler_.executed());

    // Order: get the transaction
    scheduler_.order(&order_queue_);
    ASSERT_EQ(1, order_queue_.size());
    EXPECT_EQ(fragment_state_, order_queue_[0]);
    order_queue_.clear();

    // Now execute the transaction
    scheduler_.execute(fragment_state_);
    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(ExecutionEngine::OK, fragment_state_->response().status);
    EXPECT_EQ("out", fragment_state_->response().result);

    // Can't commit or abort single partition transactions
    EXPECT_DEATH(scheduler_.commit(&txn_state_));
    EXPECT_DEATH(scheduler_.abort(&txn_state_));
}

TEST_F(OrderedSchedulerTest, SinglePartitionAbort) {
    // Next work: aborted
    engine_.commit_ = false;
    addOrderExecuteFragment();

    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(ExecutionEngine::ABORT_USER, fragment_state_->response().status);
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_DEATH(scheduler_.commit(&txn_state_));
    EXPECT_DEATH(scheduler_.abort(&txn_state_));
}

TEST_F(OrderedSchedulerTest, OrderSinglePartitionLimit) {
    // Queue more transactions than our limit
    TransactionState txns[OrderedScheduler::MAX_TXNS_PER_LOOP + 1];
    for (int i = 0; i < base::arraySize(txns); ++i) {
        fragmentArrived(&txns[i]);
    }

    // Order and execute the first batch
    scheduler_.order(&order_queue_);
    EXPECT_EQ(OrderedScheduler::MAX_TXNS_PER_LOOP, order_queue_.size());
    for (size_t i = 0; i < order_queue_.size(); ++i) {
        scheduler_.execute(order_queue_[i]);
    }
    order_queue_.clear();

    EXPECT_TRUE(scheduler_.idle());
    scheduler_.order(&order_queue_);
    EXPECT_EQ(1, order_queue_.size());
    EXPECT_FALSE(scheduler_.idle());
}

TEST_F(OrderedSchedulerTest, OrderMultiPartitionLimit) {
    // Queue more transactions than our limit
    TransactionState sp_txns[OrderedScheduler::MAX_TXNS_PER_LOOP + 1];
    for (int i = 0; i < base::arraySize(sp_txns); ++i) {
        fragmentArrived(&sp_txns[i]);
    }

    // Queue one multi-partition transaction
    fragment_.multiple_partitions = true;
    fragment_state_ = fragmentArrived(&txn_state_);

    // Order and execute the first batch
    scheduler_.order(&order_queue_);
    EXPECT_EQ(OrderedScheduler::MAX_TXNS_PER_LOOP, order_queue_.size());
    for (size_t i = 0; i < order_queue_.size(); ++i) {
        if (i == 0) {
            EXPECT_EQ(fragment_state_, order_queue_[i]);
        } else {
            EXPECT_EQ(&sp_txns[i-1].last_fragment(), order_queue_[i]);
        }
        scheduler_.execute(order_queue_[i]);
    }
    order_queue_.clear();

    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(NULL, scheduler_.executed());
    
    EXPECT_TRUE(scheduler_.idle());
    scheduler_.order(&order_queue_);
    ASSERT_EQ(1, order_queue_.size());
    scheduler_.execute(order_queue_[0]);
    order_queue_.clear();

    // block: waiting for multi-partition transaction
    EXPECT_FALSE(scheduler_.idle());
    scheduler_.order(&order_queue_);
    EXPECT_EQ(0, order_queue_.size());

    // abort the multi-partition transaction in order to free undo
    scheduler_.abort(&txn_state_);

    for (int i = 0; i < base::arraySize(sp_txns) - 1; ++i) {
        EXPECT_NE(NULL, scheduler_.executed());
    }
    EXPECT_TRUE(scheduler_.idle());
    scheduler_.order(&order_queue_);
    EXPECT_EQ(1, order_queue_.size());
}

TEST_F(OrderedSchedulerTest, SpeculateLimit) {
    scheduler_.cc_mode(OrderedScheduler::CC_SPECULATIVE);

    // Queue more transactions than our limit
    TransactionState sp_txns[OrderedScheduler::MAX_TXNS_PER_LOOP + 1];
    for (int i = 0; i < base::arraySize(sp_txns); ++i) {
        fragmentArrived(&sp_txns[i]);
    }

    // Queue one multi-partition transaction
    fragment_.multiple_partitions = true;
    fragment_state_ = fragmentArrived(&txn_state_);

    // Order and execute the first batch
    scheduler_.order(&order_queue_);
    EXPECT_EQ(OrderedScheduler::MAX_TXNS_PER_LOOP, order_queue_.size());
    for (size_t i = 0; i < order_queue_.size(); ++i) {
        if (i == 0) {
            EXPECT_EQ(fragment_state_, order_queue_[i]);
        } else {
            EXPECT_EQ(&sp_txns[i-1].last_fragment(), order_queue_[i]);
        }
        scheduler_.execute(order_queue_[i]);
    }
    order_queue_.clear();

    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(NULL, scheduler_.executed());
    
    EXPECT_TRUE(scheduler_.idle());
    scheduler_.order(&order_queue_);
    ASSERT_EQ(2, order_queue_.size());
    scheduler_.execute(order_queue_[0]);
    scheduler_.execute(order_queue_[1]);
    order_queue_.clear();
    EXPECT_EQ(NULL, scheduler_.executed());

    // block: waiting for multi-partition transaction
    EXPECT_FALSE(scheduler_.idle());
    scheduler_.order(&order_queue_);
    EXPECT_EQ(0, order_queue_.size());

    // abort the multi-partition transaction in order to free undo
    scheduler_.abort(&txn_state_);

    for (int i = 0; i < base::arraySize(sp_txns); ++i) {
        EXPECT_NE(NULL, scheduler_.executed());
    }
    EXPECT_FALSE(scheduler_.idle());
    scheduler_.order(&order_queue_);
    EXPECT_EQ(0, order_queue_.size());
}

TEST_F(OrderedSchedulerTest, MultiPhaseCommit) {
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;

    addOrderExecuteFragment();
    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());
    EXPECT_EQ(ExecutionEngine::OK, fragment_state_->response().status);

    // add another fragment
    addOrderExecuteFragment();
    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());

    scheduler_.commit(&txn_state_);
    EXPECT_EQ(1, engine_.free_count_);
    EXPECT_EQ(NULL, scheduler_.executed());
}

TEST_F(OrderedSchedulerTest, MultiPhaseSinglePartitionCommit) {
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;

    addOrderExecuteFragment();
    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());
    EXPECT_EQ(ExecutionEngine::OK, fragment_state_->response().status);

    // add another fragment: downgraded to single partition
    fragment_.multiple_partitions = false;
    fragment_.last_fragment = true;
    addOrderExecuteFragment();
    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(1, engine_.free_count_);

    // Cannot commit this: it was single partition committed!
    // NOTE: single partition commit is different than (prepare, commit), which would
    // be multiple_partition = true; last_fragment =  true.
    EXPECT_DEATH(scheduler_.commit(&txn_state_));
}

TEST_F(OrderedSchedulerTest, MultiPhaseUserAbort) {
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;

    // execute work that wil abort
    engine_.commit_ = false;
    addOrderExecuteFragment();
    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(ExecutionEngine::ABORT_USER, fragment_state_->response().status);

    // cannot commit aborted transactions!
    EXPECT_DEATH(scheduler_.commit(&txn_state_));

    // cannot add to aborted transactions
    EXPECT_DEATH(txn_state_.addFragment(fragment_));

    scheduler_.abort(&txn_state_);
    EXPECT_EQ(1, engine_.undo_count_);
    EXPECT_EQ(NULL, scheduler_.executed());
}

TEST_F(OrderedSchedulerTest, MultiPartitionQueueTransactions) {
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    addOrderExecuteFragment();
    EXPECT_EQ(fragment_state_, scheduler_.executed());

    // 2 SP transactions are queued
    fragment_.multiple_partitions = false;
    fragment_.last_fragment = true;
    TransactionState sp1;
    FragmentState* sp1_frag;
    addOrderExecuteFragment(&sp1, &sp1_frag);
    TransactionState sp2;
    FragmentState* sp2_frag;
    addOrderExecuteFragment(&sp2, &sp2_frag);
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_EQ(1, engine_.execute_count_);

    // The first transaction commits, the others get executed (after idle)
    scheduler_.commit(&txn_state_);
    EXPECT_EQ(3, engine_.execute_count_);
    EXPECT_EQ(sp1_frag, scheduler_.executed());
    EXPECT_EQ(sp2_frag, scheduler_.executed());
    EXPECT_EQ(NULL, scheduler_.executed());
}

#ifdef FOO
TEST_F(OrderedSchedulerTest, AbortQueuedSPTransaction) {
    // queue an SP transaction
    Transaction* txn = scheduler_.begin(NULL, "foo", true, false);
    EXPECT_EQ(0, engine_.execute_count_);
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());

    // trying to commit the queued transaction: death
    EXPECT_DEATH(scheduler_.commit(txn));

    // abort the queued transaction: this works
    scheduler_.abort(txn);

    EXPECT_FALSE(scheduler_.idle());
}

TEST_F(OrderedSchedulerTest, AbortQueuedMPTransaction) {
    Transaction* txn = scheduler_.begin(NULL, "foo", false, true);
    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(txn, scheduler_.getWorkUnitResult());

    // an MP transaction is queued
    Transaction* mp = scheduler_.begin(NULL, "foo", false, true);
    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());

    // trying to commit the queued transaction: death
    EXPECT_DEATH(scheduler_.commit(mp));

    // abort the queued transaction: this works
    scheduler_.abort(mp);

    // The first transaction commits, nothing is executed
    scheduler_.commit(txn);
    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_EQ(1, engine_.execute_count_);

    // Adding another transaction works
    txn = scheduler_.begin(NULL, "foo", false, true);
    EXPECT_EQ(2, engine_.execute_count_);
}

TEST_F(OrderedSchedulerTest, AbortUnsentTransaction) {
    // Start a transaction that is executed
    Transaction* txn = scheduler_.begin(NULL, "foo", false, true);
    EXPECT_EQ(1, engine_.execute_count_);

    // Abort the transaction, such as via timeout: removed from the result queue
    // This happens in rare cases when the queue builds up such that the message to start a
    // transaction ends up in the same batch as the message to abort it. We end up executing
    // then immediately aborting. Somewhat wasteful, but this is very rare.
    // TODO: Should DtxnServer handle this, since now each scheduler needs to handle it?
    // TODO: Should be a way to share this test between schedulers?
    scheduler_.abort(txn);
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());
}
#endif

TEST_F(OrderedSchedulerTest, BadCCMode) {
    int bad_cc_value = -1;
    EXPECT_DEATH(scheduler_.cc_mode((OrderedScheduler::ConcurrencyControl) bad_cc_value));

    // Cannot change concurrency while transactions are active
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    addOrderExecuteFragment();
    EXPECT_DEATH(scheduler_.cc_mode(OrderedScheduler::CC_SPECULATIVE));
    scheduler_.abort(&txn_state_);
}

// MP txn, SP txn: both commit
TEST_F(OrderedSchedulerTest, SpeculativeExecutionCommit) {
    scheduler_.cc_mode(OrderedScheduler::CC_SPECULATIVE);

    // Execute a multiple partition transaction that has the "done" bit set
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = true;
    addOrderExecuteFragment();
    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());

    // Receive a transaction from another client: it should be speculatively executed (after idle)
    fragment_.multiple_partitions = false;
    TransactionState sp1;
    FragmentState* sp1_frag;
    addOrderExecuteFragment(&sp1, &sp1_frag);
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(2, engine_.undo_stack_.size());

    // This will *not* send out a response
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_FALSE(scheduler_.idle());

    // The transaction commits
    scheduler_.commit(&txn_state_);

    // *now* the other client gets a response
    EXPECT_EQ(sp1_frag, scheduler_.executed());
    EXPECT_EQ(2, engine_.execute_count_);  // no re-execution
    EXPECT_EQ(0, engine_.undo_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(2, engine_.free_count_);
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_FALSE(scheduler_.idle());
}

// MP1, MP2, MP3: MP1 aborts, MP2 and MP3 are reexecuted and commit
TEST_F(OrderedSchedulerTest, SpeculativeExecutionAbort) {
    scheduler_.cc_mode(OrderedScheduler::CC_SPECULATIVE);

    // Execute a multiple partition transaction that has the "done" bit set
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = true;
    addOrderExecuteFragment();
    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());

    // Receive a transactions from another client: it should be speculatively executed
    TransactionState mp2;
    FragmentState* mp2_frag;
    addOrderExecuteFragment(&mp2, &mp2_frag);
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(2, engine_.undo_stack_.size());
    TransactionState mp3;
    FragmentState* mp3_frag;
    addOrderExecuteFragment(&mp3, &mp3_frag);
    EXPECT_EQ(3, engine_.execute_count_);
    EXPECT_EQ(3, engine_.undo_stack_.size());
    
    // This will *not* send out a response
    EXPECT_EQ(NULL, scheduler_.executed());

    // The first transaction aborts
    scheduler_.abort(&txn_state_);

    EXPECT_EQ(mp2_frag, scheduler_.executed());
    EXPECT_EQ(5, engine_.execute_count_);
    EXPECT_EQ(3, engine_.undo_count_);
    EXPECT_EQ(2, engine_.undo_stack_.size());
    EXPECT_FALSE(scheduler_.idle());

    // A 4th mp transaction arrives: must be ordered after mp3
    TransactionState mp4;
    FragmentState* mp4_frag;
    addOrderExecuteFragment(&mp4, &mp4_frag);

    scheduler_.commit(&mp2);
    EXPECT_EQ(mp3_frag, scheduler_.executed());
    EXPECT_EQ(1, engine_.free_count_);
    EXPECT_EQ(2, engine_.undo_stack_.size());
    EXPECT_FALSE(scheduler_.idle());

    scheduler_.commit(&mp3);
    scheduler_.commit(&mp4);
}

TEST_F(OrderedSchedulerTest, SpeculativeExecutionNullUndo) {
    scheduler_.cc_mode(OrderedScheduler::CC_SPECULATIVE);

    // Execute a multiple partition transaction that has the "done" bit set, and no undo
    engine_.null_undo_ = true;
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = true;
    addOrderExecuteFragment();
    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    // This will send out a response
    EXPECT_EQ(fragment_state_, scheduler_.executed());

    // Receive a transaction from another client: it should be speculatively executed (after idle)
    // make this transaction speculatively abort
    fragment_.multiple_partitions = false;
    engine_.commit_ = false;
    TransactionState sp;
    FragmentState* sp_frag = NULL;
    addOrderExecuteFragment(&sp, &sp_frag);
    engine_.commit_ = true;
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());

    // The transaction commits
    scheduler_.commit(&txn_state_);

    // *now* the other client gets a response
    EXPECT_EQ(sp_frag, scheduler_.executed());
    EXPECT_EQ(ExecutionEngine::ABORT_USER, sp_frag->response().status);
    EXPECT_EQ(2, engine_.execute_count_);  // no re-execution
    EXPECT_EQ(0, engine_.undo_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(0, engine_.free_count_);

    // Begin another multi-partition
    fragment_.multiple_partitions = true;
    txn_state_.clear();
    addOrderExecuteFragment();
    EXPECT_EQ(3, engine_.execute_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(ExecutionEngine::OK, fragment_state_->response().status);

    // Add a single parition that is speculated on top
    fragment_.multiple_partitions = false;
    sp.clear();
    addOrderExecuteFragment(&sp, &sp_frag);
    EXPECT_EQ(4, engine_.execute_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());

    // abort the mp txn
    scheduler_.abort(&txn_state_);

    // results should be returned after idle
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_EQ(5, engine_.execute_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(sp_frag, scheduler_.executed());
    EXPECT_EQ(ExecutionEngine::OK, sp_frag->response().status);
}

// MP txn arrives but is not done, SP txn arrives, MP txn finishes, SP is speculatively executed
TEST_F(OrderedSchedulerTest, SpeculativeGeneralTxnQueued) {
    scheduler_.cc_mode(OrderedScheduler::CC_SPECULATIVE);

    // Start the MP transaction
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    addOrderExecuteFragment();
    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());
    // This will send out a response
    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(ExecutionEngine::OK, fragment_state_->response().status);

    // Receive an SP transaction: it must be queued
    fragment_.multiple_partitions = false;
    fragment_.last_fragment = true;
    TransactionState sp;
    FragmentState* sp_frag;
    addOrderExecuteFragment(&sp, &sp_frag);
    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());
    EXPECT_EQ(NULL, scheduler_.executed());

    // The MP transaction finishes, SP is speculatively executed
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = true;
    FragmentState* mp_frag2 = NULL;
    addOrderExecuteFragment(&txn_state_, &mp_frag2);
    EXPECT_EQ(mp_frag2, scheduler_.executed());
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_EQ(3, engine_.execute_count_);
    EXPECT_EQ(2, engine_.undo_stack_.size());

    // mp commits: sp is output
    scheduler_.commit(&txn_state_);
    EXPECT_EQ(sp_frag, scheduler_.executed());
    EXPECT_EQ(NULL, scheduler_.executed());
}

// one shot mp txn starts, general mp txn is speculated
TEST_F(OrderedSchedulerTest, SpeculateGeneralTxn) {
    scheduler_.cc_mode(OrderedScheduler::CC_SPECULATIVE);

    // Start the MP transaction
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = true;
    addOrderExecuteFragment();
    EXPECT_EQ(fragment_state_, scheduler_.executed());

    // Receive an MP and an SP transaction: MP is speculated then SP speculated.
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    TransactionState mp2;
    FragmentState* mp2_frag1 = NULL;
    addOrderExecuteFragment(&mp2, &mp2_frag1);
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(2, engine_.undo_stack_.size());
    EXPECT_EQ(NULL, scheduler_.executed());

    // Receive sp transaction: blocked
    fragment_.multiple_partitions = false;
    fragment_.last_fragment = true;
    TransactionState sp;
    FragmentState* sp_frag = NULL;
    addOrderExecuteFragment(&sp, &sp_frag);
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(2, engine_.undo_stack_.size());
    EXPECT_EQ(NULL, scheduler_.executed());

    // The original MP transaction commits, mp2 is output, sp is still blocked
    scheduler_.commit(&txn_state_);
    EXPECT_EQ(mp2_frag1, scheduler_.executed());
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_EQ(1, engine_.free_count_);
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());

    // mp2 finishes, sp is speculated
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = true;
    FragmentState* mp2_frag2 = NULL;
    addOrderExecuteFragment(&mp2, &mp2_frag2);
    EXPECT_EQ(mp2_frag2, scheduler_.executed());
    EXPECT_EQ(4, engine_.execute_count_);
    EXPECT_EQ(2, engine_.undo_stack_.size());

    // commit mp2; sp is output
    scheduler_.commit(&mp2);
    EXPECT_EQ(3, engine_.free_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(sp_frag, scheduler_.executed());
}

#ifdef FOOBAR
// mp txn starts, SP txn 1 speculatively aborts, sp txn 2 commits, mp txn commits
TEST_F(OrderedSchedulerTest, SpeculativeAbort) {
    scheduler_.cc_mode(OrderedScheduler::CC_SPECULATIVE);

    // Start the one-shot MP transaction
    Transaction* mp = scheduler_.begin(NULL, "foo", true, true);
    EXPECT_EQ(mp, scheduler_.getWorkUnitResult());
    mp->resultSent();

    // sp transaction speculatively aborts
    engine_.commit_ = false;
    Transaction* sp = scheduler_.begin(NULL, "foo", true, false);
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());

    // sp transaction commits
    engine_.commit_ = true;
    Transaction* sp2 = scheduler_.begin(NULL, "foo", true, false);
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_EQ(3, engine_.execute_count_);
    EXPECT_EQ(2, engine_.undo_stack_.size());
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());
    
    // The MP transaction commits, sp sp2 are output
    scheduler_.commit(mp);
    EXPECT_EQ(sp, scheduler_.getWorkUnitResult());
    EXPECT_EQ(ExecutionEngine::ABORT_USER, sp->last_status());
    EXPECT_EQ(sp2, scheduler_.getWorkUnitResult());
    EXPECT_EQ(ExecutionEngine::OK, sp2->last_status());
    EXPECT_EQ(2, engine_.free_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    
    scheduler_.abort(sp);
    scheduler_.commit(sp2);
}

// mp txn starts, SP txn speculatively aborts, mp txn aborts, sp is re-executed
TEST_F(OrderedSchedulerTest, SpeculativeAbortReexecute) {
    scheduler_.cc_mode(OrderedScheduler::CC_SPECULATIVE);

    // Start the one-shot MP transaction
    Transaction* mp = scheduler_.begin(NULL, "foo", true, true);
    EXPECT_EQ(mp, scheduler_.getWorkUnitResult());
    mp->resultSent();

    // sp transaction speculatively aborts
    engine_.commit_ = false;
    Transaction* sp = scheduler_.begin(NULL, "foo", true, false);
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());
    EXPECT_EQ(1, engine_.undo_count_);
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());
    engine_.commit_ = true;

    // The MP transaction aborts, sp is output after idle
    scheduler_.abort(mp);
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_EQ(3, engine_.execute_count_);
    EXPECT_EQ(2, engine_.undo_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    ASSERT_EQ(sp, scheduler_.getWorkUnitResult());
    EXPECT_EQ(ExecutionEngine::OK, sp->last_status());   
    scheduler_.commit(sp);
}
#endif

// c1, c1, c1, c2, c2, c1
TEST_F(OrderedSchedulerTest, DependentSpeculationCommit) {
    scheduler_.cc_mode(OrderedScheduler::CC_SPECULATIVE);
    scheduler_.set_batching(true);

    // Execute a multiple partition transaction
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = true;
    addOrderExecuteFragment();
    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());

    // MP transaction from same client: speculated
    fragment_.id = 2;
    TransactionState mp2;
    FragmentState* mp2_frag;
    addOrderExecuteFragment(&mp2, &mp2_frag);
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(2, engine_.undo_stack_.size());
    EXPECT_EQ(mp2_frag, scheduler_.executed());
    EXPECT_EQ(1, mp2_frag->response().dependency);

    fragment_.id = 3;
    TransactionState mp3;
    FragmentState* mp3_frag;
    addOrderExecuteFragment(&mp3, &mp3_frag);
    EXPECT_EQ(3, engine_.execute_count_);
    EXPECT_EQ(3, engine_.undo_stack_.size());
    EXPECT_EQ(mp3_frag, scheduler_.executed());
    EXPECT_EQ(2, mp3_frag->response().dependency);

    // Begin an MP transaction from different coordinator
    fragment_.client_id = 2;
    fragment_.id = 0;
    TransactionState mp_other1;
    FragmentState* mp_other1_frag;
    addOrderExecuteFragment(&mp_other1, &mp_other1_frag);
    EXPECT_EQ(4, engine_.execute_count_);
    EXPECT_EQ(4, engine_.undo_stack_.size());
    // This will *not* send out a response
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_FALSE(scheduler_.idle());

    // #2 from the 2nd coordinator
    fragment_.id = 1;
    TransactionState mp_other2;
    FragmentState* mp_other2_frag;
    addOrderExecuteFragment(&mp_other2, &mp_other2_frag);
    EXPECT_EQ(5, engine_.execute_count_);
    EXPECT_EQ(5, engine_.undo_stack_.size());
    // This will *not* send out a response
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_FALSE(scheduler_.idle());

    // Another MP transaction from original coordinator
    fragment_.client_id = 1;
    fragment_.id = 4;
    TransactionState mp4;
    FragmentState* mp4_frag;
    addOrderExecuteFragment(&mp4, &mp4_frag);
    EXPECT_EQ(6, engine_.execute_count_);
    EXPECT_EQ(6, engine_.undo_stack_.size());
    // This will *not* send out a response
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_FALSE(scheduler_.idle());

    // The first batch of transactions commit
    scheduler_.commit(&txn_state_);
    EXPECT_EQ(NULL, scheduler_.executed());
    scheduler_.commit(&mp2);
    EXPECT_EQ(NULL, scheduler_.executed());
    scheduler_.commit(&mp3);

    // *now* the other coordinator gets a batch
    EXPECT_EQ(mp_other1_frag, scheduler_.executed());
    EXPECT_EQ(-1, mp_other1_frag->response().dependency);
    EXPECT_EQ(6, engine_.execute_count_);  // no re-execution
    EXPECT_EQ(0, engine_.undo_count_);
    EXPECT_EQ(3, engine_.undo_stack_.size());
    EXPECT_EQ(3, engine_.free_count_);
    EXPECT_EQ(mp_other2_frag, scheduler_.executed());
    EXPECT_EQ(mp_other1_frag->request().id, mp_other2_frag->response().dependency);
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_FALSE(scheduler_.idle());

    // The transactions commits
    scheduler_.commit(&mp_other1);
    scheduler_.commit(&mp_other2);

    // *now* the original client gets a response
    EXPECT_EQ(mp4_frag, scheduler_.executed());
    EXPECT_EQ(-1, mp4_frag->response().dependency);
    EXPECT_EQ(6, engine_.execute_count_);  // no re-execution
    EXPECT_EQ(0, engine_.undo_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());
    EXPECT_EQ(5, engine_.free_count_);
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_FALSE(scheduler_.idle());
    scheduler_.commit(&mp4);
}

TEST_F(OrderedSchedulerTest, DependentSpeculationAbort) {
    scheduler_.cc_mode(OrderedScheduler::CC_SPECULATIVE);
    scheduler_.set_batching(true);

    // Execute 3 multiple partition transactions
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = true;
    addOrderExecuteFragment();
    EXPECT_EQ(fragment_state_, scheduler_.executed());
    EXPECT_EQ(-1, fragment_state_->response().dependency);

    fragment_.id = 2;
    TransactionState mp2;
    FragmentState* mp2_frag;
    addOrderExecuteFragment(&mp2, &mp2_frag);
    EXPECT_EQ(mp2_frag, scheduler_.executed());
    EXPECT_EQ(fragment_state_->request().id, mp2_frag->response().dependency);

    fragment_.id = 3;
    TransactionState mp3;
    FragmentState* mp3_frag;
    addOrderExecuteFragment(&mp3, &mp3_frag);
    EXPECT_EQ(3, engine_.execute_count_);
    EXPECT_EQ(3, engine_.undo_stack_.size());
    EXPECT_EQ(mp3_frag, scheduler_.executed());
    EXPECT_EQ(mp2_frag->request().id, mp3_frag->response().dependency);
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_FALSE(scheduler_.idle());

    // abort the first: remaining are re-executed
    scheduler_.abort(&txn_state_);
    EXPECT_EQ(mp2_frag, scheduler_.executed());
    EXPECT_EQ(-1, mp2_frag->response().dependency);
    EXPECT_EQ(mp3_frag, scheduler_.executed());
    EXPECT_EQ(mp2_frag->request().id, mp3_frag->response().dependency);
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_EQ(5, engine_.execute_count_);
    EXPECT_EQ(3, engine_.undo_count_);
    EXPECT_EQ(2, engine_.undo_stack_.size());

    // abort again
    scheduler_.abort(&mp2);
    EXPECT_EQ(mp3_frag, scheduler_.executed());
    EXPECT_EQ(-1, mp3_frag->response().dependency);
    EXPECT_EQ(NULL, scheduler_.executed());
    EXPECT_EQ(6, engine_.execute_count_);
    EXPECT_EQ(5, engine_.undo_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());

    scheduler_.abort(&mp3);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
