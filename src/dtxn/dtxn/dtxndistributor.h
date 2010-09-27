// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXNDISTRIBUTOR_H
#define DTXNDISTRIBUTOR_H

#include <cassert>
#include <string>
#include <vector>

#include <stdint.h>

#include "dtxn/distributedtransaction.h"
#include "dtxn/executionengine.h"  // ExecutionEngine::Status

namespace dtxn {

/** Tracks information about a transaction that is being coordinated by the distributor. */
// TODO: Replace this with DistributedTransaction
class CoordinatedTransaction {
public:
    CoordinatedTransaction(size_t num_partitions, const std::string& initial_client_message) :
            status_(ExecutionEngine::OK),
            partition_status_(num_partitions),
            received_messages_(1, std::make_pair(CLIENT_PARTITION, initial_client_message)),
            multiple_partitions_(false),
            state_(NULL) {
        assert(partition_status_.size() > 0);
    }

    // Special index for the client partition.
    static const int CLIENT_PARTITION = -1;

    typedef std::vector<std::pair<int, std::string> > MessageList;

    /** Returns the list of messages received in the last round. */ 
    const MessageList& received() const { return received_messages_; }

    /** Returns the list of messages sent in the last round. */
    const MessageList& sent() const { return sent_messages_; }

    void setMultiplePartitions() {
        assert(!isAllDone() || sent_messages_.size() > 1);
        assert(!multiple_partitions_);
        multiple_partitions_ = true;
    }

    bool multiple_partitions() const __attribute__((warn_unused_result)) {
        return multiple_partitions_;
    }

    void sentMessages() {
        assert(!sent_messages_.empty());
        received_messages_.clear();
    }

    bool isDone(int index) const __attribute__((warn_unused_result)) {
        assert(0 <= index && index < partition_status_.size());
        return !partition_status_[index].active();
    }

    bool isAllDone() const __attribute__((warn_unused_result)) {
        for (int i = 0; i < partition_status_.size(); ++i) {
            if (!partition_status_[i].isDone()) {
                return false;
            }
        }
        return true;
    }

    // Returns true if the coordinator has provided the final client response.
    bool haveClientResponse() const __attribute__((warn_unused_result)) {
        if (sent_messages_.size() == 1 && sent_messages_[0].first == CLIENT_PARTITION) {
            assert(isAllDone());
            return true;
        }
        return false;
    }

    void setAllDone() {
        for (int i = 0; i < partition_status_.size(); ++i) {
            if (partition_status_[i].active() || partition_status_[i].unknown()) {
                partition_status_[i].setDone();
            }
        }
    }

    ExecutionEngine::Status status() const __attribute__((warn_unused_result)) { return status_; }

    void commit(const std::string& client_response) {
        // NOTE: client_response might be empty if transaction has no results (just commit/abort)
        assert(sent_messages_.empty());
        assert(status_ == ExecutionEngine::OK);
        setAllDone();
        sent_messages_.push_back(std::make_pair(CLIENT_PARTITION, client_response));
    }

    void abort(const std::string& client_response) {
        // Can only abort or commit once, but we can abort while sending or receiving
        assert(!haveClientResponse());
        assert(status_ != ExecutionEngine::ABORT_DEADLOCK || client_response.empty());
        setAllDone();
        sent_messages_.clear();
        sent_messages_.push_back(std::make_pair(CLIENT_PARTITION, client_response));
        if (status_ == ExecutionEngine::OK) status_ = ExecutionEngine::ABORT_USER;
    }
    
    void send(int index, const std::string& message) {
        assert(0 <= index && index < partition_status_.size());
        assert(!message.empty());
        assert(!containsKey(sent_messages_, index));
        assert(!received_messages_.empty());
        assert(status_ == ExecutionEngine::OK);
        partition_status_[index].setActive();
        sent_messages_.push_back(std::make_pair(index, message));
    }
    
    void receive(int index, const std::string& message, ExecutionEngine::Status status) {
        assert(0 <= index && index < partition_status_.size());
        assert(!containsKey(received_messages_, index));
        assert(containsKey(sent_messages_, index));
        assert(status != ExecutionEngine::ABORT_DEADLOCK || message.empty());
        assert(status == ExecutionEngine::OK || status == ExecutionEngine::ABORT_USER ||
                status == ExecutionEngine::ABORT_DEADLOCK);

        // Priority, from lowest to highest: ExecutionEngine::OK, ExecutionEngine::ABORT_USER,
        // ExecutionEngine::ABORT_DEADLOCK
        if ((status_ == ExecutionEngine::OK && status != ExecutionEngine::OK) ||
                (status_ == ExecutionEngine::ABORT_USER &&
                status == ExecutionEngine::ABORT_DEADLOCK)) {
            // forget any messages we have received
            for (size_t i = 0; i < received_messages_.size(); ++i) {
                received_messages_[i].second.clear();
            }
            status_ = status;
        }

        // Keep the message only if the status matches our current status
        received_messages_.push_back(std::make_pair(index, std::string()));        
        if (status_ == status) {
            received_messages_.back().second = message;
        }

        assert(received_messages_.size() <= sent_messages_.size());
    }

    // Remove the last response received by index. Used when aborting a dependency.
    void removeResponse(int index) {
        for (int i = 0; i < received_messages_.size(); ++i) {
            if (received_messages_[i].first == index) {
                received_messages_.erase(received_messages_.begin() + i);
                return;
            }
        }
        // we didn't find index: should not happen
        assert(false);
    }

    bool hasResponse(int index) const __attribute__((warn_unused_result)) {
        for (int i = 0; i < received_messages_.size(); ++i) {
            if (received_messages_[i].first == index) {
                return true;
            }
        }
        return false;
    }

    // The transaction is about to move to the next round of communication. This is not part of
    // receive in order to support transactions with dependencies, which need to be able to abort
    // and undo a receive.
    void readyNextRound() {
        assert(received_messages_.size() == sent_messages_.size());
        sent_messages_.clear();
    }

    bool receivedAll() const __attribute__((warn_unused_result)) {
        return received_messages_.size() == sent_messages_.size();
    }

    int numInvolved() const __attribute__((warn_unused_result)) {
        int result = 0;
        for (int i = 0; i < partition_status_.size(); ++i) {
            result += partition_status_[i].isParticipant();
        }
        return result;
    }

    bool isPartitionInvolved(int index) const __attribute((warn_unused_result)) {
        return partition_status_[index].isParticipant();
    }

    std::vector<int> involvedPartitions() const __attribute__((warn_unused_result)) {
        std::vector<int> result;
        for (int i = 0; i < partition_status_.size(); ++i) {
            if (partition_status_[i].isParticipant()) result.push_back(i);
        }
        return result;
    }

    void state(void* next_state) { state_ = next_state; }
    void* state() { return state_; }

private:
    bool containsKey(const MessageList& messages, int index) const
            __attribute__((warn_unused_result)) {
        for (int i = 0; i < messages.size(); ++i) {
            if (messages[i].first == index) return true;
        }
        return false;
    }

    // The current status of the transaction
    ExecutionEngine::Status status_;

    // Stores the status of each individual partition.
    std::vector<PartitionState> partition_status_;

    MessageList received_messages_;

    // Next round of messages to be sent out
    MessageList sent_messages_;

    bool multiple_partitions_;

    // Opaque pointer for use by the Coordinator.
    void* state_;
};

/** Interface for the asynchronous transaction manager. */
class TransactionManager {
public:
    virtual ~TransactionManager() {}

    /** Send a round of communication for transaction. The callback will be invoked once al
    responses are received. */
    virtual void sendRound(CoordinatedTransaction* transaction) = 0;

    /** Commit or abort this transaction. To commit, all pending requests must
    have completed. An abort can be triggered at any time.
    virtual void finish(CoordinatedTransaction* transaction, bool commit,
            const std::tr1::function<void()>& callback) = 0;
    */
};

/** Interface for the transaction coordinator. This decides how to distribute transactions to
partitions of the database. */
class Coordinator {
public:
    Coordinator() : manager_(NULL) {}
    virtual ~Coordinator() {}

    // A new transaction has been received from the client. The coordinator must supply the messages
    // to go out.
    virtual void begin(CoordinatedTransaction* transaction) = 0;

    // Responses for all the previous messages have been received. The coordinaor must supply the
    // next messages.
    // TODO: This is currently *not* called if a remote partition aborts. This does not work for
    // TPC-C new order, for example. In that case, an aborted transaction still returns data!
    virtual void nextRound(CoordinatedTransaction* transaction) = 0;

    // The transaction has completed, so the coordinator should perform any necessary clean-up.
    virtual void done(CoordinatedTransaction* transaction) = 0;

    // Sets the transaction manager this coordinator is attached to.
    void set_manager(TransactionManager* manager) {
        manager_ = manager;
    }

protected:
    void sendRound(CoordinatedTransaction* transaction) {
        manager_->sendRound(transaction);
    }

private:
    TransactionManager* manager_;
};

class MockTransactionManager : public TransactionManager {
public:
    MockTransactionManager(Coordinator* coordinator) {
        coordinator->set_manager(this);
    }

    virtual void sendRound(CoordinatedTransaction* transaction) {}
};

}  // namespace dtxn

#endif
