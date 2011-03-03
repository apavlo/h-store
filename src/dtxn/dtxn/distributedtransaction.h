// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_DISTRIBUTEDTRANSACTION_H__
#define DTXN_DISTRIBUTEDTRANSACTION_H__

#include <cassert>
#include <string>
#include <vector>

#include <stdint.h>

namespace dtxn {

/** Records the state of each partition that may or may not be involved in a transaction. */
/* GraphViz graph of these states: dot -Tps example.dot > out.ps
digraph G {
    node [shape=rectangle,fontname=Helvetica];
    edge [fontname=Helvetica];
    unknown -> active [label=write];
    unknown -> finished [label=done];

    active -> preparing [label=done];
    preparing -> prepared [label=ack];
    preparing -> finished [label="single partition ack"]
    prepared -> finished [label=commit];
}*/
class PartitionState {
public:
    PartitionState() : status_((int8_t) UNKNOWN) {}

    bool isParticipant() const { return status_ != UNKNOWN && status_ != FINISHED; }

    // Returns true if the transaction has no more work to do at this partition. However, this
    // partition may be involved in a two-phase commit, so more messages might need to
    // be exchanged.
    bool isDone() const { return !(status_ == UNKNOWN || status_ == ACTIVE); }

    bool unknown() const { return status_ == UNKNOWN; }
    bool active() const { return status_ == ACTIVE; }
    bool finished() const { return status_ == FINISHED; }
    bool preparing() const { return status_ == PREPARING; }
    bool prepared() const { return status_ == PREPARED; }

    void setActive() {
        assert(status_ == UNKNOWN || status_ == ACTIVE);
        status_ = ACTIVE;
    }

    void setDone() {
        assert(status_ == UNKNOWN || status_ == ACTIVE);
        if (status_ == UNKNOWN) {
            // Did nothing: we are just done
            status_ = FINISHED;
        } else {
            status_ = PREPARING;
        }
    }

    void setPrepared() {
        assert(status_ == PREPARING);
        status_ = PREPARED;
    }

    void setFinished() {
        assert(status_ == PREPARED);
        status_ = FINISHED;
    }

private:
    enum Status {
        UNKNOWN = 0,
        ACTIVE,
        PREPARING,
        PREPARED,
        FINISHED,
    };

    int8_t status_;
};

/** Tracks information about a transaction that is being coordinated by the distributor. */
class DistributedTransaction {
public:
    DistributedTransaction(size_t num_partitions) :
            status_(OK),
            partition_status_(num_partitions),
            received_count_(0),
            multiple_partitions_(true),
            state_(NULL) {
        assert(partition_status_.size() > 0);
    }

    enum Status {
        /** The transaction executed correctly and committed. */
        OK = 0,

        /** The transaction contained code which directed it to abort. */
        ABORT_USER = 1,

        /** A deadlock was detected. */
        ABORT_DEADLOCK = 2,
        
        /** PAVLO: Misprediction! */
        ABORT_MISPREDICT = 5,
    };

    typedef std::vector<std::pair<int, std::string> > MessageList;

    /** Returns the list of messages received in the last round. */ 
    const MessageList& received() const { return received_messages_; }

    /** Returns the list of messages sent in the last round. */
    const MessageList& sent() const { return sent_messages_; }

    bool multiple_partitions() const __attribute__((warn_unused_result)) {
        return multiple_partitions_;
    }

    void sentMessages() {
        assert(!sent_messages_.empty());
        received_messages_.clear();
    }

    void setDone(int index) {
        assert(validPartitionIndex(index));
        if (partition_status_[index].active() && !containsKey(sent_messages_, index)) {
            // Need to send an explicit prepare
            sent_messages_.push_back(std::make_pair(index, std::string()));
        }

        partition_status_[index].setDone();
    }

    bool isAllDone() const __attribute__((warn_unused_result)) {
        for (int i = 0; i < partition_status_.size(); ++i) {
            if (!partition_status_[i].isDone()) {
                return false;
            }
        }
        return true;
    }

    /** Mark all partitions as being finished with this transaction. NOTE: This sends empty
    "prepare" messages to any active participants without a message in this round. */
    void setAllDone() {
        int participants = 0;
        for (int i = 0; i < partition_status_.size(); ++i) {
            // this partition could already be done: eg. it was explicitly marked done earlier
            if (!partition_status_[i].isDone()) {
                setDone(i);
            }

            if (partition_status_[i].isParticipant()) {
                participants += 1;
            }
        }

        assert(participants > 0);
        if (participants == 1) {
            multiple_partitions_ = false;
        }
    }

    Status status() const __attribute__((warn_unused_result)) { return status_; }

    /** Send message to partition index. NOTE: Prepare messages are created by setAllDone(). */
    void send(int index, const std::string& message) {
        assert(validPartitionIndex(index));
        assert(!message.empty());
        assert(!containsKey(sent_messages_, index));
        assert(status_ == OK);
        assert(received_count_ == 0);
        partition_status_[index].setActive();
        sent_messages_.push_back(std::make_pair(index, message));
    }
    
    void receive(int index, const std::string& message, Status status) {
        assert(validPartitionIndex(index));
        assert(!containsKey(received_messages_, index));
        assert(containsKey(sent_messages_, index));
        // If this is a prepare response, it must be empty
        assert(!findPartition(sent_messages_, index)->second.empty() ||
                message.empty());
        assert(status != ABORT_DEADLOCK || message.empty());
        assert(status == OK || status == ABORT_USER || status == ABORT_MISPREDICT ||
                status == ABORT_DEADLOCK);
        assert(partition_status_[index].isParticipant());

        // Priority, from lowest to highest: OK, ABORT_USER, ABORT_DEADLOCK
        if (status_ < status) {
            // forget any messages we have received
            received_messages_.clear();
            status_ = status;
        }

        // Keep the message only if the status matches our current status
        if (status_ == status) {
            received_messages_.push_back(std::make_pair(index, message));
        }
        received_count_ += 1;
        assert(received_messages_.size() <= sent_messages_.size());
        assert(received_count_ <= sent_messages_.size());

        // If we were preparing, this partition is now prepared
        if (partition_status_[index].preparing()) {
            partition_status_[index].setPrepared();
        }
    }

    // TODO: Can this be combined into readyNextRound?
    void removePrepareResponses() {
        assert(!received_messages_.empty());
        assert(receivedAll());

        for (int i = 0; i < sent_messages_.size(); ++i) {
            if (sent_messages_[i].second.empty()) {
                assert(partition_status_[sent_messages_[i].first].isDone());

                // remove the response from the responses
                MessageList::iterator it = findPartition(&received_messages_, sent_messages_[i].first);
                assert(it != received_messages_.end());
                received_messages_.erase(it);
            }
        }
    }

    // Remove the last response received by index. Used when aborting a dependency.
    void removeResponse(int index) {
        for (int i = 0; i < received_messages_.size(); ++i) {
            if (received_messages_[i].first == index) {
                received_messages_.erase(received_messages_.begin() + i);
                received_count_ -= 1;
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
    // TODO: Remove this? Move it elsewhere?
    void readyNextRound() {
        assert(received_count_ == sent_messages_.size());
        sent_messages_.clear();
        received_count_ = 0;
    }

    bool receivedAll() const __attribute__((warn_unused_result)) {
        return received_count_ == sent_messages_.size();
    }

    int numParticipants() const __attribute__((warn_unused_result)) {
        int result = 0;
        for (int i = 0; i < partition_status_.size(); ++i) {
            result += partition_status_[i].isParticipant();
        }
        return result;
    }

    bool isParticipant(int index) const __attribute((warn_unused_result)) {
        assert(validPartitionIndex(index));
        return partition_status_[index].isParticipant();
    }

    bool isDone(int index) const __attribute__((warn_unused_result)) {
        assert(validPartitionIndex(index));
        return partition_status_[index].isDone();
    }

    bool isActive(int index) const __attribute__((warn_unused_result)) {
        assert(validPartitionIndex(index));
        return partition_status_[index].active();
    }

    bool isUnknown(int index) const __attribute__((warn_unused_result)) {
        assert(validPartitionIndex(index));
        return partition_status_[index].unknown();
    }

    bool isPrepared(int index) const __attribute__((warn_unused_result)) {
        assert(validPartitionIndex(index));
        return partition_status_[index].prepared();
    }

    bool isFinished(int index) const __attribute__((warn_unused_result)) {
        assert(validPartitionIndex(index));
        return partition_status_[index].finished();
    }

    std::vector<int> getParticipants() const __attribute__((warn_unused_result)) {
        std::vector<int> result;
        for (int i = 0; i < partition_status_.size(); ++i) {
            if (partition_status_[i].isParticipant()) result.push_back(i);
        }
        return result;
    }

    void state(void* next_state) { state_ = next_state; }
    void* state() { return state_; }


    // PAVLO
    bool has_payload() const { return !payload_.empty(); }
    void set_payload(const std::string& payload) {
        assert(!payload.empty());
		payload_ = payload;
    }
    const std::string& payload() const { return payload_; }

private:
    bool validPartitionIndex(int index) const {
        return 0 <= index && index < partition_status_.size();
    }

    // Yucky template to work with both const_iterator and non-const iterator. Gross.
    // TODO: get rid of this? we don't really use the const version.
    template <typename ItType> ItType findPartition(ItType it, ItType end, int value) const {
        while (it != end) {
            if (it->first == value) break;
            ++it;
        }
        return it;
    }
    /** Returns an iterator to the message for partition_index in messages, or messages->end() if not found. */
    MessageList::iterator findPartition(MessageList* messages, int partition_index) const {
        return findPartition(messages->begin(), messages->end(), partition_index);
    }
    MessageList::const_iterator findPartition(const MessageList& messages, int partition_index) const {
        return findPartition(messages.begin(), messages.end(), partition_index);
    }

    bool containsKey(const MessageList& messages, int index) const
            __attribute__((warn_unused_result)) {
        return findPartition(messages, index) != messages.end();
    }

    // The current status of the transaction
    Status status_;

    // Stores the status of each individual partition.
    std::vector<PartitionState> partition_status_;

    MessageList received_messages_;
    int received_count_;

    // Next round of messages to be sent out
    MessageList sent_messages_;

    bool multiple_partitions_;

    // Opaque pointer for use by the Coordinator.
    void* state_;

    // PAVLO: Let things attach payload data that we can send around during the finish process
    std::string payload_;
};

}  // namespace dtxn
#endif
