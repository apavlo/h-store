/* AUTOMATICALLY GENERATED: DO NOT EDIT */
#ifndef __DTXN_MESSAGES_H
#define __DTXN_MESSAGES_H

#include <cassert>
#include <string>
#include <vector>

#include "base/assert.h"
#include "io/message.h"
#include "serialization.h"

namespace io {
class FIFOBuffer;
}

namespace dtxn {

// Contains a transaction fragment to be processed by the execution engine.
class Fragment : public io::Message {
public:
    Fragment() :
        client_id(0),
        id(0),
        multiple_partitions(false),
        last_fragment(true) {}

    // Unique identifier for the client sending the transaction.
    int32_t client_id;

    // Unique identifier for this transaction. Used to identify messages that belong to a single transaction.
    int64_t id;

    // True if this transaction spans multiple partitions.
    bool multiple_partitions;

    // True if this is the last fragment of this transaction for this partition.
    bool last_fragment;

    // The actual transaction to be processed.
    std::string transaction;

    // Fragment messsage payload
    std::string payload;

    bool operator==(const Fragment& other) const {
        if (client_id != other.client_id) return false;
        if (id != other.id) return false;
        if (multiple_partitions != other.multiple_partitions) return false;
        if (last_fragment != other.last_fragment) return false;
        if (transaction != other.transaction) return false;
//         if (payload != other.payload) return false;
        return true;
    }
    bool operator!=(const Fragment& other) const { return !(*this == other); }

    void appendToString(std::string* _out_) const {
        serialization::serialize(client_id, _out_);
        serialization::serialize(id, _out_);
        serialization::serialize(multiple_partitions, _out_);
        serialization::serialize(last_fragment, _out_);
        serialization::serialize(transaction, _out_);
        serialization::serialize(payload, _out_);
    }

    virtual void serialize(io::FIFOBuffer* _out_) const {
        serialization::serialize(client_id, _out_);
        serialization::serialize(id, _out_);
        serialization::serialize(multiple_partitions, _out_);
        serialization::serialize(last_fragment, _out_);
        serialization::serialize(transaction, _out_);
        serialization::serialize(payload, _out_);
    }

    const char* parseFromString(const char* _start_, const char* _end_) {
        _start_ = serialization::deserialize(&client_id, _start_, _end_);
        _start_ = serialization::deserialize(&id, _start_, _end_);
        _start_ = serialization::deserialize(&multiple_partitions, _start_, _end_);
        _start_ = serialization::deserialize(&last_fragment, _start_, _end_);
        _start_ = serialization::deserialize(&transaction, _start_, _end_);
        _start_ = serialization::deserialize(&payload, _start_, _end_);
        return _start_;
    }

    void parseFromString(const std::string& _str_) {
        const char* end = parseFromString(_str_.data(), _str_.data() + _str_.size());
        ASSERT(end == _str_.data() + _str_.size());
    }

    static int32_t typeCode() { return 1229065059; }
};

// The response from the partition.
class FragmentResponse : public io::Message {
public:
    FragmentResponse() :
        id(0),
        status(-1),
        dependency(-1) {}

    // Identifies the transaction that this decision refers to.
    int64_t id;

    // Contains the ExecutionEngine::Status for this transaction.
    int32_t status;

    // The result of the transaction.
    std::string result;

    // ID of a previous transaction that this depends on.
    int32_t dependency;

    bool operator==(const FragmentResponse& other) const {
        if (id != other.id) return false;
        if (status != other.status) return false;
        if (result != other.result) return false;
        if (dependency != other.dependency) return false;
        return true;
    }
    bool operator!=(const FragmentResponse& other) const { return !(*this == other); }

    void appendToString(std::string* _out_) const {
        serialization::serialize(id, _out_);
        serialization::serialize(status, _out_);
        serialization::serialize(result, _out_);
        serialization::serialize(dependency, _out_);
    }

    virtual void serialize(io::FIFOBuffer* _out_) const {
        serialization::serialize(id, _out_);
        serialization::serialize(status, _out_);
        serialization::serialize(result, _out_);
        serialization::serialize(dependency, _out_);
    }

    const char* parseFromString(const char* _start_, const char* _end_) {
        _start_ = serialization::deserialize(&id, _start_, _end_);
        _start_ = serialization::deserialize(&status, _start_, _end_);
        _start_ = serialization::deserialize(&result, _start_, _end_);
        _start_ = serialization::deserialize(&dependency, _start_, _end_);
        return _start_;
    }

    void parseFromString(const std::string& _str_) {
        const char* end = parseFromString(_str_.data(), _str_.data() + _str_.size());
        ASSERT(end == _str_.data() + _str_.size());
    }

    static int32_t typeCode() { return 704147483; }
};

// Instructs a replica to commit or abort a transaction.
class CommitDecision : public io::Message {
public:
    CommitDecision() :
        client_id(0),
        id(0),
        commit(false) {}

    // Unique identifier for the client sending the transaction.
    int32_t client_id;

    // Identifies the transaction that this decision refers to.
    int64_t id;

    // True if the transaction should commit.
    bool commit;

    // Commit messsage payload
    std::string payload;

    bool operator==(const CommitDecision& other) const {
        if (client_id != other.client_id) return false;
        if (id != other.id) return false;
        if (commit != other.commit) return false;
//         if (payload != other.payload) return false;
        return true;
    }
    bool operator!=(const CommitDecision& other) const { return !(*this == other); }

    void appendToString(std::string* _out_) const {
        serialization::serialize(client_id, _out_);
        serialization::serialize(id, _out_);
        serialization::serialize(commit, _out_);
        serialization::serialize(payload, _out_);
    }

    virtual void serialize(io::FIFOBuffer* _out_) const {
        serialization::serialize(client_id, _out_);
        serialization::serialize(id, _out_);
        serialization::serialize(commit, _out_);
        serialization::serialize(payload, _out_);
    }

    const char* parseFromString(const char* _start_, const char* _end_) {
        _start_ = serialization::deserialize(&client_id, _start_, _end_);
        _start_ = serialization::deserialize(&id, _start_, _end_);
        _start_ = serialization::deserialize(&commit, _start_, _end_);
        _start_ = serialization::deserialize(&payload, _start_, _end_);
        return _start_;
    }

    void parseFromString(const std::string& _str_) {
        const char* end = parseFromString(_str_.data(), _str_.data() + _str_.size());
        ASSERT(end == _str_.data() + _str_.size());
    }

    static int32_t typeCode() { return -1953901859; }
};

// Transaction record stored in a log.
class LogEntry : public io::Message {
public:
    LogEntry() :
        multiple_partitions(false),
        decision_log_entry(-1),
        commit(false) {}

    // All fragments for the transaction collected together.
    std::vector<std::string > fragments;

    // True if this transaction spans multiple partitions.
    bool multiple_partitions;

    // If this is a decision, this is the log entry for the transaction.
    int32_t decision_log_entry;

    // If this is a decision, True if the transaction should be committed.
    bool commit;

    bool operator==(const LogEntry& other) const {
        if (fragments != other.fragments) return false;
        if (multiple_partitions != other.multiple_partitions) return false;
        if (decision_log_entry != other.decision_log_entry) return false;
        if (commit != other.commit) return false;
        return true;
    }
    bool operator!=(const LogEntry& other) const { return !(*this == other); }

    void appendToString(std::string* _out_) const {
        {
            int32_t _size_ = static_cast<int32_t>(fragments.size());
            assert(0 <= _size_ && static_cast<size_t>(_size_) == fragments.size());
            serialization::serialize(_size_, _out_);
            for (int _i0_ = 0; _i0_ < _size_; ++_i0_) {
                serialization::serialize(fragments[_i0_], _out_);
            }
        }
        serialization::serialize(multiple_partitions, _out_);
        serialization::serialize(decision_log_entry, _out_);
        serialization::serialize(commit, _out_);
    }

    virtual void serialize(io::FIFOBuffer* _out_) const {
        {
            int32_t _size_ = static_cast<int32_t>(fragments.size());
            assert(0 <= _size_ && static_cast<size_t>(_size_) == fragments.size());
            serialization::serialize(_size_, _out_);
            for (int _i1_ = 0; _i1_ < _size_; ++_i1_) {
                serialization::serialize(fragments[_i1_], _out_);
            }
        }
        serialization::serialize(multiple_partitions, _out_);
        serialization::serialize(decision_log_entry, _out_);
        serialization::serialize(commit, _out_);
    }

    const char* parseFromString(const char* _start_, const char* _end_) {
        {
            int32_t _size_;
            _start_ = serialization::deserialize(&_size_, _start_, _end_);
            assert(_size_ >= 0);
            fragments.resize(_size_);
            for (int _i2_ = 0; _i2_ < _size_; ++_i2_) {
                _start_ = serialization::deserialize(&fragments[_i2_], _start_, _end_);
            }
        }
        _start_ = serialization::deserialize(&multiple_partitions, _start_, _end_);
        _start_ = serialization::deserialize(&decision_log_entry, _start_, _end_);
        _start_ = serialization::deserialize(&commit, _start_, _end_);
        return _start_;
    }

    void parseFromString(const std::string& _str_) {
        const char* end = parseFromString(_str_.data(), _str_.data() + _str_.size());
        ASSERT(end == _str_.data() + _str_.size());
    }

    static int32_t typeCode() { return -1915125041; }
};

}  // namespace dtxn
#endif  // __DTXN_MESSAGES_H
