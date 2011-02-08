// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_DTXNMANAGER_H__
#define DTXN_DTXNMANAGER_H__

// Forward declaration of std::tr1::function
namespace std { namespace tr1 {
template <typename T> class function;
}}

namespace dtxn {

class DistributedTransaction;

// Manages distributed transactions. Specific implementations provide
// different protocols, concurrency control mechanisms and fault-tolerance.
// TODO: Creating DistributedTransactions requires knowing the number of partitions. Fix this?
// this may not be as "easy" as adding a newTransaction() method here: there is a complex
// relationship between the partitions that exist and the DtxnManager that needs to be considered.
class DtxnManager {
public:
    virtual ~DtxnManager() {}

    typedef std::tr1::function<void()> Callback;

    // Executes a round of work represented by transaction. callback will be called with results.
    virtual void execute(DistributedTransaction* transaction,
            const Callback& callback) = 0;

    // Must be called to commit/abort a distributed transaction. If transaction was a single
    // partition transaction (one participant, last_fragment == true), then this is not required.
    virtual void finish(DistributedTransaction* transaction, bool commit, const std::string& payload,
            const Callback& callback) = 0;
};

}  // namespace dtxn
#endif
