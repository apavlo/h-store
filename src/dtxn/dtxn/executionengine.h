// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_EXECUTIONENGINE_H__
#define DTXN_EXECUTIONENGINE_H__

#include <string>

namespace dtxn {

class Transaction;

class ExecutionEngine {
public:
    virtual ~ExecutionEngine() {}

    // TODO: Only use this for transaction status? Need to add timeout and user abort?
    enum Status {
        /** Execution completed successfully. */
        OK = 0,
        /** The user code is requesting an abort. */
        ABORT_USER,
        /** Deadlock detected an abort. */
        ABORT_DEADLOCK,
        /** Blocked trying to acquire locks. */
        BLOCKED,
        /** PAVLO: Aborting because of a misprediction. */
        ABORT_MISPREDICT = 5,
        // TODO: Add a timeout abort code
        /** An invalid state. Should only be used internally. */
        INVALID = -1,
    };

    /** Execute a work unit, possibly with an undo buffer and possibly using locks. Returns OK if
    the work unit committed. If this returns ABORT_USER, the work unit aborted and the caller should
    called applyUndo to undo any effects of the transaction. In both the OK and ABORT_USER cases, 
    output will contain valid data. This can only return BLOCKED if transaction is not null. This
    indicates that the work unit was unable to obtain some lock it required. It must be re-executed
    at some point in the future. When it is re-executed, it must either avoid duplicate actions by
    storing data in output or undo, or must be idempotent.
    
    If undo is null, no undo data is required.
    If transaction is not null, undo must not be null. This indicates locks are to be acquired via
    transaction. */
    // TODO: Originally, there was one undo object per work unit. Then I changed it to be one undo
    // object per transaction. However, the optimistic concurrency control scheduler may want to
    // undo just ONE work unit, to avoid redoing extra work. Which is better?
    virtual Status tryExecute(const std::string& work_unit, std::string* output, void** undo,
            Transaction* transaction, const std::string& payload) = 0;

    /** Returns true if this engine supports locks. If true, then we can call tryExecute with a
    non-null TransactionState. */
    virtual bool supports_locks() const = 0;

    // Execute with an undo buffer, and track read/write sets.
    //virtual void execute(const std::string& transaction, std::string* undo, ReadWriteSet* read_write) = 0;

    // Undo a transaction using the given undo buffer. The undo buffer should be freed after this
    // returns. The caller will ensure that undo buffers are applied in the right order.
    virtual void applyUndo(void* undo_buffer, const std::string& payload) = 0;

    // Frees an undo buffer without applying it.
    virtual void freeUndo(void* undo_buffer, const std::string& payload) = 0;

    // Runs the engine by listening on port. This will return only when SIGINT is delivered.
    static void run(ExecutionEngine* engine, int port);
};

}  // namespace dtxn

#endif
