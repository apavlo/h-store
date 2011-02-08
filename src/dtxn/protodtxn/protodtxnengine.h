// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef PROTODTXN_PROTODTXNENGINE_H__
#define PROTODTXN_PROTODTXNENGINE_H__

#include <vector>

#include "base/circularbuffer.h"
#include "dtxn/executionengine.h"

namespace io {
class EventLoop;
}

namespace protodtxn {

class ExecutionEngine;

class ProtoDtxnEngine : public dtxn::ExecutionEngine {
public:
    // The event_loop will be used in a "blocking" way to make RPCs. This means it basically needs
    // to be its own private event loop, to avoid re-entrant problems. Does not own event_loop or
    // proto_engine.
    // TODO: Yuck! Make ExecutionEngine a non-blocking interface, so this actually works?
    ProtoDtxnEngine(io::EventLoop* event_loop, protodtxn::ExecutionEngine* proto_engine) :
            event_loop_(event_loop), proto_engine_(proto_engine), next_id_(1),
            request_active_(false) {}
    virtual ~ProtoDtxnEngine() {}

    virtual dtxn::ExecutionEngine::Status tryExecute(const std::string& work_unit, std::string* output, void** undo,
            dtxn::Transaction* transaction, const std::string& payload);
    virtual bool supports_locks() const { return false; }
    virtual void applyUndo(void* undo_buffer, const std::string& payload);
    virtual void freeUndo(void* undo_buffer, const std::string& payload);

private:
    void finish(void* undo_buffer, bool commit, const std::string& payload);
    void responseArrived();

    io::EventLoop* event_loop_;
    protodtxn::ExecutionEngine* proto_engine_;

    int next_id_;
    bool request_active_;
};

}
#endif
