// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "protodtxn/protodtxnengine.h"

#include "base/assert.h"
#include "base/debuglog.h"
#include "io/eventloop.h"
#include "protodtxn/dtxn.pb.h"
#include "protorpc/protorpccontroller.h"

using google::protobuf::NewCallback;

namespace protodtxn {

dtxn::ExecutionEngine::Status ProtoDtxnEngine::tryExecute(
        const std::string& work_unit, std::string* output, void** undo,
        dtxn::Transaction* transaction, const std::string& payload) {
    CHECK(!request_active_);
    request_active_ = true;

    // Get the transaction id, or allocate a new one
    int64_t transaction_id = 0;
    if (undo != NULL && *undo != NULL) {
        transaction_id = static_cast<int64_t>(reinterpret_cast<intptr_t>(*undo));
    } else {
        // Allocate a new transaction id
        transaction_id = next_id_;
        next_id_ += 1;
        if (undo != NULL) {
            *undo = reinterpret_cast<void*>(transaction_id);
        }
    }
    assert(transaction_id > 0);
    assert(undo == NULL || *undo == (void*) transaction_id);

    // TODO: Cache these objects?
    protorpc::ProtoRpcController controller;
    Fragment request;
    request.set_transaction_id(transaction_id);
    request.set_work(work_unit);
    request.set_undoable(undo != NULL);

    // PAVLO
    if (&payload != NULL) {
        LOG_DEBUG("Setting Fragment (request?) payload directly for txn %ld [%s]", transaction_id, payload.c_str());
        request.set_payload(payload);
    } else {
        LOG_DEBUG("Given a NULL payload for txn %ld! I can't live like this", transaction_id);
    }

    FragmentResponse response;
    proto_engine_->Execute(&controller, &request, &response, 
            NewCallback(this, &ProtoDtxnEngine::responseArrived));
    event_loop_->run();
    CHECK(!request_active_);

    output->assign(response.output());
    return static_cast<dtxn::ExecutionEngine::Status>(response.status());
}

void ProtoDtxnEngine::applyUndo(void* undo_buffer, const std::string& payload) {
    LOG_DEBUG("Applying Undo Buffer [payload=%s]", payload.c_str());
    finish(undo_buffer, false, payload);
}

void ProtoDtxnEngine::freeUndo(void* undo_buffer, const std::string& payload) {
    LOG_DEBUG("Freeing Undo Buffer [payload=%s]", payload.c_str());
    finish(undo_buffer, true, payload);
}

void ProtoDtxnEngine::finish(void* undo_buffer, bool commit, const std::string& payload) {
    CHECK(!request_active_);
    request_active_ = true;

    intptr_t transaction_id = reinterpret_cast<intptr_t>(undo_buffer);
    assert(transaction_id > 0);
    LOG_DEBUG("Finish [txn_id=%ld, payload=%s]", static_cast<int64_t>(transaction_id), payload.c_str());

    // TODO: Cache these objects?
    protorpc::ProtoRpcController controller;
    FinishRequest request;
    request.set_transaction_id(static_cast<int64_t>(transaction_id));
    request.set_commit(commit);
    request.set_payload(payload);
    FinishResponse response;
    proto_engine_->Finish(&controller, &request, &response, 
            NewCallback(this, &ProtoDtxnEngine::responseArrived));
    event_loop_->run();
    CHECK(!request_active_);
}

void ProtoDtxnEngine::responseArrived() {
    CHECK(request_active_);
    request_active_ = false;
    event_loop_->exit();
}

}  // namespace protodtxn
