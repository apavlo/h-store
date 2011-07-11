// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "protodtxn/protodtxncoordinator.h"

#include <tr1/functional>
#include <tr1/inttypes.h>

#include "base/assert.h"
#include "base/debuglog.h"
#include "dtxn/dtxnmanager.h"
#include "dtxn/distributedtransaction.h"

using std::string;

using dtxn::DistributedTransaction;
using google::protobuf::Closure;
using google::protobuf::RpcController;

namespace protodtxn {

class ProtoDtxnCoordinatorRequest {
public:
    ProtoDtxnCoordinatorRequest(ProtoDtxnCoordinator* coordinator,
            int64_t id,
            int num_partitions) :
            coordinator_(coordinator),
            controller_(NULL),
            response_(NULL),
            done_(NULL),
            id_(id),
            transaction_(new DistributedTransaction(num_partitions)),
            callback_(std::tr1::bind(&ProtoDtxnCoordinatorRequest::executeDone, this)),
            finish_callback_(std::tr1::bind(&ProtoDtxnCoordinatorRequest::finishDone, this)) {
        assert(coordinator_ != NULL);
    }

    ~ProtoDtxnCoordinatorRequest() {
        delete transaction_;
    }

    void setResponse(RpcController* controller, CoordinatorResponse* response, Closure* done) {
        LOG_DEBUG("Set Response Txn #%" PRIu64, id_);
        CHECK_M(controller_ == NULL, "RpcController already invoked for Txn #%" PRIu64, id_);
        CHECK_M(response_ == NULL, "CoordinatorResponse is already set for Txn #%" PRIu64, id_);
        CHECK_M(done_ == NULL, "Already executed done callback for Txn #%" PRIu64, id_);
        //~ assert(controller != NULL);
        CHECK_M(response != NULL, "Incoming RpcController is NULL for Txn #%" PRIu64, id_);
        CHECK_M(done != NULL, "Incoming done callback is NULL for Txn #%" PRIu64, id_);
        controller_ = controller;
        response_ = response;
        done_ = done;
        LOG_DEBUG("Txn #%lld now has a response! What do you think about that?", id_);
    }

    DistributedTransaction* transaction() { return transaction_; }

    const std::tr1::function<void()>& callback() { return callback_; }
    const std::tr1::function<void()>& finish_callback() { return finish_callback_; }

    void setFinish(Closure* done) {
        CHECK(done_ == NULL);
        CHECK(done != NULL);
        done_ = done;
    }

private:
    void executeDone() {
        LOG_DEBUG("executeDone %ld", id_);
        // Pass back the answers to the client
        // TODO: Need to handle more complicated things?
        for (int i = 0; i < transaction_->received().size(); ++i) {
            CoordinatorResponse::PartitionResponse* partition = response_->add_response();
            partition->set_partition_id(transaction_->received()[i].first);
            partition->set_output(transaction_->received()[i].second);
        }
        response_->set_transaction_id(id_);
        response_->set_status((FragmentResponse::Status) transaction_->status());

        transaction_->readyNextRound();

        // NULL all the fields so we don't accidentally touch them after the callback
        Closure* done = done_;
        controller_ = NULL;
        response_ = NULL;
        done_ = NULL;
        done->Run();
    }

    void finishDone() {
        Closure* done = done_;
        done_ = NULL;
        ProtoDtxnCoordinatorRequest* state = coordinator_->internalDeleteTransaction(id_);
        ASSERT(state == this);
        done->Run();
    }

    ProtoDtxnCoordinator* coordinator_;

    RpcController* controller_;
    CoordinatorResponse* response_;
    Closure* done_;

    int64_t id_;
    DistributedTransaction* transaction_;
    std::tr1::function<void()> callback_;
    std::tr1::function<void()> finish_callback_;
};

ProtoDtxnCoordinator::ProtoDtxnCoordinator(dtxn::DtxnManager* dtxn_manager, int num_partitions) :
        dtxn_manager_(dtxn_manager),
        num_partitions_(num_partitions) {
    CHECK(dtxn_manager_ != NULL);
    CHECK(num_partitions_ > 0);
    LOG_DEBUG("Evan's magic has started");
}

ProtoDtxnCoordinator::~ProtoDtxnCoordinator() {
}

void ProtoDtxnCoordinator::Execute(RpcController* controller,
        const CoordinatorFragment* request,
        CoordinatorResponse* response,
        Closure* done) {
    LOG_DEBUG("Execute %ld", request->transaction_id());
    LOG_DEBUG("Execute %ld fragments: %d last fragment? %d done partitions: %d\n",
            request->transaction_id(), request->fragment_size(),
            request->last_fragment(), request->done_partition_size());
    CHECK(request->fragment_size() > 0);
    assert(!response->IsInitialized());

    // TODO: Check that transaction ids are increasing or otherwise unique?
    ProtoDtxnCoordinatorRequest* state = NULL;
    TransactionMap::iterator it = transactions_.find(request->transaction_id());
    if (it == transactions_.end()) {
        state = new ProtoDtxnCoordinatorRequest(this, request->transaction_id(), num_partitions_);

        std::pair<TransactionMap::iterator, bool> result = transactions_.insert(
                std::make_pair(request->transaction_id(), state));
        CHECK(result.second);
    } else {
        state = it->second;
        bool multiple_partitions = state->transaction()->multiple_partitions();
        if (multiple_partitions == false) {
            std::string payload = (state->transaction()->has_payload() ? state->transaction()->payload() : request->payload());
            LOG_ERROR("Txn #%s wants multiple partitions when it was suppose to be single-partitioned", payload.c_str());
        }
        CHECK(multiple_partitions);
    }
    state->setResponse(controller, response, done);

    // PAVLO:
    if (state->transaction()->has_payload() == false && request->has_payload() == true) {
        LOG_DEBUG("Setting DistributedTransaction payload [%s]", request->payload().c_str());
        state->transaction()->set_payload(request->payload());
    }

    for (int i = 0; i < request->fragment_size(); ++i) {
        state->transaction()->send(
                request->fragment(i).partition_id(), request->fragment(i).work());
    }

    // Check which partitions are now done
    if (request->last_fragment()) {
        CHECK(request->done_partition_size() == 0);
        state->transaction()->setAllDone();
    } else if (request->done_partition_size() > 0) {
        for (int i = 0; i < request->done_partition_size(); ++i) {
            state->transaction()->setDone(request->done_partition(i));
        }
    }

    dtxn_manager_->execute(state->transaction(), state->callback());
    state->transaction()->sentMessages();
}

void ProtoDtxnCoordinator::Finish(RpcController* controller,
        const FinishRequest* request,
        FinishResponse* response,
        Closure* done) {
    LOG_DEBUG("Finish %ld [payload=%s]", request->transaction_id(), request->payload().c_str());
    LOG_DEBUG("Finish %ld commit = %d\n",
            request->transaction_id(), request->commit());
    
    // Finish this transaction
    TransactionMap::iterator it = transactions_.find(request->transaction_id());
    CHECK(it != transactions_.end());
    ProtoDtxnCoordinatorRequest* state = it->second;

    if (state->transaction()->multiple_partitions() &&
            state->transaction()->status() == DistributedTransaction::OK) {
        state->setFinish(done);
        LOG_DEBUG("Telling our DtxnManager that we want to finish");
        dtxn_manager_->finish(state->transaction(), request->commit(), request->payload(), state->finish_callback());
    } else {
        CHECK(request->commit() == (state->transaction()->status() == DistributedTransaction::OK));
        // This is a single partition transaction, or it aborted: just delete the state
        internalDeleteTransaction(request->transaction_id());
        done->Run();
    }
}

ProtoDtxnCoordinatorRequest* ProtoDtxnCoordinator::internalDeleteTransaction(int64_t transaction_id) {
    TransactionMap::iterator it = transactions_.find(transaction_id);
    CHECK(it != transactions_.end());

    ProtoDtxnCoordinatorRequest* state = it->second;
    transactions_.erase(it);
    delete state;
    return state;
}

}  // namespace protodtxn
