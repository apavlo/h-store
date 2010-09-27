// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/ordered/orderedclient.h"

#include <netinet/in.h>

#include "io/libeventloop.h"
#include "dtxn/messages.h"
#include "networkaddress.h"

using std::string;
using std::vector;

namespace dtxn {

OrderedClient::OrderedClient(MessageConnection* sequencer) :
        sequencer_(sequencer), response_(new FragmentResponse), next_transaction_id_(0) {
    sequencer_->setTarget(this);
}

OrderedClient::~OrderedClient() {
    delete sequencer_;
    delete response_;
}

void OrderedClient::execute(
        const string& request, TransactionCallback* callback) {
    assert(callback != NULL);
    assert(!request.empty());

    dtxn::Fragment transaction;
    transaction.id = next_transaction_id_ + assert_range_cast<int32_t>(queue_.size());
    transaction.transaction = request;
    transaction.last_fragment = true;
    sequencer_->send(transaction);
    queue_.push_back(callback);
}

void OrderedClient::messageReceived(
        MessageConnection* connection, int32_t type_code, const string& buffer) {
    assert(connection == sequencer_);
    assert(type_code == response_->typeCode());
    response_->parseFromString(buffer);

    // responses currently come back in FIFO order
    assert(response_->id == next_transaction_id_);
    next_transaction_id_ += 1;
    TransactionCallback* callback = queue_.dequeue();
    callback->transactionComplete(*response_);
}

void OrderedClient::connectionArrived(MessageConnection* connection) {
    // This must never be called
    assert(false);
}

void OrderedClient::connectionClosed(MessageConnection* connection) {
    assert(connection == sequencer_);
    // TODO: Handle this.
    assert(false);
}

// Create a new OrderedClient communicating with distributor.
DtxnClient* DtxnClient::create(io::EventLoop* event_loop, const NetworkAddress& distributor) {
    int sock = connectTCP(distributor.sockaddr());
    assert(sock >= 0);
    return new OrderedClient(new TCPMessageConnection(
            ((io::LibEventLoop*) event_loop)->base(), new TCPConnection(sock)));
}

}  // namespace dtxn
