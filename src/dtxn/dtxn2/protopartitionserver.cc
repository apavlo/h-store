// Copyright 2011 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn2/protopartitionserver.h"

#include "base/stlutil.h"
#include "dtxn/messages.h"
#include "dtxn2/partitionserver.h"
#include "messageconnection.h"
#include "net/messageserver.h"

using google::protobuf::RpcController;
using google::protobuf::Closure;
using protodtxn::DtxnPartitionFragment;
using protodtxn::FragmentResponse;
using protodtxn::FinishRequest;
using protodtxn::FinishResponse;
using std::string;

namespace dtxn2 {

// A hack to permit the MessageServer to respond via ProtoRPC.
// TODO: This is super, super ugly. Eventually, ProtoPartitionServer should replace PartitionServer
// and this won't be needed
class ProtoPartitionServer::ResponseHackConnection : public MessageConnection {
public:
    ResponseHackConnection(ProtoPartitionServer* server) :
        server_(server), handle_(NULL), dtxn_id_(0) {
        clear();
    }

    void clear() {
        response_ = NULL;
        done_ = NULL;
        transaction_id_ = 0;
        finishing_ = false;
    }

    virtual ~ResponseHackConnection() {
        assert(response_ == NULL);
        assert(done_ == NULL);
    }

    void setResponse(bool finishing, FragmentResponse* response, Closure* done) {
        assert(finishing_ == false);
        assert(response_ == NULL);
        assert(done_ == NULL);
        finishing_ = finishing;
        response_ = response;
        done_ = done;
        assert(response_ != NULL);
        assert(done_ != NULL);
    }

    // Implement this to write a message over the connection. It must write bytes from out_buffer_.
    // Returns true if the send is successful. The send can only fail because the connection died
    // or is closed.
    virtual bool flush() {
        // This is disgusting: we need to parse the type of of the output buffer, stuff it into
        // the correct type, then send it
        //out_buffer_;
        const void* data = NULL;
        int length = 0;
        string out;
        out_buffer_.readBuffer(&data, &length);
        assert(length >= 8);
        data = (char*) data + 8;
        length -= 8;
        do {
            assert(length > 0);
            out.append((const char*) data, length);
            out_buffer_.readBuffer(&data, &length);
        } while (length > 0);
        assert(length == 0);

        dtxn::FragmentResponse r;
        r.parseFromString(out);
        response_->set_status((FragmentResponse::Status) r.status);
        response_->set_output(r.result);

        Closure* callback = done_;
        if (finishing_) {
            server_->recycleConnection(this);
        }

        callback->Run();
        return true;
    }

    void handle(net::ConnectionHandle* handle) {
        assert(handle_ == NULL);
        handle_ = handle;
        assert(handle_ != NULL);
    }

    net::ConnectionHandle* handle() {
        return handle_;
    }

    void startTransaction(int64_t transaction_id) {
        assert(transaction_id_ == 0);
        transaction_id_ = transaction_id;
        dtxn_id_ += 1;
    }

    int64_t transaction_id() const { return transaction_id_; }
    int dtxn_id() const { return dtxn_id_; }

private:
    ProtoPartitionServer* const server_;
    net::ConnectionHandle* handle_;
    // Only used for error checking
    int64_t transaction_id_;
    int dtxn_id_;

    // True if the current request is a "finishing" request
    bool finishing_;

    FragmentResponse* response_;
    Closure* done_;
};

ProtoPartitionServer::ProtoPartitionServer(PartitionServer* server, net::MessageServer* msg_server) :
        partition_server_(server),
        msg_server_(msg_server) {
    assert(partition_server_ != NULL);
    assert(msg_server_ != NULL);
}

ProtoPartitionServer::~ProtoPartitionServer() {
    // drop all connections from the msg_server_
    for (int i = 0; i < unused_connections_.size(); ++i) {
        msg_server_->closeConnection(unused_connections_[i]->handle());
    }
    // TODO: How to clean up active transactions?
    assert(transaction_connections_.empty());
}

void ProtoPartitionServer::Execute(RpcController* controller,
        const DtxnPartitionFragment* request,
        FragmentResponse* response,
        Closure* done) {
    ResponseHackConnection* fake_connection = getTransactionConnection(request->transaction_id());
    fake_connection->setResponse(
            request->commit() == DtxnPartitionFragment::LOCAL_COMMIT, response, done);

    dtxn::Fragment fragment;
    fragment.id = fake_connection->dtxn_id();
    fragment.transaction = request->work();
    fragment.payload = request->payload();

    switch (request->commit()) {
    case DtxnPartitionFragment::OPEN:
        fragment.multiple_partitions = true;
        fragment.last_fragment = false;
        break;

    case DtxnPartitionFragment::LOCAL_COMMIT:
        fragment.multiple_partitions = false;
        fragment.last_fragment = true;
        break;

    case DtxnPartitionFragment::PREPARE:
        fragment.multiple_partitions = true;
        fragment.last_fragment = true;
        break;

    default:
        assert(false);
    }

    partition_server_->fragmentReceived(fake_connection->handle(), fragment);
}

void ProtoPartitionServer::Finish(RpcController* controller,
        const FinishRequest* request,
        FinishResponse* response,
        Closure* done) {
    ResponseHackConnection* fake_connection = getTransactionConnection(request->transaction_id());

    // Copy data to the DTXN message
    dtxn::CommitDecision decision;
    decision.id = fake_connection->dtxn_id();
    decision.commit = request->commit();
    decision.payload = request->payload();
    partition_server_->decisionReceived(fake_connection->handle(), decision);

    // PartitionServer does not respond to commits; we do
    recycleConnection(fake_connection);
    done->Run();
}

ProtoPartitionServer::ResponseHackConnection* ProtoPartitionServer::getTransactionConnection(
        int64_t transaction_id) {
    ResponseHackConnection* connection = NULL;
    std::pair<TransactionMap::iterator, bool> result = transaction_connections_.insert(
            std::make_pair(transaction_id, connection));
    if (result.second) {
        // inserted, which means we must allocate a new connection
        if (unused_connections_.empty()) {
            connection = new ResponseHackConnection(this);
            net::ConnectionHandle* handle = msg_server_->addConnection(connection);
            connection->handle(handle);
        } else {
            connection = unused_connections_.back();
            unused_connections_.pop_back();
        }
        connection->startTransaction(transaction_id);
        assert(result.first->second == NULL);
        result.first->second = connection;
    } else {
        connection = result.first->second;
        assert(connection->transaction_id() == transaction_id);
    }
    return connection;
}

void ProtoPartitionServer::recycleConnection(ResponseHackConnection* connection) {
    size_t count = transaction_connections_.erase(connection->transaction_id());
    ASSERT(count == 1);
    connection->clear();
    unused_connections_.push_back(connection);
}

}  // namespace dtxn2
