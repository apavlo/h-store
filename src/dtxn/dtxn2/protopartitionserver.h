// Copyright 2011 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN2_PROTOPARTITIONSERVER_H__
#define DTXN2_PROTOPARTITIONSERVER_H__

#include "base/unordered_map.h"
#include "protodtxn/dtxn.pb.h"

namespace net {
class MessageServer;
}

namespace dtxn2 {

class PartitionServer;

/** Provides a ProtoRPC interface to the partition.
TODO: Currently this is a wrapper around PartitionServer, but should eventually replace it.

PartitionServer provides the glue logic between the Scheduler and ReplicatedLog services.
That logic should eventually migrate here.
*/
class ProtoPartitionServer : public protodtxn::Partition {
public:
    // Does not own any of the pointers.
    ProtoPartitionServer(PartitionServer* server, net::MessageServer* msg_server);
    virtual ~ProtoPartitionServer();

    virtual void Execute(google::protobuf::RpcController* controller,
        const protodtxn::DtxnPartitionFragment* request,
        protodtxn::FragmentResponse* response,
        google::protobuf::Closure* done);
    virtual void Finish(google::protobuf::RpcController* controller,
        const protodtxn::FinishRequest* request,
        protodtxn::FinishResponse* response,
        google::protobuf::Closure* done);

private:
    PartitionServer* partition_server_;
    net::MessageServer* msg_server_;

    class ResponseHackConnection;
    ResponseHackConnection* getTransactionConnection(int64_t transaction_id);
    void recycleConnection(ResponseHackConnection* connection);

    std::vector<ResponseHackConnection*> unused_connections_;

    // Maps a transaction id to a connection
    typedef base::unordered_map<int64_t, ResponseHackConnection*> TransactionMap;
    TransactionMap transaction_connections_;
};

}  // namespace dtxn2
#endif
