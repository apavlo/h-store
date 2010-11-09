// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef PROTODTXN_PROTODTXNCOORDINATOR_H__
#define PROTODTXN_PROTODTXNCOORDINATOR_H__

#include "base/unordered_map.h"
#include "protodtxn/dtxn.pb.h"

namespace dtxn {
class DtxnManager;
}

namespace protodtxn {

class ProtoDtxnCoordinatorRequest;

class ProtoDtxnCoordinator : public Coordinator {
public:
    // Does not own dtxn_manager.
    ProtoDtxnCoordinator(dtxn::DtxnManager* dtxn_manager, int num_partitions);
    virtual ~ProtoDtxnCoordinator();

    virtual void Execute(::google::protobuf::RpcController* controller,
            const CoordinatorFragment* request,
            CoordinatorResponse* response,
            ::google::protobuf::Closure* done);

    virtual void Finish(::google::protobuf::RpcController* controller,
            const FinishRequest* request,
            FinishResponse* response,
            ::google::protobuf::Closure* done);

    // INTERNAL: Remove request from the transaction map and delete state.
    ProtoDtxnCoordinatorRequest* internalDeleteTransaction(int64_t transaction_id);

private:
    dtxn::DtxnManager* dtxn_manager_;
    int num_partitions_;

    typedef base::unordered_map<int64_t, ProtoDtxnCoordinatorRequest*> TransactionMap;
    TransactionMap transactions_;
};

}
#endif
