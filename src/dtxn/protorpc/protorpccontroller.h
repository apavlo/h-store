// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef PROTORPC_PROTORPCCONTROLLER_H__
#define PROTORPC_PROTORPCCONTROLLER_H__

#include <string>

#include "google/protobuf/service.h"

namespace google { namespace protobuf {
class MessageLite;
}}

namespace protorpc {

class RpcResponse;

class ProtoRpcController : public google::protobuf::RpcController {
public:
    ProtoRpcController() : response_(NULL), callback_(NULL), failed_(false) {}

    virtual void Reset();

    virtual bool Failed() const {
        return failed_;
    }

    virtual std::string ErrorText() const {
        return error_text_;
    }

    virtual void StartCancel();
    virtual void SetFailed(const std::string& error_text);
    virtual bool IsCanceled() const;
    virtual void NotifyOnCancel(google::protobuf::Closure* callback);

    void setCall(google::protobuf::MessageLite* response, google::protobuf::Closure* callback);

    void finished(const RpcResponse& response);

private:
    google::protobuf::MessageLite* response_;
    google::protobuf::Closure* callback_;
    bool failed_;
    std::string error_text_;
};

}
#endif
