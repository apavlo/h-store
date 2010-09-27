// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "protorpc/protorpccontroller.h"

#include "base/assert.h"
#include "protorpc/Protocol.pb.h"

using google::protobuf::Closure;
using google::protobuf::MessageLite;
using std::string;

namespace protorpc {

void ProtoRpcController::Reset() {
    CHECK(response_ == NULL);
    CHECK(callback_ == NULL);
    response_ = NULL;
    callback_ = NULL;
    failed_ = false;
    error_text_.clear();
}

void ProtoRpcController::StartCancel() {
    CHECK(false);
}

void ProtoRpcController::SetFailed(const string& error_text) {
    CHECK(false);
}

 bool ProtoRpcController::IsCanceled() const {
    CHECK(false);
    return false;
}

void ProtoRpcController::NotifyOnCancel(Closure* callback) {
    CHECK(false);
}

void ProtoRpcController::setCall(MessageLite* response, Closure* callback) {
    CHECK(response != NULL);
    CHECK(callback != NULL);
    CHECK(response_ == NULL);
    CHECK(callback_ == NULL);

    response_ = response;
    callback_ = callback;
}

void ProtoRpcController::finished(const RpcResponse& response) {
    CHECK(callback_ != NULL);
    CHECK(response.has_error_reason() || response.has_response());
    if (response.has_error_reason()) {
        CHECK(!response.has_response());
        failed_ = true;
        error_text_ = response.error_reason();
    } else {
        CHECK(!response.has_error_reason());
        CHECK(response.has_response());
        assert(!failed_);

        bool success = response_->ParseFromString(response.response());
        CHECK(success);
    }

    Closure* callback = callback_;
    callback_ = NULL;
    response_ = NULL;
    callback->Run();
}

}
