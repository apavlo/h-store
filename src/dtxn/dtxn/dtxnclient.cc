// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/dtxnclient.h"

#include "dtxn/executionengine.h"
#include "dtxn/messages.h"
#include "io/libeventloop.h"
#include "libevent/include/event2/event.h"


using std::string;

namespace dtxn {

// Converts the transaction callback interface into a blocking interface
class TransactionWaiter : public TransactionCallback {
public:
    TransactionWaiter() : done_(false) {}

    void wait(io::LibEventLoop* event_loop) {
        while (!done_) {
            // TODO: make this less of a hack
            event_base_loop(event_loop->base(), EVLOOP_ONCE);
        }
    }

    virtual void transactionComplete(const FragmentResponse& response) {
        assert(!done_);
        result_ = response;
        done_ = true;
    }

    const FragmentResponse& result() const {
        assert(done_);
        return result_;
    }

private:
    FragmentResponse result_;
    bool done_;
};

bool DtxnClient::blockingExecute(
        io::EventLoop* event_loop, const std::string& request, std::string* output) {
    TransactionWaiter waiter;
    execute(request, &waiter);
    waiter.wait((io::LibEventLoop*) event_loop);

    // TODO: PERFORMANCE HACK: Does this make any difference?
    output->swap(const_cast<FragmentResponse&>(waiter.result()).result);
    return waiter.result().status == ExecutionEngine::OK;
}

}  // namespace dtxn
