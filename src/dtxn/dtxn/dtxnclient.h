// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXNCLIENT_H
#define DTXNCLIENT_H

#include <string>

class NetworkAddress;

namespace io {
class EventLoop;
}

namespace dtxn {

class FragmentResponse;

class TransactionCallback {
public:
    virtual ~TransactionCallback() {}

    // Called with the response when the transaction has finished executing.
    virtual void transactionComplete(const FragmentResponse& response) = 0;
};

/** Asynchronous client interface to the transaction system. */
class DtxnClient {
public:
    virtual ~DtxnClient() {}

    // Request a transaction to be executed asynchronously via the event loop. callback is not
    // owned by this object.
    // TODO: Avoid a copy by taking a FragmentResponse*?
    virtual void execute(const std::string& request, TransactionCallback* callback) = 0;

    /** Wrapper around the asynchronous execute to block. This is a bit of a hack but works.
    @return true if the transaction committed. */
    bool blockingExecute(io::EventLoop* event_loop, const std::string& request, std::string* output);

    // Returns a DtxnClient allocated with new. It uses event_loop to drive I/O callbacks.
    // It will connect to distributor to execute transactions. This factory method will return a
    // specific client, depending on the version of the Dtxn library linked in.
    static DtxnClient* create(io::EventLoop* event_loop, const NetworkAddress& distributor);
};

}  // namespace dtxn

#endif
