// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_ORDEREDCLIENT_H
#define DTXN_ORDEREDCLIENT_H

#include <string>

#include "base/circularbuffer.h"
#include "dtxn/dtxnclient.h"
#include "messageconnection.h"

namespace dtxn {

class FragmentResponse;

// Simple ordered dispatch client: sends transactions on a single TCP connection to a sequencer.
class OrderedClient : public DtxnClient, public MessageConnectionCallback {
public:
    // Create an OrderedClient that will use sequencer. sequencer is owned by this object.
    OrderedClient(MessageConnection* sequencer);
    ~OrderedClient();

    virtual void execute(const std::string& request, TransactionCallback* callback);

    // INTERNAL: A message has arrived, which should be a transaction response.
    virtual void messageReceived(MessageConnection* connection, int32_t type_code,
            const std::string& buffer);
    virtual void connectionArrived(MessageConnection* connection);
    virtual void connectionClosed(MessageConnection* connection);

private:
    // INTERNAL: A transaction response has arrived.
    //~ void transactionResponse(MessageConnection* connection, const FragmentResponse& response);
    MessageConnection* sequencer_;

    // Cached object for parsing efficiency
    FragmentResponse* response_;

    // The next transaction id that we will receive a response for.
    int32_t next_transaction_id_;
    CircularBuffer<TransactionCallback*> queue_;
};

}  // namespace dtxn

#endif
