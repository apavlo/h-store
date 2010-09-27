// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "net/messageconnectionutil.h"

#include <netinet/in.h>

#include "messageconnection.h"
#include "io/libeventloop.h"
#include "networkaddress.h"
#include "tcplistener.h"

using std::vector;

namespace net {

// TODO: Make TCPMessageConnection take an EventLoop not an event_base.
vector<MessageConnection*> createConnections(
        io::EventLoop* event_loop,
        const vector<NetworkAddress>& addresses) {
    vector<MessageConnection*> connections;
    for (int i = 0; i < addresses.size(); ++i) {
        int sock = connectTCP(addresses[i].sockaddr());
        connections.push_back(new TCPMessageConnection(
                ((io::LibEventLoop*) event_loop)->base(), new TCPConnection(sock)));
    }

    return connections;
}

}  // namespace net
