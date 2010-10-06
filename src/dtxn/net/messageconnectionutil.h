// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef NET_MESSAGECONNECTIONUTIL_H__
#define NET_MESSAGECONNECTIONUTIL_H__

#include <vector>

class MessageConnection;
class NetworkAddress;
class TCPConnection;

namespace io {
class EventLoop;
}

namespace net {

// Utility function for creating a set of TCP connections.
std::vector<MessageConnection*> createConnections(
        io::EventLoop* event_loop,
        const std::vector<NetworkAddress>& addresses);

std::vector<TCPConnection*> createConnectionsWithRetry(
        io::EventLoop* event_loop,
        const std::vector<NetworkAddress>& addresses);

}  // namespace net
#endif
