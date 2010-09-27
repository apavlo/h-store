// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef REPLICATION_PRIMARYBACKUPLISTENER_H__
#define REPLICATION_PRIMARYBACKUPLISTENER_H__

#include "replication/primarybackup.h"

class MessageConnection;

namespace dtxn {
class Partition;
}

namespace io {
class EventLoop;
}

namespace net {
class ConnectionHandle;
class MessageServer;
}

namespace replication {

/** Wraps a PrimaryBackupReplica, connecting it to the messaging protocol. */
class PrimaryBackupListener : public replication::PrimaryBackupCommunication {
public:
    // Does not own event_loop or server. Server must be destroyed after.
    PrimaryBackupListener(io::EventLoop* event_loop, net::MessageServer* server);
    ~PrimaryBackupListener();

    // This class owns the connections.
    void setPrimary(const std::vector<MessageConnection*>& member_connections);
    void setBackup();

    virtual void send(int replica_index, const Entry& entry);
    virtual void send(int replica_index, const Ack& ack);
    virtual void flush();

    PrimaryBackupReplica* replica() { return &replica_; }

    // Idle callback handler for EventLoop.
    static bool idle(void* argument);

private:
    void entry(net::ConnectionHandle* handle, const Entry& entry);
    void ack(net::ConnectionHandle* handle, const Ack& ack);

    int handleToIndex(net::ConnectionHandle* handle);

    io::EventLoop* event_loop_;
    net::MessageServer* server_;
    std::vector<net::ConnectionHandle*> handles_;
    PrimaryBackupReplica replica_;
};

}  // namespace replication
#endif
