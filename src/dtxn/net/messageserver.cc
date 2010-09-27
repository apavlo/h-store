// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "net/messageserver.h"

#include "base/stlutil.h"

using std::string;

namespace net {

MessageServer::~MessageServer() {
    STLDeleteValues(&callbacks_);
    STLDeleteElements(&connections_);
}

void MessageServer::messageReceived(
        MessageConnection* source, int32_t type, const string& buffer) {
    CallbackMap::iterator i = callbacks_.find(type);
    assert(i != callbacks_.end());
    assert(connections_.find(source) != connections_.end());

    // TODO: We should ensure that handles are unique per "client." For example, if a client
    // connects, the TCP connection breaks and it re-establishes communication, it should get the
    // same handle. More importantly, if a connection is closed and a new one arrives, they must
    // get *different* handles! This probably means this needs to be a sequence number mapping
    // to a connection via a hash table.
    i->second->callback(reinterpret_cast<ConnectionHandle*>(source), buffer);
}

void MessageServer::connectionArrived(MessageConnection* connection) {
    assert(connections_.find(connection) == connections_.end());
    
    connections_.insert(connection);
}

void MessageServer::connectionClosed(MessageConnection* connection) {
    ConnectionSet::iterator i = connections_.find(connection);
    assert(i != connections_.end());
    connections_.erase(i);
    delete connection;
}

ConnectionHandle* MessageServer::addConnection(MessageConnection* connection) {
    connection->setTarget(this);
    connectionArrived(connection);
    return reinterpret_cast<ConnectionHandle*>(connection);
}

void MessageServer::closeConnection(ConnectionHandle* handle) {
    MessageConnection* connection = handleToConnection(handle);
    if (connection != NULL) {
        connectionClosed(connection);
    }
}

void MessageServer::internalAddCallback(int32_t message_id, MessageCallback* handler) {
    std::pair<CallbackMap::iterator, bool> result =
            callbacks_.insert(std::make_pair(message_id, handler));
    assert(result.second);
}

void MessageServer::internalRemoveCallback(int32_t message_id) {
    CallbackMap::iterator it = callbacks_.find(message_id);
    assert(it != callbacks_.end());
    delete it->second;
    callbacks_.erase(it);
}

MessageConnection* MessageServer::handleToConnection(ConnectionHandle* handle) {
    MessageConnection* connection = reinterpret_cast<MessageConnection*>(handle);
    if (connections_.find(connection) != connections_.end()) {
        return connection;
    }
    return NULL;
}

}
