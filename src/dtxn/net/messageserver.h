// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef NET_MESSAGESERVER_H__
#define NET_MESSAGESERVER_H__

#include <cassert>
#include <tr1/unordered_set>
#include <string>

#include "base/unordered_map.h"
#include "messageconnection.h"

class MessageConnection;
class TCPConnection;

namespace io {
class EventLoop;
}

namespace net {

// An opaque type representing a connection. Used to send responses.
struct ConnectionHandle;

// Listens for messages, converting them into callbacks.
class MessageServer : public MessageConnectionCallback {
public:
    // Starts listening on port, using event_loop. Owns event_loop.
    //~ MessageServer();
    ~MessageServer();

    // Template magic to call a callback on any object via a method pointer.
    // TODO: Use tr1::function instead? Or generate an interface?
    template <typename ServerType, typename MessageType>
    void addCallback(
            void (ServerType::*method)(ConnectionHandle* source, const MessageType& argument),
            ServerType* server) {
        typedef void (ServerType::*CallbackType)(
                ConnectionHandle* source,
                const MessageType& argument);
        class TemplateHandler : public MessageCallback {
        public:
            TemplateHandler(ServerType* callback_server, CallbackType callback_method) :
                    server_(callback_server),
                    method_(callback_method) {}
            virtual void callback(ConnectionHandle* source, const std::string& message) {
                message_.parseFromString(message.data(), message.data() + message.size());
                (server_->*method_)(source, message_);
            }

        private:
            // reused to parse messages, avoiding excess calls to allocate/free memory
            MessageType message_;
            ServerType* server_;
            const CallbackType method_;
        };

        assert(server != NULL);
        assert(method != NULL);
        internalAddCallback(MessageType::typeCode(), new TemplateHandler(server, method));
    }

    // Removes a previously registered callback
    template <typename ServerType, typename MessageType>
    void removeCallback(
            void (ServerType::*method)(ConnectionHandle* source, const MessageType& argument)) {
        internalRemoveCallback(MessageType::typeCode());
    }

    // Converts the message represented by type and buffer into an object, then calls the callback.
    virtual void messageReceived(MessageConnection* source, int32_t type, const std::string& buffer);
    virtual void connectionArrived(MessageConnection* connection);
    virtual void connectionClosed(MessageConnection* connection);

    template <typename T>
    bool send(ConnectionHandle* handle, const T& message) {
        MessageConnection* connection = handleToConnection(handle);
        if (connection != NULL) {
            return connection->send(message);
        }
        return false;
    }

    template <typename T>
    bool bufferedSend(ConnectionHandle* handle, const T& message) {
        MessageConnection* connection = handleToConnection(handle);
        if (connection != NULL) {
            connection->bufferedSend(message);
            return true;
        }
        return false;
    }

    bool flush(ConnectionHandle* handle) {
        MessageConnection* connection = handleToConnection(handle);
        if (connection != NULL) {
            connection->flush();
            return true;
        }
        return false;
    }

    // Transfers ownership of connection to this message server. This is used to connect "client"
    // connections to this server. Returns the handle for this connection.
    ConnectionHandle* addConnection(MessageConnection* connection);

    // Calls addConnection for each connection in connections.
    std::vector<ConnectionHandle*> addConnections(
            const std::vector<MessageConnection*> connections) {
        std::vector<ConnectionHandle*> out;
        out.reserve(connections.size());
        for (size_t i = 0; i < connections.size(); ++i) {
            out.push_back(addConnection(connections[i]));
        }
        return out;
    }

    // Closes the connection represented by handle.
    void closeConnection(ConnectionHandle* handle);
    
    // Returns true if a handler for messages with type_code is registered.
    bool registered(int32_t type_code) const {
        return callbacks_.find(type_code) != callbacks_.end();
    }

    template <typename T>
    bool registered() const {
        return registered(T::typeCode());
    }

private:
    // Interface for calling a message callback on any object.
    class MessageCallback {
    public:
        virtual ~MessageCallback() {}
        virtual void callback(ConnectionHandle* source, const std::string& message) = 0;
    };

    MessageConnection* handleToConnection(ConnectionHandle* connection);

    void internalAddCallback(int32_t type, MessageCallback* handler);
    void internalRemoveCallback(int32_t type);

    typedef base::unordered_map<int32_t, MessageCallback*> CallbackMap;
    CallbackMap callbacks_;

    typedef std::tr1::unordered_set<MessageConnection*> ConnectionSet;
    // Used to validate connection handles used to sent responses.
    ConnectionSet connections_;
};

}  // namespace net
#endif
