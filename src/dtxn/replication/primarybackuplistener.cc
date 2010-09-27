// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "replication/primarybackuplistener.h"

#include <netinet/in.h>

#include "dtxn/configparser.h"
#include "io/libeventloop.h"
#include "io/message.h"
#include "net/messageserver.h"
#include "serialization.h"

using std::string;
using std::vector;

namespace replication {

PrimaryBackupListener::PrimaryBackupListener(
        io::EventLoop* event_loop, net::MessageServer* server) :

        event_loop_(event_loop),
        server_(server),
        replica_(this) {
    event_loop_->addIdleCallback(PrimaryBackupListener::idle, this);
    server_->addCallback(&PrimaryBackupListener::entry, this);
    server_->addCallback(&PrimaryBackupListener::ack, this);
}

PrimaryBackupListener::~PrimaryBackupListener() {
    event_loop_->removeIdleCallback(PrimaryBackupListener::idle);
}

void PrimaryBackupListener::setPrimary(const vector<MessageConnection*>& member_connections) {
    // Connect to the replicas: the first index is ourself.
    assert(handles_.empty());
    handles_.push_back(NULL);
    STLExtend(&handles_, server_->addConnections(member_connections));

    replica_.setPrimary((int) handles_.size());
}

void PrimaryBackupListener::setBackup() {
    // Indicates that this is a backup
    handles_.push_back(NULL);
    replica_.setBackup();
}

void PrimaryBackupListener::send(int replica_index, const replication::Entry& entry) {
    if (buffered()) {
        server_->bufferedSend(handles_[replica_index], entry);
    } else {
        server_->send(handles_[replica_index], entry);
    }
}

void PrimaryBackupListener::send(int replica_index, const replication::Ack& ack) {
    if (buffered()) {
        server_->bufferedSend(handles_[replica_index], ack);
    } else {
        server_->send(handles_[replica_index], ack);
    }
}

void PrimaryBackupListener::flush() {
    assert(buffered());
    assert(!handles_.empty());

    // flush all connections
    if (handles_.size() == 1) {
        server_->flush(handles_[0]);
    } else {
        // flush all except our own connection (which is NULL)
        for (size_t i = 1; i < handles_.size(); ++i) {
            server_->flush(handles_[i]);
        }
    }
}

void PrimaryBackupListener::entry(net::ConnectionHandle* handle, const Entry& entry) {
    assert(handles_.size() == 1);
    if (handles_[0] == NULL) {
        handles_[0] = handle;
    }
    assert(handles_[0] == handle);
    replica_.receive(handleToIndex(handle), entry);
}

void PrimaryBackupListener::ack(net::ConnectionHandle* handle, const Ack& ack) {
    replica_.receive(handleToIndex(handle), ack);
}

bool PrimaryBackupListener::idle(void* argument) {
    PrimaryBackupListener* listener = reinterpret_cast<PrimaryBackupListener*>(argument);

    // TODO: This flush is a hack to flush pending ACKs before delivering the committed log
    // entries when running as a backup. Figure out a better policy.
    if (listener->buffered()) listener->flush();

    listener->replica_.idle();
    return false;
}

int PrimaryBackupListener::handleToIndex(net::ConnectionHandle* handle) {
    for (int i = 0; i < handles_.size(); ++i) {
        if (handles_[i] == handle) {
            return i;
        }
    }
    assert(false);
    return -1;
}

}  // namespace replication
