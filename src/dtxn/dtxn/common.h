// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_COMMON_H
#define DTXN_COMMON_H

namespace io {
class EventLoop;
}

// Utility functions shared by multiple dtxn implementations.
namespace dtxn {

class Coordinator;
class Partition;
class Scheduler;

// Runs a server using scheduler for partition. This server is the replica numbered replica_index.
// scheduler will be deleted by this function. The event_loop will be used for events. This
// permits the caller to hook up other stuff (eg. other network connections).
void runServer(io::EventLoop* event_loop, Scheduler* scheduler, const Partition& partition,
        int replica_index);

// Runs a distributor using coordinator on port. coordinator will not be deleted by the function.
void runDistributor(Coordinator* coordinator, int port, const char* configuration_file);

}  // namespace dtxn

#endif
