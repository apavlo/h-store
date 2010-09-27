// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN2_COMMON_H
#define DTXN2_COMMON_H

namespace dtxn {
class Partition;
}

namespace io {
class EventLoop;
}

namespace dtxn2 {

class Scheduler;

// Runs a server using scheduler for partition. This server is the replica numbered replica_index.
// The event_loop will be used for events. This permits the caller to hook up other stuff (eg.
// other network connections).
void runServer(io::EventLoop* event_loop, Scheduler* scheduler, const dtxn::Partition& partition,
        int replica_index);

}  // namespace dtxn2

#endif
