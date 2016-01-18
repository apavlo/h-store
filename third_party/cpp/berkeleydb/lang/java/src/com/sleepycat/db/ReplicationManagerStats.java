/*-
 * Automatically built by dist/s_java_stat.
 * Only the javadoc comments can be edited.
 *
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2015 Oracle and/or its affiliates.  All rights reserved.
 */

package com.sleepycat.db;

/**
Replication Manager statistics for a database environment.
*/
public class ReplicationManagerStats {
    // no public constructor
    /* package */ ReplicationManagerStats() {}

    private long st_perm_failed;
    /**
    The number of times a message critical for maintaining database
    integrity (for example, a transaction commit), originating at this site,
    did not receive sufficient acknowledgement from clients, according to
    the configured acknowledgement policy and acknowledgement timeout
    */
    public long getPermFailed() {
        return st_perm_failed;
    }

    private long st_msgs_queued;
    /**
    The number of outgoing messages which could not be transmitted
    immediately, due to a full network buffer, and had to be queued for
    later delivery.
    */
    public long getMsgsQueued() {
        return st_msgs_queued;
    }

    private long st_msgs_dropped;
    /**
    The number of outgoing messages that were dropped, because the outgoing
    message queue was full. (Berkeley DB replication is tolerant of dropped
    messages, and will automatically request retransmission of any missing
    messages as needed.) 
    */
    public long getMsgsDropped() {
        return st_msgs_dropped;
    }

    private int st_incoming_queue_gbytes;
    /**
    Gigabytes component of the memory consumption for the messages
    currently in the incoming queue. 
    */
    public int getIncomingQueueGbytes() {
        return st_incoming_queue_gbytes;
    }

    private int st_incoming_queue_bytes;
    /**
    Bytes component of the memory consumption for the messages currently
    in the incoming queue.
    */
    public int getIncomingQueueBytes() {
        return st_incoming_queue_bytes;
    }

    private long st_incoming_msgs_dropped;
    /**
    The number of incoming messages that were dropped because the incoming
    queue was full. (Berkeley DB replication is tolerant of dropped
    messages, and will automatically request retransmission of any missing
    messages as needed.) 
    */
    public long getIncomingMsgsDropped() {
        return st_incoming_msgs_dropped;
    }

    private long st_connection_drop;
    /**
    The number of existing connections that have been dropped
    since the statistics were last reset.
    */
    public long getConnectionDrop() {
        return st_connection_drop;
    }

    private long st_connect_fail;
    /**
    The number of times an attempt to open a new TCP/IP connection
    failed.
    */
    public long getConnectFail() {
        return st_connect_fail;
    }

    private int st_elect_threads;
    /** 
    Number of currently active election threads.
    */
    public int getElectThreads() {
        return st_elect_threads;
    }

    private int st_max_elect_threads;
    /** 
    The number of election threads for which space is reserved.
    */
    public int getMaxElectThreads() {
        return st_max_elect_threads;
    }

    private int st_site_participants;
    /**
    Number of replication group participant sites.
    */
    public int getSiteParticipants() {
        return st_site_participants;
    }

    private int st_site_total;
    /**
    Total number of replication group sites.
    */
    public int getSiteTotal() {
        return st_site_total;
    }

    private int st_site_views;
    /**
    Number of replication group view sites.
    */
    public int getSiteViews() {
        return st_site_views;
    }

    private long st_takeovers;
    /**
    The number of times a subordinate process took over as the replication
    process after a previous replication process has finished successfully. 
    */
    public long getTakeovers() {
        return st_takeovers;
    }

    /**
    For convenience, the ReplicationManagerStats class has a toString method
    that lists all the data fields.
    */
    public String toString() {
        return "ReplicationManagerStats:"
            + "\n  st_perm_failed=" + st_perm_failed
            + "\n  st_msgs_queued=" + st_msgs_queued
            + "\n  st_msgs_dropped=" + st_msgs_dropped
            + "\n  st_incoming_queue_gbytes=" + st_incoming_queue_gbytes
            + "\n  st_incoming_queue_bytes=" + st_incoming_queue_bytes
            + "\n  st_incoming_msgs_dropped=" + st_incoming_msgs_dropped
            + "\n  st_connection_drop=" + st_connection_drop
            + "\n  st_connect_fail=" + st_connect_fail
            + "\n  st_elect_threads=" + st_elect_threads
            + "\n  st_max_elect_threads=" + st_max_elect_threads
            + "\n  st_site_participants=" + st_site_participants
            + "\n  st_site_total=" + st_site_total
            + "\n  st_site_views=" + st_site_views
            + "\n  st_takeovers=" + st_takeovers
            ;
    }
}
