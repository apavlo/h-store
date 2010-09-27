// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef HSTORE_CONFIGPARSER_H
#define HSTORE_CONFIGPARSER_H

#include <cassert>
#include <string>
#include <vector>

#include "networkaddress.h"

namespace dtxn {

// Describes the configuration of a partition for the distributed transaction system. It specifies
// the network location of all the replicas, and a partition criteria.
// TODO: Does the partition criteria even make sense?
class Partition {
public:
    Partition() {}

    // Sets the partition criteria.
    void criteria(const std::string& c) { criteria_ = c; }
    const std::string& criteria() const { return criteria_; }

    // Add a replica at location
    void addReplica(const NetworkAddress& location) {
        replicas_.push_back(location);
    }

    int numReplicas() const { return static_cast<int>(replicas_.size()); }
    const NetworkAddress& replica(int index) const {
        assert(0 <= index && index < replicas_.size());
        return replicas_[index];
    }
    const std::vector<NetworkAddress>& replicas() const { return replicas_; }

    std::vector<NetworkAddress> backups() const {
        std::vector<NetworkAddress> copy = replicas_;
        copy.erase(copy.begin());
        return copy;
    }

private:
    // Partition criteria
    std::string criteria_;

    // Replicas in this partition.
    std::vector<NetworkAddress> replicas_;
};

// Fills partitions with the data parsed from configuration.
void parseConfiguration(const std::string& configuration, std::vector<Partition>* partitions);

// Returns the partitions parsed from file at configuration_path.
std::vector<Partition> parseConfigurationFromPath(const char* configuration_path);

// Returns the addresses of the primaries from partitions.
std::vector<NetworkAddress> primaryAddresses(const std::vector<Partition>& partitions);

}  // namespace dtxn
#endif  // CONFIG
