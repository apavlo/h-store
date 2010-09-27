// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "configparser.h"

#include <netdb.h>

#include "base/assert.h"
#include "strings/utils.h"

using std::string;
using std::vector;

namespace dtxn {

inline static bool isWhiteSpace(char c) {
    return c == ' ' || c == '\t';
}

void parseConfiguration(const string& configuration, vector<Partition>* partitions) {
    assert(partitions != NULL);

    // Split the string on "\n"
    vector<string> lines = strings::splitExcluding(configuration, '\n');

    for (int i = 0; i < lines.size(); ++i) {
        // Strip comments
        size_t index = lines[i].find('#');
        if (index != string::npos) {
            lines[i].resize(index);
        }

        // Strip trailing whitespace
        ssize_t end = lines[i].size();
        while (end > 0 && isWhiteSpace(lines[i][end-1])) {
            end -= 1;
        }
        if (end < lines[i].size()) {
            lines[i].resize(end);
        }

        // Strip leading whitespace
        int start = 0;
        while (start < lines[i].size() && isWhiteSpace(lines[i][start])) {
            start += 1;
        }
        if (start > 0) {
            lines[i] = lines[i].substr(start);
        }
    }

    bool inPartition = false;
    for (int i = 0; i < lines.size(); ++i) {
        // blank line: end of partition
        if (lines[i].empty()) {
            inPartition = false;
        } else if (!inPartition) {
            // Begin a new partition!
            inPartition = true;
            partitions->push_back(Partition());
            partitions->back().criteria(lines[i]);
        } else {
            // Continue an old partition: must be a replica
            NetworkAddress address;
            bool success = address.parse(lines[i]);
            ASSERT(success);
            partitions->back().addReplica(address);
        }
    }
}

vector<Partition> parseConfigurationFromPath(const char* configuration_path) {
    // Read and parse the configuration file.
    string data = strings::readFile(configuration_path);
    vector<Partition> partitions;
    parseConfiguration(data, &partitions);
    return partitions;
}

vector<NetworkAddress> primaryAddresses(const vector<Partition>& partitions) {
    vector<NetworkAddress> out;
    out.reserve(partitions.size());
    for (size_t i = 0; i < partitions.size(); ++i) {
        out.push_back(partitions[i].replica(0));
    }
    return out;
}

}  // namespace dtxn
