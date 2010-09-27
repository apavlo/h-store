// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "base/assert.h"
#include "protorpc/null.pb.h"
#include "protorpc/Protocol.pb.h"
#include "protorpc/sequencereader.h"
#include "google/protobuf/io/coded_stream.h"

using namespace protorpc;

int main(int argc, const char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "sequencereader (sequence file)\n");
        return 1;
    }
    const char* const log_path = argv[1];

    // Read the requests
    RpcRequest temp_request;
    NullMessage temp_embedded;
    int counter = 0;
    for (SequenceReader reader(log_path); reader.hasValue(); reader.advance()) {
        bool success = temp_request.ParseFromString(reader.current());
        ASSERT(success);
        success = temp_embedded.ParseFromString(temp_request.request());
        ASSERT(success);

        printf("%d %s %zd bytes { %s}\n\n", temp_request.sequence_number(), temp_request.method_name().c_str(),
                temp_request.request().size(),
                temp_embedded.DebugString().c_str());
        counter += 1;
    }
    printf("%d requests read from log\n", counter);

    // Avoid valgrind warnings
#ifndef DEBUG
    google::protobuf::ShutdownProtobufLibrary();
#endif
    return 0;
}
