// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "protorpc/sequencereader.h"

#include <fcntl.h>

#include "base/assert.h"
#include <google/protobuf/io/coded_stream.h>  // system include avoids conversion warnings
#include "google/protobuf/io/zero_copy_stream_impl.h"

using google::protobuf::io::CodedInputStream;
using google::protobuf::io::FileInputStream;
using std::string;

namespace protorpc {

SequenceReader::SequenceReader(const string& file_path) {
    int fd = open(file_path.c_str(), O_RDONLY, 0);
    assert(fd >= 0);
    input_ = new FileInputStream(fd);
    advance();
}

void SequenceReader::advance() {
    // Attempt to read the next sequence
    CodedInputStream in(input_);
    uint32_t size = 0;
    bool success = in.ReadVarint32(&size);
    if (!success) {
        // we are probably at EOF
        close();
        return;
    }

    success = in.ReadString(&buffer_, size);
    assert(success);
    assert(buffer_.size() == size);
}

void SequenceReader::close() {
    assert(input_->GetErrno() == 0);
    bool success = input_->Close();
    ASSERT(success);
    delete input_;
    input_ = NULL;
}

}  // namespace protorpc
