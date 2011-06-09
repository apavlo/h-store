LOG_LEVEL=500

PROTOBUF_PROTOC=$(PROTOBUF_BIN)/src/protoc
PROTOBUF_PROTOC=$(PROTOBUF_BIN)/src/protoc

DTXN_PROTO_NAME=edu/mit/dtxn/dtxn
DTXN_PROTO_PATH=$(PROTORPC_PATH)/$(DTXN_PROTO_NAME).proto
DTXN_PROTO_SYMLINK:=$(PREFIX)/protodtxn/$(shell basename $(DTXN_PROTO_NAME)).pb.h

CCFLAGS=-g -D_EVENT_HAVE_CONFIG_H -DLOG_LEVEL=$(LOG_LEVEL) -Wall -Wextra -Werror -Wformat-y2k -Wformat-nonliteral -Wformat-security -Wmissing-include-dirs -Wswitch-enum -Winit-self -Wundef -Wpointer-arith -Wcast-align -Wlogical-op -Wredundant-decls -Winvalid-pch -Wwrite-strings -Wno-sign-compare -Wno-unused-parameter
# -Woverloaded-virtual -Wconversion

LDFLAGS=$(PROTOBUF_BIN)/src/.libs/libprotobuf.a -lrt

CC=gcc
CC_COMPILE=$(CC) $(CCFLAGS) -I. -I$(PREFIX) -I$(PROTOBUF_SRC)/src -c -o $@ $<
CC_LIBRARY=ar rcs $@ $^
CC_BINARY=g++ -o $@ $^ $(LDFLAGS) 

all: $(PREFIX) protobuf protorpc io libevent logging \
$(PREFIX)/$(DTXN_PROTO_NAME).pb.o \
$(PREFIX)/dtxn/libdtxn.a \
$(PREFIX)/messageconnection.o \
$(PREFIX)/randomgenerator.o \
$(PREFIX)/networkaddress.o \
$(PREFIX)/buffer.o \
$(PREFIX)/serialization.o \
$(PREFIX)/strings/json.o \
$(PREFIX)/strings/utils.o \
$(PREFIX)/tcplistener.o \
$(PREFIX)/net/messageserver.o \
$(PREFIX)/net/messageserver_demo.o \
$(PREFIX)/net/messageconnectionutil.o \
$(PREFIX)/net/messageserver_demo \
$(PREFIX)/replication/primarybackuplistener.o \
$(PREFIX)/replication/primarybackup.o \
$(PREFIX)/replication/nulllog.o \
$(PREFIX)/protodtxn/protodtxncoordinator \
$(PREFIX)/protodtxn/protodtxnengine \
$(PREFIX)/stupidunit/stupidunit.o \
$(PREFIX)/dtxn2/common.o \
$(PREFIX)/dtxn2/blockingscheduler.o \
$(PREFIX)/dtxn2/partitionserver.o \
$(PREFIX)/dtxn2/protopartitionserver.o

$(PREFIX):
	mkdir -p $(PREFIX)/protodtxn
	mkdir -p $(PREFIX)/dtxn2
	mkdir -p $(PREFIX)/protorpc
	mkdir -p $(PREFIX)/io
	mkdir -p $(PREFIX)/dtxn/locks
	mkdir -p $(PREFIX)/dtxn
	mkdir -p $(PREFIX)/replication
	mkdir -p $(PREFIX)/base
	mkdir -p $(PREFIX)/dtxn/ordered
	mkdir -p $(PREFIX)/libevent
	mkdir -p $(PREFIX)/net
	mkdir -p $(PREFIX)/logging
	mkdir -p $(PREFIX)/dtxn/occ
	mkdir -p $(PREFIX)/stupidunit
	mkdir -p $(PREFIX)/strings


clean:
	rm -rf $(PREFIX)

## ------------------------------------------------------------------
## Protocol Buffer Stuff
## ------------------------------------------------------------------
protobuf: $(PREFIX) $(PREFIX)/$(DTXN_PROTO_NAME).pb.cc $(PREFIX)/protorpc/Protocol.pb.cc $(PREFIX)/protorpc/null.pb.cc

$(PREFIX)/$(DTXN_PROTO_NAME).pb.cc: $(DTXN_PROTO_PATH)
	$(PROTOBUF_PROTOC) --proto_path=$(PROTORPC_PATH) --cpp_out=$(PREFIX) $(DTXN_PROTO_PATH)
	test -L $(DTXN_PROTO_SYMLINK) || ln -s $(PREFIX)/$(DTXN_PROTO_NAME).pb.h $(DTXN_PROTO_SYMLINK)
$(PREFIX)/protorpc/Counter.pb.cc: protorpc/Counter.proto
	$(PROTOBUF_PROTOC) --proto_path=./ --cpp_out=$(PREFIX) protorpc/Counter.proto
$(PREFIX)/protorpc/Protocol.pb.cc: protorpc/Protocol.proto
	$(PROTOBUF_PROTOC) --proto_path=./ --cpp_out=$(PREFIX) protorpc/Protocol.proto
$(PREFIX)/protorpc/null.pb.cc: protorpc/null.proto
	$(PROTOBUF_PROTOC) --proto_path=./ --cpp_out=$(PREFIX) protorpc/null.proto

## ------------------------------------------------------------------
## PROTORPC
## ------------------------------------------------------------------
protorpc: $(PREFIX)/$(DTXN_PROTO_NAME).pb.o $(PREFIX)/protorpc/Counter.pb.o $(PREFIX)/protorpc/rpcreplay $(PREFIX)/protorpc/protoserver_test.o $(PREFIX)/protorpc/sequencereader $(PREFIX)/protorpc/sequencereader_main.o $(PREFIX)/protorpc/protorpcchannel_test.o $(PREFIX)/protorpc/sequencereader.o $(PREFIX)/protorpc/protoconnectionbuffer.o $(PREFIX)/protorpc/protoconnectionbuffer_test.o $(PREFIX)/protorpc/protorpcchannel.o $(PREFIX)/protorpc/serviceregistry.o $(PREFIX)/protorpc/rpcreplay.o $(PREFIX)/protorpc/rpcproxy.o $(PREFIX)/protorpc/rpcproxy $(PREFIX)/protorpc/protoserver.o $(PREFIX)/protorpc/protorpccontroller.o $(PREFIX)/protorpc/serviceregistry_test.o
$(PREFIX)/protorpc/rpcreplay: $(PREFIX)/protorpc/rpcreplay.o $(PREFIX)/protorpc/Protocol.pb.o $(PREFIX)/protorpc/null.pb.o $(PREFIX)/protorpc/protoconnectionbuffer.o $(PREFIX)/protorpc/protorpcchannel.o $(PREFIX)/protorpc/protorpccontroller.o $(PREFIX)/protorpc/sequencereader.o $(PREFIX)/io/buffer.o $(PREFIX)/networkaddress.o $(PREFIX)/randomgenerator.o $(PREFIX)/strings/utils.o $(PREFIX)/tcplistener.o $(PREFIX)/io/libeventloop.o $(PREFIX)/libevent/libevent_core.a
	$(CC_BINARY)
$(PREFIX)/protorpc/protoserver.o: protorpc/protoserver.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/Protocol.pb.o: $(PREFIX)/protorpc/Protocol.pb.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/serviceregistry_test.o: protorpc/serviceregistry_test.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/protoconnectionbuffer_test.o: protorpc/protoconnectionbuffer_test.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/protoconnectionbuffer.o: protorpc/protoconnectionbuffer.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/protorpccontroller.o: protorpc/protorpccontroller.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/Counter.pb.o: $(PREFIX)/protorpc/Counter.pb.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/null.pb.o: $(PREFIX)/protorpc/null.pb.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/rpcproxy: $(PREFIX)/protorpc/rpcproxy.o $(PREFIX)/protorpc/Protocol.pb.o $(PREFIX)/protorpc/null.pb.o $(PREFIX)/protorpc/protoconnectionbuffer.o $(PREFIX)/protorpc/protorpcchannel.o $(PREFIX)/protorpc/protorpccontroller.o $(PREFIX)/protorpc/protoserver.o $(PREFIX)/io/buffer.o $(PREFIX)/networkaddress.o $(PREFIX)/strings/utils.o $(PREFIX)/tcplistener.o $(PREFIX)/io/libeventloop.o $(PREFIX)/libevent/libevent_core.a
	$(CC_BINARY)
$(PREFIX)/protorpc/protorpcchannel.o: protorpc/protorpcchannel.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/serviceregistry.o: protorpc/serviceregistry.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/rpcreplay.o: protorpc/rpcreplay.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/rpcproxy.o: protorpc/rpcproxy.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/protoserver_test.o: protorpc/protoserver_test.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/sequencereader: $(PREFIX)/protorpc/sequencereader_main.o $(PREFIX)/protorpc/sequencereader.o $(PREFIX)/protorpc/Protocol.pb.o $(PREFIX)/protorpc/null.pb.o
	$(CC_BINARY)
$(PREFIX)/protorpc/sequencereader.o: protorpc/sequencereader.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/sequencereader_main.o: protorpc/sequencereader_main.cc
	$(CC_COMPILE)

## ------------------------------------------------------------------
## LIBEVENT
## ------------------------------------------------------------------
#libevent:  $(PREFIX)/libevent/strlcpy.o $(PREFIX)/libevent/poll.o $(PREFIX)/libevent/http.o $(PREFIX)/libevent/epoll.o $(PREFIX)/libevent/epoll_sub.o $(PREFIX)/libevent/evutil.o $(PREFIX)/libevent/log.o $(PREFIX)/libevent/select.o $(PREFIX)/libevent/signal.o $(PREFIX)/libevent/event.o $(PREFIX)/libevent/event_tagging.o $(PREFIX)/libevent/buffer.o  $(PREFIX)/libevent/libevent_core.a $(PREFIX)/libevent/libevent_http.a

$(PREFIX)/libevent/strlcpy.o: libevent/strlcpy.c
	$(CC_COMPILE)
$(PREFIX)/libevent/poll.o: libevent/poll.c
	$(CC_COMPILE)
#$(PREFIX)/libevent/http.o: libevent/http.c
#	$(CC_COMPILE)
$(PREFIX)/libevent/epoll_sub.o: libevent/epoll_sub.c
	$(CC_COMPILE)
$(PREFIX)/libevent/epoll.o: libevent/epoll.c
	$(CC_COMPILE)
#$(PREFIX)/libevent/libevent_http.a: $(PREFIX)/libevent/http.o $(PREFIX)/libevent/strlcpy.o
#	$(CC_LIBRARY)
$(PREFIX)/libevent/evmap.o: libevent/evmap.c
	$(CC_COMPILE)
$(PREFIX)/libevent/evthread.o: libevent/evthread.c
	$(CC_COMPILE)
$(PREFIX)/libevent/evthread_pthread.o: libevent/evthread_pthread.c
	$(CC_COMPILE)
$(PREFIX)/libevent/evutil.o: libevent/evutil.c
	$(CC_COMPILE)
$(PREFIX)/libevent/log.o: libevent/log.c
	$(CC_COMPILE)
$(PREFIX)/libevent/select.o: libevent/select.c
	$(CC_COMPILE)
$(PREFIX)/libevent/signal.o: libevent/signal.c
	$(CC_COMPILE)
$(PREFIX)/libevent/event.o: libevent/event.c
	$(CC_COMPILE)
$(PREFIX)/libevent/libevent_core.a: $(PREFIX)/libevent/event.o $(PREFIX)/libevent/buffer.o $(PREFIX)/libevent/log.o $(PREFIX)/libevent/evmap.o $(PREFIX)/libevent/evthread.o $(PREFIX)/libevent/evthread_pthread.o $(PREFIX)/libevent/evutil.o $(PREFIX)/libevent/select.o $(PREFIX)/libevent/poll.o $(PREFIX)/libevent/epoll.o $(PREFIX)/libevent/signal.o $(PREFIX)/libevent/strlcpy.o
	$(CC_LIBRARY)
$(PREFIX)/libevent/event_tagging.o: libevent/event_tagging.c
	$(CC_COMPILE)
$(PREFIX)/libevent/buffer.o: libevent/buffer.c
	$(CC_COMPILE)

## ------------------------------------------------------------------
## IO
## ------------------------------------------------------------------
io: $(PREFIX)/io/libeventloop.o $(PREFIX)/io/buffer.o
$(PREFIX)/io/libeventloop.o: io/libeventloop.cc
	$(CC_COMPILE)
$(PREFIX)/io/buffer.o: io/buffer.cc
	$(CC_COMPILE)
$(PREFIX)/io/libeventloop_test.o: io/libeventloop_test.cc
	$(CC_COMPILE)

## ------------------------------------------------------------------
## LOGGING
## ------------------------------------------------------------------
logging: $(PREFIX)/logging/minlogcrash.o $(PREFIX)/logging/logbench.o $(PREFIX)/logging/crc32c.o $(PREFIX)/logging/logfilecrashserver $(PREFIX)/logging/logfilecrash.o $(PREFIX)/logging/sequencefile.o $(PREFIX)/logging/minlogcrash $(PREFIX)/logging/crc32ctables.o  $(PREFIX)/logging/logfilecrash

$(PREFIX)/logging/minlogcrash.o: logging/minlogcrash.c
	$(CC_COMPILE)
$(PREFIX)/logging/logfile_test: $(PREFIX)/logging/logfile_test.o $(PREFIX)/logging/crc32c.o $(PREFIX)/logging/crc32ctables.o $(PREFIX)/logging/logfile.o $(PREFIX)/logging/sequencefile.o $(PREFIX)/io/buffer.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/logging/logbench.o: logging/logbench.cc
	$(CC_COMPILE)
$(PREFIX)/logging/crc32c.o: logging/crc32c.cc
	$(CC_COMPILE) -msse4.2
$(PREFIX)/logging/crc32c_test: $(PREFIX)/logging/crc32c_test.o $(PREFIX)/logging/crc32c.o $(PREFIX)/logging/crc32ctables.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/logging/logfile_test.o: logging/logfile_test.cc
	$(CC_COMPILE)
$(PREFIX)/logging/rndwritebench: $(PREFIX)/logging/rndwritebench.o $(PREFIX)/logging/crc32c.o $(PREFIX)/logging/crc32ctables.o $(PREFIX)/logging/logfile.o $(PREFIX)/logging/sequencefile.o $(PREFIX)/io/buffer.o $(PREFIX)/randomgenerator.o
	$(CC_BINARY)
$(PREFIX)/logging/crc32cbench: $(PREFIX)/logging/crc32cbench.o $(PREFIX)/logging/crc32c.o $(PREFIX)/logging/crc32ctables.o
	$(CC_BINARY)
$(PREFIX)/logging/sequencefile_test.o: logging/sequencefile_test.cc
	$(CC_COMPILE)
$(PREFIX)/logging/logfilecrashserver: $(PREFIX)/logging/logfilecrashserver.o
	$(CC_BINARY)
$(PREFIX)/logging/rndwritebench.o: logging/rndwritebench.cc
	$(CC_COMPILE)
$(PREFIX)/logging/logfilecrashserver.o: logging/logfilecrashserver.cc
	$(CC_COMPILE)
$(PREFIX)/logging/logfilebench: $(PREFIX)/logging/logfilebench.o $(PREFIX)/logging/crc32c.o $(PREFIX)/logging/crc32ctables.o $(PREFIX)/logging/logfile.o $(PREFIX)/logging/sequencefile.o $(PREFIX)/io/buffer.o
	$(CC_BINARY)
$(PREFIX)/logging/sequencefile_test: $(PREFIX)/logging/sequencefile_test.o $(PREFIX)/logging/sequencefile.o $(PREFIX)/logging/crc32c.o $(PREFIX)/logging/crc32ctables.o $(PREFIX)/io/buffer.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/logging/logfile.o: logging/logfile.cc
	$(CC_COMPILE)
$(PREFIX)/logging/logfilecrash.o: logging/logfilecrash.cc
	$(CC_COMPILE)
$(PREFIX)/logging/crc32ctables.o: logging/crc32ctables.cc
	$(CC_COMPILE)
$(PREFIX)/logging/sequencefile.o: logging/sequencefile.cc
	$(CC_COMPILE)
$(PREFIX)/logging/minlogcrash: $(PREFIX)/logging/minlogcrash.o
	$(CC_BINARY)
$(PREFIX)/logging/logfilebench.o: logging/logfilebench.cc
	$(CC_COMPILE)
$(PREFIX)/logging/crc32c_test.o: logging/crc32c_test.cc
	$(CC_COMPILE)
$(PREFIX)/logging/crc32cbench.o: logging/crc32cbench.cc
	$(CC_COMPILE)

## ------------------------------------------------------------------
## BASE TEST FILES
## ------------------------------------------------------------------
tests:	$(PREFIX)/stupidunit/stupidunit_test $(PREFIX)/replication/primarybackup_test $(PREFIX)/logging/logfile_test $(PREFIX)/io/memoryinputstream_test $(PREFIX)/dtxn/dtxndistributor_test $(PREFIX)/dtxn/transactionstate_test $(PREFIX)/logging/crc32c_test $(PREFIX)/dtxn/distributedtransaction_test $(PREFIX)/protorpc/protoserver_test $(PREFIX)/dtxn/transaction_test $(PREFIX)/strings/utils_test $(PREFIX)/dtxn/dtxnserver_test $(PREFIX)/dtxn2/blockingscheduler_test $(PREFIX)/protorpc/protoconnectionbuffer_test $(PREFIX)/protodtxn/protodtxncoordinator_test $(PREFIX)/dtxn2/partitionserver_test $(PREFIX)/dtxn2/protopartitionserver_test $(PREFIX)/protorpc/protorpcchannel_test $(PREFIX)/logging/sequencefile_test $(PREFIX)/replication/primarybackuplistener_test $(PREFIX)/io/libeventloop_test $(PREFIX)/replication/nulllog_test $(PREFIX)/net/messageserver_test $(PREFIX)/protorpc/serviceregistry_test $(PREFIX)/protodtxn/protodtxnengine_test $(PREFIX)/strings/json_test $(PREFIX)/io/buffer_test $(PREFIX)/dtxn/configparser_test $(PREFIX)/replication/cachedslidingarray_test $(PREFIX)/serialization_test $(PREFIX)/dtxn/locks/lockscheduler_test $(PREFIX)/dtxn/occ/occtransaction_test $(PREFIX)/tcplistener_test $(PREFIX)/networkaddress_test $(PREFIX)/dtxn/occ/occscheduler_test 

$(PREFIX)/base/chunkedarray_test.o: base/chunkedarray_test.cc
	$(CC_COMPILE)
$(PREFIX)/base/array_test.o: base/array_test.cc
	$(CC_COMPILE)
$(PREFIX)/base/cachedcircularbuffer_test.o: base/cachedcircularbuffer_test.cc
	$(CC_COMPILE)
$(PREFIX)/base/unordered_map_test: $(PREFIX)/base/unordered_map_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/base/function_thread.o: base/function_thread.cc
	$(CC_COMPILE)
$(PREFIX)/base/chunkedarray_test: $(PREFIX)/base/chunkedarray_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/base/circularbuffer_test.o: base/circularbuffer_test.cc
	$(CC_COMPILE)
$(PREFIX)/base/time_test: $(PREFIX)/base/time_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/base/assert_test.o: base/assert_test.cc
	$(CC_COMPILE)
$(PREFIX)/base/unordered_set_test: $(PREFIX)/base/unordered_set_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/base/array_test: $(PREFIX)/base/array_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/base/unordered_set_test.o: base/unordered_set_test.cc
	$(CC_COMPILE)
$(PREFIX)/base/assert_test: $(PREFIX)/base/assert_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/base/unordered_map_test.o: base/unordered_map_test.cc
	$(CC_COMPILE)
$(PREFIX)/base/slidingarray_test: $(PREFIX)/base/slidingarray_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/base/stringutil_test.o: base/stringutil_test.cc
	$(CC_COMPILE)
$(PREFIX)/base/cachedcircularbuffer_test: $(PREFIX)/base/cachedcircularbuffer_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/base/stlutil_test: $(PREFIX)/base/stlutil_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/base/cast_test: $(PREFIX)/base/cast_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/base/stlutil_test.o: base/stlutil_test.cc
	$(CC_COMPILE)
$(PREFIX)/base/circularbuffer_test: $(PREFIX)/base/circularbuffer_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/protodtxn/protodtxncoordinator_test.o: protodtxn/protodtxncoordinator_test.cc
	$(CC_COMPILE)
$(PREFIX)/net/messageserver_test.o: net/messageserver_test.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/serviceregistry_test: $(PREFIX)/protorpc/serviceregistry_test.o $(PREFIX)/protorpc/Counter.pb.o $(PREFIX)/protorpc/Protocol.pb.o $(PREFIX)/protorpc/protorpccontroller.o $(PREFIX)/protorpc/serviceregistry.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/protorpc/protorpcchannel_test: $(PREFIX)/protorpc/protorpcchannel_test.o $(PREFIX)/protorpc/Counter.pb.o $(PREFIX)/protorpc/Protocol.pb.o $(PREFIX)/protorpc/protoconnectionbuffer.o $(PREFIX)/protorpc/protorpcchannel.o $(PREFIX)/protorpc/protorpccontroller.o $(PREFIX)/base/function_thread.o $(PREFIX)/io/buffer.o $(PREFIX)/io/libeventloop.o $(PREFIX)/libevent/libevent_core.a $(PREFIX)/stupidunit/stupidunit.o $(PREFIX)/tcplistener.o
	$(CC_BINARY)
$(PREFIX)/protorpc/protoconnectionbuffer_test: $(PREFIX)/protorpc/protoconnectionbuffer_test.o $(PREFIX)/protorpc/protoconnectionbuffer.o $(PREFIX)/protorpc/Counter.pb.o $(PREFIX)/io/buffer.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/protorpc/protoserver_test: $(PREFIX)/protorpc/protoserver_test.o $(PREFIX)/protorpc/Counter.pb.o $(PREFIX)/protorpc/Protocol.pb.o $(PREFIX)/protorpc/protoconnectionbuffer.o $(PREFIX)/protorpc/protorpccontroller.o $(PREFIX)/protorpc/protoserver.o $(PREFIX)/protorpc/serviceregistry.o $(PREFIX)/io/buffer.o $(PREFIX)/io/libeventloop.o $(PREFIX)/libevent/libevent_core.a $(PREFIX)/stupidunit/stupidunit.o $(PREFIX)/tcplistener.o
	$(CC_BINARY)	

## ------------------------------------------------------------------
## THE GOOD STUFF
## ------------------------------------------------------------------

$(PREFIX)/dtxn/ordered/orderedscheduler.o: dtxn/ordered/orderedscheduler.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/libdtxn.a: $(PREFIX)/dtxn/common.o $(PREFIX)/dtxn/configparser.o $(PREFIX)/dtxn/dtxnclient.o $(PREFIX)/dtxn/dtxnserver.o $(PREFIX)/dtxn/transactionstate.o $(PREFIX)/dtxn/locks/lock.o $(PREFIX)/dtxn/locks/locktransaction.o $(PREFIX)/dtxn/locks/lockscheduler.o $(PREFIX)/dtxn/occ/occscheduler.o $(PREFIX)/dtxn/ordered/orderedclient.o $(PREFIX)/dtxn/ordered/orderedscheduler.o $(PREFIX)/dtxn/ordered/ordereddistributor.o $(PREFIX)/io/libeventloop.o $(PREFIX)/net/messageserver.o $(PREFIX)/replication/primarybackup.o $(PREFIX)/replication/primarybackuplistener.o
	$(CC_LIBRARY)
$(PREFIX)/stupidunit/stupidunit_test: $(PREFIX)/stupidunit/stupidunit_test.o $(PREFIX)/stupidunit/stupidunit.o $(PREFIX)/strings/utils.o
	$(CC_BINARY)
$(PREFIX)/replication/primarybackup_test: $(PREFIX)/replication/primarybackup_test.o $(PREFIX)/replication/primarybackup.o $(PREFIX)/io/buffer.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn/dtxnserver.o: dtxn/dtxnserver.cc
	$(CC_COMPILE)
$(PREFIX)/buffer.o: buffer.cc
	$(CC_COMPILE)
$(PREFIX)/simpleclient: $(PREFIX)/simpleclient.o $(PREFIX)/buffer.o $(PREFIX)/messageconnection.o $(PREFIX)/networkaddress.o $(PREFIX)/tcplistener.o $(PREFIX)/io/buffer.o $(PREFIX)/strings/utils.o $(PREFIX)/libevent/libevent_core.a
	$(CC_BINARY)
$(PREFIX)/replication/primarybackuplistener.o: replication/primarybackuplistener.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/occ/occscheduler.o: dtxn/occ/occscheduler.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/occ/occscheduler_test.o: dtxn/occ/occscheduler_test.cc
	$(CC_COMPILE)
$(PREFIX)/protorpc/protorpcchannel_test.o: protorpc/protorpcchannel_test.cc
	$(CC_COMPILE)
$(PREFIX)/protodtxn/protodtxnengine.o: protodtxn/protodtxnengine.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn2/partitionserver_test.o: dtxn2/partitionserver_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn2/protopartitionserver_test.o: dtxn2/protopartitionserver_test.cc
	$(CC_COMPILE)
$(PREFIX)/base/cast_test.o: base/cast_test.cc
	$(CC_COMPILE)
$(PREFIX)/replication/nulllog_test.o: replication/nulllog_test.cc
	$(CC_COMPILE)
$(PREFIX)/messageconnection_test.o: messageconnection_test.cc
	$(CC_COMPILE)
$(PREFIX)/messageconnection.o: messageconnection.cc
	$(CC_COMPILE)
$(PREFIX)/base/slidingarray_test.o: base/slidingarray_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/dtxnserver_test.o: dtxn/dtxnserver_test.cc
	$(CC_COMPILE)
$(PREFIX)/stupidunit/stupidunit.o: stupidunit/stupidunit.cc
	$(CC_COMPILE)
$(PREFIX)/replication/primarybackup.o: replication/primarybackup.cc
	$(CC_COMPILE)
$(PREFIX)/protodtxn/protodtxnengine_main.o: protodtxn/protodtxnengine_main.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/ordered/orderedscheduler_test.o: dtxn/ordered/orderedscheduler_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/locks/locktransaction.o: dtxn/locks/locktransaction.cc
	$(CC_COMPILE)
$(PREFIX)/$(DTXN_PROTO_NAME).pb.o: $(PREFIX)/$(DTXN_PROTO_NAME).pb.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/ordered/orderedclient.o: dtxn/ordered/orderedclient.cc
	$(CC_COMPILE)
$(PREFIX)/io/memoryinputstream_test: $(PREFIX)/io/memoryinputstream_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/strings/utils_test.o: strings/utils_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/dtxndistributor_test: $(PREFIX)/dtxn/dtxndistributor_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/serialization_test: $(PREFIX)/serialization_test.o $(PREFIX)/serialization.o $(PREFIX)/io/buffer.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/net/messageconnectionutil.o: net/messageconnectionutil.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/transactionstate_test: $(PREFIX)/dtxn/transactionstate_test.o $(PREFIX)/dtxn/transactionstate.o $(PREFIX)/io/buffer.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn/common.o: dtxn/common.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/locks/lockscheduler_test: $(PREFIX)/dtxn/locks/lockscheduler_test.o $(PREFIX)/dtxn/locks/lockscheduler.o $(PREFIX)/dtxn/locks/lock.o $(PREFIX)/dtxn/locks/locktransaction.o $(PREFIX)/io/buffer.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/replication/primarybackup_test.o: replication/primarybackup_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/locks/lockscheduler.o: dtxn/locks/lockscheduler.cc
	$(CC_COMPILE)
$(PREFIX)/base/stringutil_test: $(PREFIX)/base/stringutil_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/randomgenerator.o: randomgenerator.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/distributedtransaction_test: $(PREFIX)/dtxn/distributedtransaction_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn/ordered/orderedclient_test: $(PREFIX)/dtxn/ordered/orderedclient_test.o $(PREFIX)/dtxn/ordered/orderedclient.o $(PREFIX)/buffer.o $(PREFIX)/messageconnection.o $(PREFIX)/tcplistener.o $(PREFIX)/serialization.o $(PREFIX)/networkaddress.o $(PREFIX)/io/buffer.o $(PREFIX)/strings/utils.o $(PREFIX)/libevent/libevent_core.a $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/strings/json.o: strings/json.cc
	$(CC_COMPILE)
$(PREFIX)/strings/utils.o: strings/utils.cc
	$(CC_COMPILE)
$(PREFIX)/io/memoryinputstream_test.o: io/memoryinputstream_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/ordered/ordereddistributor_test: $(PREFIX)/dtxn/ordered/ordereddistributor_test.o $(PREFIX)/dtxn/libdtxn.a $(PREFIX)/buffer.o $(PREFIX)/messageconnection.o $(PREFIX)/tcplistener.o $(PREFIX)/serialization.o $(PREFIX)/networkaddress.o $(PREFIX)/io/buffer.o $(PREFIX)/strings/utils.o $(PREFIX)/libevent/libevent_core.a $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn/locks/lock_test: $(PREFIX)/dtxn/locks/lock_test.o $(PREFIX)/dtxn/locks/lock.o $(PREFIX)/dtxn/locks/locktransaction.o $(PREFIX)/io/buffer.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/replication/nulllog.o: replication/nulllog.cc
	$(CC_COMPILE)
$(PREFIX)/messageconnection_test: $(PREFIX)/messageconnection_test.o $(PREFIX)/buffer.o $(PREFIX)/messageconnection.o $(PREFIX)/networkaddress.o $(PREFIX)/tcplistener.o $(PREFIX)/io/buffer.o $(PREFIX)/strings/utils.o $(PREFIX)/stupidunit/stupidunit.o $(PREFIX)/libevent/libevent_core.a
	$(CC_BINARY)
$(PREFIX)/dtxn/transaction_test: $(PREFIX)/dtxn/transaction_test.o $(PREFIX)/io/buffer.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn2/common.o: dtxn2/common.cc
	$(CC_COMPILE)
$(PREFIX)/simpleclient.o: simpleclient.cc
	$(CC_COMPILE)
$(PREFIX)/protodtxn/protodtxncoordinator.o: protodtxn/protodtxncoordinator.cc
	$(CC_COMPILE)
$(PREFIX)/tcplistener_test.o: tcplistener_test.cc
	$(CC_COMPILE)
$(PREFIX)/strings/utils_test: $(PREFIX)/strings/utils_test.o $(PREFIX)/strings/utils.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn/ordered/ordereddtxnmanager_test.o: dtxn/ordered/ordereddtxnmanager_test.cc
	$(CC_COMPILE)
$(PREFIX)/protodtxn/protodtxnengine_test.o: protodtxn/protodtxnengine_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/dtxnserver_test: $(PREFIX)/dtxn/dtxnserver_test.o $(PREFIX)/dtxn/dtxnserver.o $(PREFIX)/dtxn/transactionstate.o $(PREFIX)/io/buffer.o $(PREFIX)/net/messageserver.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/serialization_test.o: serialization_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn2/blockingscheduler_test: $(PREFIX)/dtxn2/blockingscheduler_test.o $(PREFIX)/dtxn2/blockingscheduler.o $(PREFIX)/io/buffer.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn/occ/occtransaction_test.o: dtxn/occ/occtransaction_test.cc
	$(CC_COMPILE)
$(PREFIX)/protodtxn/protodtxncoordinator_test: $(PREFIX)/protodtxn/protodtxncoordinator_test.o $(PREFIX)/protodtxn/protodtxncoordinator.o $(PREFIX)/$(DTXN_PROTO_NAME).pb.o $(PREFIX)/dtxn/libdtxn.a $(PREFIX)/libevent/libevent_core.a $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn/transactionstate_test.o: dtxn/transactionstate_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/ordered/ordereddtxnmanager_test: $(PREFIX)/dtxn/ordered/ordereddtxnmanager_test.o $(PREFIX)/dtxn/ordered/ordereddtxnmanager.o $(PREFIX)/dtxn/libdtxn.a $(PREFIX)/buffer.o $(PREFIX)/messageconnection.o $(PREFIX)/tcplistener.o $(PREFIX)/serialization.o $(PREFIX)/networkaddress.o $(PREFIX)/io/buffer.o $(PREFIX)/strings/utils.o $(PREFIX)/libevent/libevent_core.a $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/networkaddress_test.o: networkaddress_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/locks/lock.o: dtxn/locks/lock.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/locks/lockscheduler_test.o: dtxn/locks/lockscheduler_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn2/partitionserver_test: $(PREFIX)/dtxn2/partitionserver_test.o $(PREFIX)/dtxn2/partitionserver.o $(PREFIX)/io/buffer.o $(PREFIX)/net/messageserver.o $(PREFIX)/replication/nulllog.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn2/protopartitionserver_test: $(PREFIX)/dtxn2/protopartitionserver_test.o $(PREFIX)/dtxn2/partitionserver.o $(PREFIX)/dtxn2/protopartitionserver.o $(PREFIX)/io/buffer.o $(PREFIX)/net/messageserver.o $(PREFIX)/replication/nulllog.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/coordinatorworker.o: coordinatorworker.cc
	$(CC_COMPILE)
$(PREFIX)/net/messageserver.o: net/messageserver.cc
	$(CC_COMPILE)
$(PREFIX)/stupidunit/stupidunit_test.o: stupidunit/stupidunit_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/occ/occtransaction_test: $(PREFIX)/dtxn/occ/occtransaction_test.o $(PREFIX)/dtxn/locks/lock.o $(PREFIX)/io/buffer.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/buffer_test.o: buffer_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/ordered/orderedscheduler_test: $(PREFIX)/dtxn/ordered/orderedscheduler_test.o $(PREFIX)/dtxn/ordered/orderedscheduler.o $(PREFIX)/dtxn/transactionstate.o $(PREFIX)/io/buffer.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/replication/primarybackuplistener_test: $(PREFIX)/replication/primarybackuplistener_test.o $(PREFIX)/replication/primarybackup.o $(PREFIX)/replication/primarybackuplistener.o $(PREFIX)/io/buffer.o $(PREFIX)/net/messageserver.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn/ordered/ordereddistributor.o: dtxn/ordered/ordereddistributor.cc
	$(CC_COMPILE)
$(PREFIX)/net/messageserver_demo.o: net/messageserver_demo.cc
	$(CC_COMPILE)
$(PREFIX)/tcplistener_test: $(PREFIX)/tcplistener_test.o $(PREFIX)/buffer.o $(PREFIX)/messageconnection.o $(PREFIX)/tcplistener.o $(PREFIX)/base/function_thread.o $(PREFIX)/io/buffer.o $(PREFIX)/io/libeventloop.o $(PREFIX)/libevent/libevent_core.a $(PREFIX)/strings/utils.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn/ordered/orderedclient_test.o: dtxn/ordered/orderedclient_test.cc
	$(CC_COMPILE)
$(PREFIX)/io/libeventloop_test: $(PREFIX)/io/libeventloop_test.o $(PREFIX)/io/libeventloop.o $(PREFIX)/libevent/libevent_core.a $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/tcplistener.o: tcplistener.cc
	$(CC_COMPILE)
$(PREFIX)/networkaddress_test: $(PREFIX)/networkaddress_test.o $(PREFIX)/networkaddress.o $(PREFIX)/strings/utils.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn/ordered/ordereddistributor_test.o: dtxn/ordered/ordereddistributor_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/transactionstate.o: dtxn/transactionstate.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/locks/lock_test.o: dtxn/locks/lock_test.cc
	$(CC_COMPILE)
$(PREFIX)/io/buffer_test.o: io/buffer_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/occ/occscheduler_test: $(PREFIX)/dtxn/occ/occscheduler_test.o $(PREFIX)/dtxn/occ/occscheduler.o $(PREFIX)/dtxn/locks/lock.o $(PREFIX)/io/buffer.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/protodtxn/protodtxncoordinator_main.o: protodtxn/protodtxncoordinator_main.cc
	$(CC_COMPILE)
$(PREFIX)/net/messageserver_demo: $(PREFIX)/net/messageserver_demo.o $(PREFIX)/net/messageserver.o $(PREFIX)/buffer.o $(PREFIX)/io/buffer.o $(PREFIX)/io/libeventloop.o $(PREFIX)/libevent/libevent_core.a $(PREFIX)/messageconnection.o $(PREFIX)/networkaddress.o $(PREFIX)/serialization.o $(PREFIX)/strings/utils.o $(PREFIX)/tcplistener.o
	$(CC_BINARY)
$(PREFIX)/replication/primarybackuplistener_test.o: replication/primarybackuplistener_test.cc
	$(CC_COMPILE)
$(PREFIX)/replication/nulllog_test: $(PREFIX)/replication/nulllog_test.o $(PREFIX)/replication/nulllog.o $(PREFIX)/io/buffer.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn/distributedtransaction_test.o: dtxn/distributedtransaction_test.cc
	$(CC_COMPILE)
$(PREFIX)/net/messageserver_test: $(PREFIX)/net/messageserver_test.o $(PREFIX)/net/messageserver.o $(PREFIX)/io/buffer.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/protodtxn/protodtxncoordinator: $(PREFIX)/protodtxn/protodtxncoordinator_main.o $(PREFIX)/$(DTXN_PROTO_NAME).pb.o $(PREFIX)/dtxn/ordered/ordereddtxnmanager.o $(PREFIX)/protodtxn/protodtxncoordinator.o $(PREFIX)/buffer.o $(PREFIX)/dtxn/libdtxn.a $(PREFIX)/io/buffer.o $(PREFIX)/libevent/libevent_core.a $(PREFIX)/messageconnection.o $(PREFIX)/net/messageconnectionutil.o $(PREFIX)/networkaddress.o $(PREFIX)/protorpc/Protocol.pb.o $(PREFIX)/protorpc/protoconnectionbuffer.o $(PREFIX)/protorpc/protorpccontroller.o $(PREFIX)/protorpc/protoserver.o $(PREFIX)/protorpc/serviceregistry.o $(PREFIX)/serialization.o $(PREFIX)/strings/utils.o $(PREFIX)/tcplistener.o
	$(CC_BINARY)
$(PREFIX)/buffer_test: $(PREFIX)/buffer_test.o $(PREFIX)/buffer.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/protodtxn/protodtxnengine_test: $(PREFIX)/protodtxn/protodtxnengine_test.o $(PREFIX)/$(DTXN_PROTO_NAME).pb.o $(PREFIX)/protodtxn/protodtxnengine.o $(PREFIX)/dtxn/libdtxn.a $(PREFIX)/io/buffer.o $(PREFIX)/libevent/libevent_core.a $(PREFIX)/protorpc/protorpccontroller.o $(PREFIX)/serialization.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/replication/replicationbench.o: replication/replicationbench.cc
	$(CC_COMPILE)
$(PREFIX)/replication/cachedslidingarray_test.o: replication/cachedslidingarray_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn2/blockingscheduler.o: dtxn2/blockingscheduler.cc
	$(CC_COMPILE)
$(PREFIX)/logging/logbench: $(PREFIX)/logging/logbench.o
	$(CC_BINARY)
$(PREFIX)/dtxn/configparser.o: dtxn/configparser.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/transaction_test.o: dtxn/transaction_test.cc
	$(CC_COMPILE)
$(PREFIX)/logging/logfilecrash: $(PREFIX)/logging/logfilecrash.o $(PREFIX)/logging/crc32c.o $(PREFIX)/logging/crc32ctables.o $(PREFIX)/logging/logfile.o $(PREFIX)/logging/sequencefile.o $(PREFIX)/io/buffer.o $(PREFIX)/networkaddress.o $(PREFIX)/strings/utils.o
	$(CC_BINARY)
$(PREFIX)/strings/json_test: $(PREFIX)/strings/json_test.o $(PREFIX)/strings/json.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/io/buffer_test: $(PREFIX)/io/buffer_test.o $(PREFIX)/io/buffer.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/dtxn2/partitionserver.o: dtxn2/partitionserver.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn2/protopartitionserver.o: dtxn2/protopartitionserver.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/dtxnclient.o: dtxn/dtxnclient.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/dtxndistributor_test.o: dtxn/dtxndistributor_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/configparser_test.o: dtxn/configparser_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn2/blockingscheduler_test.o: dtxn2/blockingscheduler_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/configparser_test: $(PREFIX)/dtxn/configparser_test.o $(PREFIX)/dtxn/configparser.o $(PREFIX)/networkaddress.o $(PREFIX)/strings/utils.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/strings/json_test.o: strings/json_test.cc
	$(CC_COMPILE)
$(PREFIX)/dtxn/ordered/ordereddtxnmanager.o: dtxn/ordered/ordereddtxnmanager.cc
	$(CC_COMPILE)
$(PREFIX)/protodtxn/protodtxnengine: $(PREFIX)/protodtxn/protodtxnengine_main.o $(PREFIX)/$(DTXN_PROTO_NAME).pb.o $(PREFIX)/protodtxn/protodtxnengine.o $(PREFIX)/buffer.o $(PREFIX)/dtxn/configparser.o $(PREFIX)/dtxn2/blockingscheduler.o $(PREFIX)/dtxn2/common.o $(PREFIX)/dtxn2/partitionserver.o $(PREFIX)/dtxn2/protopartitionserver.o $(PREFIX)/io/buffer.o $(PREFIX)/io/libeventloop.o $(PREFIX)/libevent/libevent_core.a $(PREFIX)/messageconnection.o $(PREFIX)/net/messageconnectionutil.o $(PREFIX)/net/messageserver.o $(PREFIX)/networkaddress.o $(PREFIX)/protorpc/Protocol.pb.o $(PREFIX)/protorpc/protoconnectionbuffer.o $(PREFIX)/protorpc/protorpccontroller.o $(PREFIX)/protorpc/protorpcchannel.o $(PREFIX)/protorpc/serviceregistry.o $(PREFIX)/protorpc/protoserver.o $(PREFIX)/replication/primarybackup.o $(PREFIX)/replication/primarybackuplistener.o $(PREFIX)/replication/nulllog.o $(PREFIX)/serialization.o $(PREFIX)/strings/utils.o $(PREFIX)/tcplistener.o
	$(CC_BINARY)
$(PREFIX)/networkaddress.o: networkaddress.cc
	$(CC_COMPILE)
$(PREFIX)/replication/cachedslidingarray_test: $(PREFIX)/replication/cachedslidingarray_test.o $(PREFIX)/stupidunit/stupidunit.o
	$(CC_BINARY)
$(PREFIX)/base/time_test.o: base/time_test.cc
	$(CC_COMPILE)
$(PREFIX)/serialization.o: serialization.cc
	$(CC_COMPILE)