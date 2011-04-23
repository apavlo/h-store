CXXFLAGS=-g -D__unix -D__STDC_FORMAT_MACROS -D__STDC_CONSTANT_MACROS -I/opt/include/boost-1_36
LDFLAGS=-g -L/opt/lib

EGEN_OBJS= \
    ../obj/FlatFileLoader.o \
    ../obj/error.o \
    ../obj/ReadRowFunctions.o \

unittest: tc_main.o tc_checkcompliant.o tc_securityfile.o
	$(CXX) $(LDFLAGS) -o $@ $^ $(EGEN_OBJS) /opt/lib/libboost_unit_test_framework-xgcc40-mt.a


