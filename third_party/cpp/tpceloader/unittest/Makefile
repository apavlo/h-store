CCFLAGS=-D__STDC_FORMAT_MACROS -D__STDC_CONSTANT_MACROS

unittest: tc_main.o tc_checkcompliant.o
	$(CXX) $(LDFLAGS) -o $@ $^ -lboost_unit_test_framework


