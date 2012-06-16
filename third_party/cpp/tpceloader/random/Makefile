#
# Makefile to build EGenLoader, EGenValidate and EGen driver support code.
#

# PORTABILITY NOTES
# EGen makes use of "standardized" printf/scanf format specifiers
# and constant declaration macros which are typically defined in
# <inttypes.h> on Un*x-like platforms.  In order to get EGen to
# compile on some platforms, it may be neccessary to set one (or
# both) of -D__STDC_FORMAT_MACROS and -D__STDC_CONSTANT_MACROS.
#
# Defines for HP-UX
# CXX=/opt/aCC/bin/aCC
# CCFLAGS=+DD64 -Aa -O -ext -mt
# LDFLAGS=+DD64 -Aa -O -ext -mt
# AR=ar

SOURCE=$(realpath ..)
PREFIX=$(realpath ..)

# Directory for random files
DRIVER = $(SOURCE)/random
# Directory for JNI includes
JNI = $(SOURCE)/..

# Directory for intermediate files
OBJ = $(PREFIX)/obj
# Directory for library files
LIB = $(PREFIX)/lib

# Platform specific defines
CCFLAGS=-g -ggdb -D__STDC_FORMAT_MACROS -D__STDC_CONSTANT_MACROS \
        -DLINUX -fPIC\
        $(DEBUG_OPTS) \
	-I$(JNI)

VPATH=$(DRIVER):$(OBJ)

.SUFFIXES: .cpp
.cpp.o:
	$(CXX) $(CCFLAGS) $(APPDEFINES) -c $< -o $(OBJ)/$@

# Specify each library as a single target
# with source and include files separates into different variables.

EGenRandomJNI_lib =		libegen_random.so
EGenRandomJNI_src =		Random.cpp \
				EGenRandomJNI.cpp
EGenRandomJNI_obj =		$(EGenRandomJNI_src:.cpp=.o)

# Using pattern rules that were defined earlier.
# All options are specified through the variables.

all:				EGenRandomJNI

EGenRandomJNI:			$(EGenRandomJNI_obj)
	cd $(OBJ); \
	$(CXX) -o $(LIB)/$(EGenRandomJNI_lib) -shared $(EGenRandomJNI_obj)

clean:
	cd $(OBJ); \
	rm -f			$(EGenDriverJNI_obj)

mkinstalldirs:
	mkdir -p $(OBJ) $(LIB)
