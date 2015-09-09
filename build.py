#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, commands, string, glob
from buildtools import *

# usage:
# The following all work as you might expect:
# python build.py
# ./build.py debug
# python build.py release
# python build.py test
# ./build.py clean
# ./build.py release clean
# python build.py release test

# The command line args can include a build level: release or debug
#  the default is debug
# The command line args can include an action: build, clean or test
#  the default is build
# The order doesn't matter
# Including multiple levels or actions is a bad idea

###############################################################################
# INITIALIZE BUILD CONTEXT
#  - Detect Platform
#  - Parse Target and Level from Command Line
###############################################################################

CTX = BuildContext(sys.argv)

# CTX is an instance of BuildContext, which is declared in buildtools.py
# BuildContext contains vars that determine how the makefile will be built
#  and how the build will go down. It also checks the platform and parses
#  command line args to determine target and build level.

###############################################################################
# SET RELEASE LEVEL CONTEXT
###############################################################################

volt_log_level = 500

if CTX.LEVEL == "MEMCHECK":
    CTX.EXTRAFLAGS += " -g3 -rdynamic -DDEBUG -DMEMCHECK"
    CTX.OUTPUT_PREFIX = "obj/memcheck"
    volt_log_level = 500

if CTX.LEVEL == "MEMCHECK_NOFREELIST":
    CTX.EXTRAFLAGS += " -g3 -rdynamic -DDEBUG -DMEMCHECK -DMEMCHECK_NOFREELIST"
    CTX.OUTPUT_PREFIX = "obj/memcheck_nofreelist"
    volt_log_level = 500

if CTX.LEVEL == "DEBUG":
    CTX.EXTRAFLAGS += " -g3 -rdynamic -DDEBUG"
    CTX.OUTPUT_PREFIX = "obj/release"
    volt_log_level = 200

if CTX.LEVEL == "RELEASE":
    CTX.EXTRAFLAGS += " -g3 -O3 -mmmx -msse -msse2 -msse3 -DNDEBUG" #  -ffast-math -funroll-loops"
    CTX.OUTPUT_PREFIX = "obj/release"
    volt_log_level = 500

# build in parallel directory instead of subdir so that relative paths work
if CTX.COVERAGE:
    CTX.EXTRAFLAGS += " -ftest-coverage -fprofile-arcs"
    CTX.OUTPUT_PREFIX += "-coverage"

# Override the default log level if they gave us one
if CTX.VOLT_LOG_LEVEL != None: volt_log_level = CTX.VOLT_LOG_LEVEL
CTX.EXTRAFLAGS += " -DVOLT_LOG_LEVEL=%d" % volt_log_level
CTX.TEST_EXTRAFLAGS += " -DVOLT_LOG_LEVEL=%d" % volt_log_level

CTX.OUTPUT_PREFIX += "/"

###############################################################################
# SET GLOBAL CONTEXT VARIABLES FOR BUILDING
###############################################################################

# these are the base compile options that get added to every compile step
# this does not include header/lib search paths or specific flags for
#  specific targets
CTX.CPPFLAGS = """-Wall -Wextra -Werror -Woverloaded-virtual -Wconversion
            -Wpointer-arith -Wcast-qual -Wcast-align -Wwrite-strings
            -Winit-self -Wno-sign-compare -Wno-unused-parameter
            -pthread
            -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -DNOCLOCK
            -fno-omit-frame-pointer
            -fvisibility=hidden -DBOOST_SP_DISABLE_THREADS"""

if (gcc_major == 4 and gcc_minor >= 3) or (gcc_major == 5):
    CTX.CPPFLAGS += " -Wno-ignored-qualifiers -fno-strict-aliasing"

# linker flags
CTX.LDFLAGS = """-g3 -ldl"""
# Done by default on Darwin -- unrecognized option for the linker on Darwin
if CTX.PLATFORM == "Linux":
    CTX.LDFLAGS +=" -rdynamic"
if CTX.COVERAGE:
    CTX.LDFLAGS += " -ftest-coverage -fprofile-arcs"
# for the google perftools profiler and the recommended stack unwinder
#CTX.LDFLAGS = """ -g3 -rdynamic -lprofiler -lunwind"""

# this is where the build will look for header files
# - the test source will also automatically look in the test root dir
CTX.INCLUDE_DIRS = ['src/ee']
CTX.SYSTEM_DIRS = [
    'third_party/cpp',
]

# extra flags that will get added to building test source
if CTX.LEVEL == "MEMCHECK":
    CTX.TEST_EXTRAFLAGS += """ -g3 -DDEBUG -DMEMCHECK"""
elif CTX.LEVEL == "MEMCHECK_NOFREELIST":
    CTX.TEST_EXTRAFLAGS += """ -g3 -DDEBUG -DMEMCHECK -DMEMCHECK_NOFREELIST"""
else:
    CTX.TEST_EXTRAFLAGS += """ -g3 -DDEBUG """

# don't worry about checking for changes in header files in the following
#  directories
CTX.IGNORE_SYS_PREFIXES = ['/usr/include', '/usr/lib', 'third_party']

# where to find the source
CTX.INPUT_PREFIX = "src/ee"

# where to find the source
CTX.THIRD_PARTY_INPUT_PREFIX = "third_party/cpp/"

# Third-Party Static Libraries
CTX.THIRD_PARTY_STATIC_LIBS = [ ]

# where to find the tests
CTX.TEST_PREFIX = "tests/ee"

###############################################################################
# HANDLE PLATFORM SPECIFIC STUFF
###############################################################################

# Defaults Section
CTX.JNIEXT = "so"
CTX.JNILIBFLAGS += " -shared"
CTX.SOFLAGS += " -shared"
CTX.SOEXT = "so"
out = Popen('java -cp tools/ SystemPropertyPrinter java.library.path'.split(),
            stdout = PIPE).communicate()[0]
libpaths = ' '.join( '-L' + path for path in out.strip().split(':') if path != '' and path != '/usr/lib' )
CTX.JNIBINFLAGS += " " + libpaths
CTX.JNIBINFLAGS += " -ljava -ljvm -lverify"

if CTX.PLATFORM == "Darwin":
    CTX.CPPFLAGS += " -DMACOSX "
    
    # 2012-02-10
    # Don't include the 'arch' flag for newer versions of OSX
    if int(CTX.PLATFORM_VERSION.split(".")[0]) < 11:
        CTX.CPPFLAGS += " -arch x86_64"
    
    # 2012-12-18
    # Disable sign conversion warnings for Mountain Lion
    if int(CTX.PLATFORM_VERSION.split(".")[0]) >= 12:
        CTX.CPPFLAGS += " -Wno-sign-conversion"
    
    CTX.JNIEXT = "jnilib"
    CTX.JNILIBFLAGS = " -bundle"
    CTX.JNIBINFLAGS = " -framework JavaVM,1.6"
    CTX.SOFLAGS += "-dynamiclib -undefined dynamic_lookup -single_module"
    CTX.SOEXT = "dylib"
    CTX.JNIFLAGS = "-framework JavaVM,1.6"

if CTX.PLATFORM == "Linux":
    CTX.CPPFLAGS += " -Wno-attributes -DLINUX -fPIC -Wno-unused-but-set-variable"
    CTX.NMFLAGS += " --demangle"

###############################################################################
# SPECIFY SOURCE FILE INPUT
###############################################################################

# the input is a map from directory name to a list of whitespace
# separated source files (cpp only for now).  Preferred ordering is
# one file per line, indented one space, in alphabetical order.

CTX.INPUT[''] = """
 voltdbjni.cpp
"""

# 2012-03-14
# Automatically grab all of our catalog files
catalog_files = [ ]
for f in glob.glob(os.path.join(CTX.INPUT_PREFIX, 'catalog', '*.cpp')):
    catalog_files.append(os.path.basename(f))
CTX.INPUT['catalog'] = "\n".join(sorted(catalog_files))

CTX.INPUT['common'] = """
 SegvException.cpp
 SerializableEEException.cpp
 SQLException.cpp
 tabletuple.cpp
 TupleSchema.cpp
 types.cpp
 UndoLog.cpp
 NValue.cpp
 MMAPMemoryManager.cpp
 RecoveryProtoMessage.cpp
 RecoveryProtoMessageBuilder.cpp
 DefaultTupleSerializer.cpp
 StringRef.cpp
"""

CTX.INPUT['execution'] = """
 JNITopend.cpp
 VoltDBEngine.cpp
"""

CTX.INPUT['executors'] = """
 abstractexecutor.cpp
 deleteexecutor.cpp
 distinctexecutor.cpp
 executorutil.cpp
 indexscanexecutor.cpp
 insertexecutor.cpp
 limitexecutor.cpp
 materializeexecutor.cpp
 nestloopexecutor.cpp
 nestloopindexexecutor.cpp
 orderbyexecutor.cpp
 projectionexecutor.cpp
 receiveexecutor.cpp
 sendexecutor.cpp
 seqscanexecutor.cpp
 unionexecutor.cpp
 updateexecutor.cpp
"""

CTX.INPUT['expressions'] = """
 abstractexpression.cpp
 expressionutil.cpp
 tupleaddressexpression.cpp
"""

CTX.INPUT['plannodes'] = """
 abstractjoinnode.cpp
 abstractoperationnode.cpp
 abstractplannode.cpp
 abstractscannode.cpp
 aggregatenode.cpp
 deletenode.cpp
 distinctnode.cpp
 indexscannode.cpp
 insertnode.cpp
 limitnode.cpp
 materializenode.cpp
 nestloopindexnode.cpp
 nestloopnode.cpp
 orderbynode.cpp
 PlanColumn.cpp
 plannodefragment.cpp
 plannodeutil.cpp
 projectionnode.cpp
 receivenode.cpp
 sendnode.cpp
 seqscannode.cpp
 unionnode.cpp
 updatenode.cpp
"""

CTX.INPUT['indexes'] = """
 arrayuniqueindex.cpp
 tableindex.cpp
 tableindexfactory.cpp
 IndexStats.cpp
"""

CTX.INPUT['storage'] = """
 constraintutil.cpp
 CopyOnWriteContext.cpp
 CopyOnWriteIterator.cpp
 ConstraintFailureException.cpp
 MaterializedViewMetadata.cpp
 mmap_persistenttable.cpp
 persistenttable.cpp
 PersistentTableStats.cpp
 PersistentTableUndoDeleteAction.cpp
 PersistentTableUndoInsertAction.cpp
 PersistentTableUndoUpdateAction.cpp
 StreamedTableStats.cpp
 streamedtable.cpp
 table.cpp
 TableCatalogDelegate.cpp
 tablefactory.cpp
 TableStats.cpp
 tableutil.cpp
 temptable.cpp
 TupleStreamWrapper.cpp
 RecoveryContext.cpp
 ReadWriteTracker.cpp
"""

CTX.INPUT['stats'] = """
 StatsAgent.cpp
 StatsSource.cpp
"""

CTX.INPUT['logging'] = """
 JNILogProxy.cpp
 LogManager.cpp
 AriesLogProxy.cpp
 Logrecord.cpp
"""
 
# specify the third party input

CTX.THIRD_PARTY_INPUT['json_spirit'] = """
 json_spirit_reader.cpp
 json_spirit_value.cpp
"""

###############################################################################
# SPECIFY THE TESTS
###############################################################################

# input format similar to source, but the executable name is listed
CTX.TESTS['.'] = """
 harness_test
"""

CTX.TESTS['catalog'] = """
 catalog_test
"""

CTX.TESTS['logging'] = """
 logging_test
"""

CTX.TESTS['common'] = """
 debuglog_test
 serializeio_test
 undolog_test
 valuearray_test
 nvalue_test
 tupleschema_test
 tabletuple_test
"""

CTX.TESTS['execution'] = """
 engine_test
"""

CTX.TESTS['expressions'] = """
 expression_test
"""

CTX.TESTS['indexes'] = """
 index_allocatortracker_test
 index_key_test
 index_multikey_test
 index_scripted_test
 index_test
"""

CTX.TESTS['storage'] = """
 CopyOnWriteTest
 constraint_test
 filter_test
 mmap_persistent_table_test
 persistent_table_log_test
 serialize_test
 StreamedTable_test
 table_and_indexes_test
 table_test
 tabletuple_export_test
 TupleStreamWrapper_test
"""

# these are incomplete and out of date. need to be replaced
# CTX.TESTS['expressions'] = """expserialize_test expression_test"""

###############################################################################
# STORAGE MMAP
###############################################################################

if CTX.STORAGE_MMAP:
    CTX.CPPFLAGS += " -DSTORAGE_MMAP"

###############################################################################
# ARIES
###############################################################################

if CTX.ARIES:
    CTX.CPPFLAGS += " -DARIES"
 
###############################################################################
# ANTI-CACHING
###############################################################################

if CTX.ANTICACHE_BUILD:
    CTX.CPPFLAGS += " -DANTICACHE"

    if CTX.ANTICACHE_NVM:
        CTX.CPPFLAGS += " -DANTICACHE_NVM"

    if CTX.ANTICACHE_REVERSIBLE_LRU:
        CTX.CPPFLAGS += " -DANTICACHE_REVERSIBLE_LRU"
        
    if CTX.ANTICACHE_DRAM:
        CTX.CPPFLAGS += " -DANTICACHE_DRAM"

    if CTX.ANTICACHE_TIMESTAMPS:
        CTX.CPPFLAGS += " -DANTICACHE_TIMESTAMPS"

    if CTX.ANTICACHE_TIMESTAMPS_PRIME:
        CTX.CPPFLAGS += " -DANTICACHE_TIMESTAMPS_PRIME"

    # Bring in berkeleydb library
    CTX.SYSTEM_DIRS.append(os.path.join(CTX.OUTPUT_PREFIX, 'berkeleydb'))
    CTX.THIRD_PARTY_STATIC_LIBS.extend([
        "berkeleydb/libdb.a",     # BerkeleyDB Base Library
        "berkeleydb/libdb_cxx.a", # BerkeleyDB C++ Library
    ])
    
    CTX.INPUT['anticache'] = """
        EvictedTupleAccessException.cpp
        UnknownBlockAccessException.cpp
        FullBackingStoreException.cpp
        AntiCacheStats.cpp
        AntiCacheDB.cpp
        BerkeleyAntiCacheDB.cpp
        NVMAntiCacheDB.cpp
        AntiCacheEvictionManager.cpp
        EvictionIterator.cpp
        EvictedTable.cpp
    """
    
    CTX.TESTS['anticache'] = """
        anticachedb_test
        berkeleydb_test
        anticache_eviction_manager_test
    """

###############################################################################
# BUILD THE MAKEFILE
###############################################################################

#print "TARGET PLATFORM: ", CTX.PLATFORM, "-", CTX.PLATFORM_VERSION
#print "CPPFLAGS: ", CTX.CPPFLAGS
#print sys.stdout.flush()

# this function (in buildtools.py) generates the makefile
# it's currently a bit ugly but it'll get cleaned up soon
buildMakefile(CTX)

###############################################################################
# RUN THE MAKEFILE
###############################################################################
numHardwareThreads = 4

if CTX.PLATFORM == "Darwin":
    numHardwareThreads = 0
    output = commands.getstatusoutput("sysctl hw.ncpu")
    numHardwareThreads = int(string.strip(string.split(output[1])[1]))
elif CTX.PLATFORM == "Linux":
    numHardwareThreads = 0
    for line in open('/proc/cpuinfo').readlines():
        name_value = map(string.strip, string.split(line, ':', 1))
        if len(name_value) != 2:
            continue
        name,value = name_value
        if name == "processor":
            numHardwareThreads = numHardwareThreads + 1
else:
    print "WARNING: Unsupported platform type '%s'" % CTX.PLATFORM
print "Detected %d hardware threads to use during the build" % (numHardwareThreads)

print 
retval = os.system("make --directory=%s -j%d nativelibs/libvoltdb.sym" % (CTX.OUTPUT_PREFIX, numHardwareThreads))
print "Make returned: ", retval
if retval != 0:
    sys.exit(-1)

###############################################################################
# RUN THE TESTS IF ASKED TO
###############################################################################

retval = 0
if CTX.TARGET == "BUILDTEST":
    retval = buildTests(CTX)
elif CTX.TARGET == "TEST":
    retval = runTests(CTX)
elif CTX.TARGET == "VOLTDBIPC":
    retval = buildIPC(CTX)

if retval != 0:
    sys.exit(-1)
    
    
