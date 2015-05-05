import os

## ==============================================
## GRAPH OPTIONS
## ==============================================

# Font Options
#OPT_FONT_FAMILY = 'serif'
OPT_FONT_NAME = 'Times New Roman'
OPT_FONT_BASE_SIZE = 18

# LEGEND
OPT_LEGEND_FONT_SIZE = OPT_FONT_BASE_SIZE-4
OPT_LEGEND_SHADOW = True

OPT_XLABEL_FONT_SIZE = OPT_FONT_BASE_SIZE+6
OPT_XTICKS_FONT_SIZE = OPT_FONT_BASE_SIZE+2

OPT_YLABEL_FONT_SIZE = OPT_FONT_BASE_SIZE+6
OPT_YTICKS_FONT_SIZE = OPT_FONT_BASE_SIZE+2

# Output Dimensions
OPT_GRAPH_WIDTH = 800
OPT_GRAPH_HEIGHT = 400
OPT_GRAPH_DPI = 200

# GRAPH OPTIONS
OPT_COLORS = ( '#DC3912', '#3366CC', '#5F04B4', '#A1A1A1', '#8D38C9', '##FFFF00')
OPT_PATTERNS = ('//', '\\\\', 'X', '', '+')
OPT_MARKERS = ('s', 'd', 'p', 'o', 'v')

OPT_ERROR_COLOR = "red"
OPT_ERROR_WIDTH = 2.0
OPT_ERROR_CAPSIZE = 5.0

OPT_BENCHMARK_LABEL_XOFFSET = 0.13
OPT_BENCHMARK_LABEL_XSPACER = 0.34
OPT_BENCHMARK_LABEL_YOFFSET = -0.1

# Labels
OPT_BENCHMARK_LABELS = {
    "tm1":      "TATP",
    "tpcc":     "TPC-C",
    "seats":    "SEATS",
    "wiki":     "Wikipedia"
}

# Grid Background
OPT_GRID_LINESTYLE = '-'
OPT_GRID_WHICH = 'major'
OPT_GRID_COLOR = '0.85'

## ==============================================
## LABELS
## ==============================================
OPT_Y_LABEL_THROUGHPUT = 'Transactions per Second'
OPT_Y_LABEL_MEMORY = 'Memory (MB)'
OPT_Y_LABEL_TIME_MS = 'Elapsed Time (ms)'

OPT_X_LABEL_TIME = 'Elapsed Time (seconds)'
OPT_X_LABEL_READ_RATIO = 'Read Ratio'
OPT_X_LABEL_WORKLOAD_SKEW = 'Workload Skew'
OPT_X_LABEL_DATA_SIZE = 'Data Size (data/memory)'
OPT_X_LABEL_TUPLE_SIZE = 'Tuple Size (bytes)'
OPT_X_LABEL_BLOCK_SIZE = 'Eviction Block Size (KB)'
OPT_X_LABEL_BLOCK_SIZE_MB = 'Eviction Block Size (MB)'
OPT_X_LABEL_NUM_INDEXES = 'Number of Indexes'

OPT_LABEL_MYSQL = "MySQL"
OPT_LABEL_HSTORE = "anti-cache\n  (LRU)"
OPT_LABEL_HSTORE_APPROX = "anti-cache\n  (aLRU)"
OPT_LABEL_NO_ANTICACHE = "H-Store"
OPT_LABEL_MEMCACHED = OPT_LABEL_MYSQL + "\n+Memcached"

## ==============================================
## DATA DIRECTORIES
## ==============================================
baseDir = os.path.dirname(__file__)
OPT_DATA_MYSQL = os.path.realpath(os.path.join(baseDir, "../data/mysql"))
OPT_DATA_HSTORE = os.path.realpath(os.path.join(baseDir, "../data/hstore"))
OPT_DATA_HSTORE_ANTICACHE = os.path.realpath(os.path.join(baseDir, "../data/hstore-anticache"))
OPT_DATA_HSTORE_APPROX = os.path.realpath(os.path.join(baseDir, "../data/hstore-approx"))
OPT_DATA_EVICTIONS = os.path.realpath(os.path.join(baseDir, "../data/evictions"))
OPT_DATA_MYSQL_TPCC = os.path.realpath(os.path.join(baseDir, "../data/tpcc/mysql"))
OPT_DATA_MEMCACHED_TPCC = os.path.realpath(os.path.join(baseDir, "../data/tpcc/memcached"))
OPT_DATA_HSTORE_TPCC = os.path.realpath(os.path.join(baseDir, "../data/tpcc/hstore"))
OPT_DATA_HSTORE_TPCC_APPROX = os.path.realpath(os.path.join(baseDir, "../data/tpcc/hstore-approx"))

OPT_DATA_BLOCKSIZE_64 = os.path.realpath(os.path.join(baseDir, "../data/blocksize/64"))
OPT_DATA_BLOCKSIZE_128 = os.path.realpath(os.path.join(baseDir, "../data/blocksize/128"))
OPT_DATA_BLOCKSIZE_256 = os.path.realpath(os.path.join(baseDir, "../data/blocksize/256"))
OPT_DATA_BLOCKSIZE_512 = os.path.realpath(os.path.join(baseDir, "../data/blocksize/512"))
OPT_DATA_BLOCKSIZE_1024 = os.path.realpath(os.path.join(baseDir, "../data/blocksize/1024"))
OPT_DATA_BLOCKSIZE_2048 = os.path.realpath(os.path.join(baseDir, "../data/blocksize/2048"))
OPT_DATA_BLOCKSIZE_4096 = os.path.realpath(os.path.join(baseDir, "../data/blocksize/4096"))

OPT_DATA_INDEX_TREE = os.path.realpath(os.path.join(baseDir, "../data/index/tree/results.csv"))
OPT_DATA_INDEX_HASH = os.path.realpath(os.path.join(baseDir, "../data/index/hash/results.csv"))

OPT_DATA_TUPLESIZE_128 = os.path.realpath(os.path.join(baseDir, "../data/tuplesize/128"))
OPT_DATA_TUPLESIZE_256 = os.path.realpath(os.path.join(baseDir, "../data/tuplesize/256"))
OPT_DATA_TUPLESIZE_512 = os.path.realpath(os.path.join(baseDir, "../data/tuplesize/512"))
OPT_DATA_TUPLESIZE_1024 = os.path.realpath(os.path.join(baseDir, "../data/tuplesize/1024"))

OPT_DATA_MERGE_BLOCK = os.path.realpath(os.path.join(baseDir, "../data/merge/block"))
OPT_DATA_MERGE_TUPLE = os.path.realpath(os.path.join(baseDir, "../data/merge/tuple"))

OPT_DATA_LRU_NONE = os.path.realpath(os.path.join(baseDir, "../data/lru/none/results.csv"))
OPT_DATA_LRU_SINGLE = os.path.realpath(os.path.join(baseDir, "../data/lru/single/results.csv"))
OPT_DATA_LRU_DOUBLE = os.path.realpath(os.path.join(baseDir, "../data/lru/double/results.csv"))

OPT_DATA_EVICT_CONSTRUCT = os.path.realpath(os.path.join(baseDir, "../data/evict/construct/results.csv"))
OPT_DATA_EVICT_WRITE = os.path.realpath(os.path.join(baseDir, "../data/evict/write/results.csv"))
OPT_DATA_EVICT_FETCH = os.path.realpath(os.path.join(baseDir, "../data/evict/fetch/results.csv"))
OPT_DATA_EVICT_BLOCK_MERGE = os.path.realpath(os.path.join(baseDir, "../data/evict/block-merge/results.csv"))
OPT_DATA_EVICT_TUPLE_MERGE = os.path.realpath(os.path.join(baseDir, "../data/evict/tuple-merge/results.csv"))













