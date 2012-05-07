#include "../../../third_party/cpp/libconhash/conhash.h"

#define HANLDED_PARTIONS 256 //the number of partitions this library can handle
class Conhash {
private:
    struct conhash_s *conhash;
    struct node_s g_nodes[HANLDED_PARTIONS];
    unsigned int node_to_partition[HANLDED_PARTIONS];
public:
    Conhash(unsigned int n);
    //this is a test constructor which creates a new hasher instance with
    //a new node added between the node with partition_num and its previous
    //one
    Conhash(unsigned int n, unsigned int partition_num, unsigned int *map);
    ~Conhash(void);
    int lookup(const unsigned object);
	unsigned int* get_map();
};

