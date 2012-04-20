#include "conhash.h"

Conhash::Conhash(unsigned int n)
{
    conhash = conhash_init(NULL, n, g_nodes, node_to_partition);
}

Conhash::Conhash(unsigned int n, unsigned int partition_num, unsigned int *map){
    int i = 0;
	while(map[i] != 0){
		node_to_partition[i] = map[i];
		i ++;
	}
	int num_partitions = i;
	unsigned int new_hash = conhash_getMigrationHash(map, num_partitions, partition_num, 2);
	node_to_partition[i] = new_hash;
	for(i=0; i<num_partitions + 1; i++){
		conhash_set_node(&g_nodes[i], node_to_partition[i], 1);
		conhash_add_node(conhash, &g_nodes[i]);
	}
}
Conhash::~Conhash()
{
    if(conhash != NULL)
    {
        conhash_fini(conhash);
    }
}

const struct node_s *Conhash::lookup(const unsigned int object)
{
    return conhash_lookup(conhash, object);
}

unsigned int *Conhash::get_map(){
	return node_to_partition;
}
