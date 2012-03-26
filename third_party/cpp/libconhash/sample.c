

#include <stdio.h>
#include <stdlib.h>

#include "conhash.h"

struct node_s g_nodes[64];
int main()
{
    int i;
    const struct node_s *node;
    char str[128];
    long hashes[512];
	long map[1000];
	int mapIndex = 0;
	
    /* init conhash instance */
    struct conhash_s *conhash = conhash_init(NULL);
    if(conhash)
    {
        /*add original partitions*/
		for(i = 0; i < 20; i ++){
			sprintf(str, "%d", i);
			conhash_set_node(&g_nodes[i], str, 1);
			conhash_add_node(conhash, &g_nodes[i]);
			long hash = __conhash_getHash(str);
			conhash_mapPartionToHash(map, i, hash);
			mapIndex ++;
		}
		for(i = 0; i < mapIndex; i ++){
			printf("partition %d\t hash: %u\n", i, map[i]);
		}
			puts("___________________________");
        /* find hash */
		long goodhash = conhash_getMigrationHash(map, mapIndex, 7, 0);
		printf("Good migration hash for partition %d is %u\n", 7, goodhash);
		conhash_mapPartionToHash(map, mapIndex ++, goodhash);
		printf("All hashes for partitions from 0 to %d\n", mapIndex-1);
		for(i = 0; i < mapIndex; i ++){
			printf("partition %d\t hash: %u\n", i, map[i]);
		}
    }
    conhash_fini(conhash);
    return 0;
}
