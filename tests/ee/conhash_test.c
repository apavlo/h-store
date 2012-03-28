/* This test is for consistent hashing
 * yang
 */
#include <stdio.h>
#include <stdlib.h>

#include "../../third_party/cpp/libconhash/conhash.h"

struct node_s g_nodes[64];
int main()
{
    int i;
    const struct node_s *node;
    char str[128];
    long hashes[512];
	long map[1000];
	int mapIndex = 0;
	int a[20];
    /* init conhash instance */
    struct conhash_s *conhash = conhash_init(NULL);
    if(conhash)
    {
        /*add original partitions*/
		for(i = 0; i < 10; i ++){
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
		/*long goodhash = conhash_getMigrationHash(map, mapIndex, 7, 0);
		printf("Good migration hash for partition %d is %u\n", 7, goodhash);
		conhash_mapPartionToHash(map, mapIndex ++, goodhash);
		printf("All hashes for partitions from 0 to %d\n", mapIndex-1);
		for(i = 0; i < mapIndex; i ++){
			printf("partition %d\t hash: %u\n", i, map[i]);
		}*/
		for(i=0; i<10; i++){
			sprintf(str, "%d", i);
			long hash = __conhash_getHash(str);
			node = conhash_lookup(conhash, str);
			a[atoi(node->iden)] ++;
		}
		for(i=0; i<10; i++){
			printf("node %d number %d\n", i, a[i]);
		}
    }
    conhash_fini(conhash);
    return 0;
}
