/* This test is for consistent hashing
 * yang
 */
#include <stdio.h>
#include <stdlib.h>

#include "/gpfs/main/home/yang/cs227/h-store/src/ee/hasher/conhash.h"
int main()
{
	Conhash *test = new Conhash(10);
	int i;
	char str[128];
	int a[10];
        /* find hash */
		/*long goodhash = conhash_getMigrationHash(map, mapIndex, 7, 0);
		printf("Good migration hash for partition %d is %u\n", 7, goodhash);
		conhash_mapPartionToHash(map, mapIndex ++, goodhash);
		printf("All hashes for partitions from 0 to %d\n", mapIndex-1);
		for(i = 0; i < mapIndex; i ++){
			printf("partition %d\t hash: %u\n", i, map[i]);
		}*/
		const struct node_s *node;
		for(i=0; i<10; i++){
			sprintf(str, "%d", i);
			node = test->lookup(str);
			printf("return node %16s\n", node->iden);
			a[atoi(node->iden)] ++;
		}
		for(i=0; i<10; i++){
			printf("node %d number %d\n", i, a[i]);
		}
		delete(test);
    return 0;
}
