
/* Copyright (C) 2010. sparkling.liang@hotmail.com. All rights reserved. */

#include "conhash.h"
#include "conhash_inter.h"

void conhash_md5_digest(const u_char *instr, u_char digest[16])
{
    md5_state_t md5state;

    md5_init(&md5state);
    md5_append(&md5state, instr, strlen(instr));
    md5_finish(&md5state, digest);
}

static void __get_vnodes(util_rbtree_node_t *node, void *data)
{
    struct __get_vnodes_s *vnodes = (struct __get_vnodes_s *)data;
    if(vnodes->cur < vnodes->size)
    {
        vnodes->values[vnodes->cur++] = node->key;
    }
}
void conhash_get_vnodes(const struct conhash_s *conhash, long *values, int size)
{
    struct __get_vnodes_s vnodes;
    if((conhash==NULL) || (values==NULL) || (size<=0))
    {
        return;
    }
    vnodes.values = values;
    vnodes.size = size;
    vnodes.cur = 0;
    util_rbtree_mid_travel(&(conhash->vnode_tree), __get_vnodes, &vnodes);
}

u_int conhash_get_vnodes_num(const struct conhash_s *conhash)
{
    if(conhash == NULL)
    {
        return 0;
    }
    return conhash->ivnodes;
}

void conhash_mapPartionToHash(long *map, int partition_num, long hash)
{
	map[partition_num] = hash;
}

long conhash_getMigrationHash(long *map, int map_len, int partition_num, int factor)
{
	/*factor is used for fine grained hash chosen: unused now*/
	char str[100]; /*the magic number 100 is for temperary usage*/
	int i;
	
	long *alias_map = malloc(map_len * sizeof(long));
	memcpy(alias_map, map, map_len*sizeof(long));
	for(i = 0; i < map_len; i ++){
		printf("before: partition %d\t hash: %u\n", i, alias_map[i]);
	}
	__conhash_quicksort(alias_map, map_len);
	puts("alias sorted hashes________________");
	for(i = 0; i < map_len; i ++){
		printf("partition %d\t hash: %ld\n", i, alias_map[i]);
	}
	
	long partition_hash = map[partition_num];
	printf("The hash for partition %d is %ld\n", partition_num, partition_hash);
	for(i = 0; i < map_len; ++i){
		if(alias_map[i] == partition_hash){
			break;
		}
	}
	if(i == map_len){
		/*didn't find the hash*/
		printf("conhash_getMigrationHash: Cannot find partition %d's hash in the hash map\n", partition_num);
		free(alias_map);
		return -1;
	}
	if(i == 0){/*if the hash is the smallest one in the map*/
		free(alias_map);
		return partition_hash/2;
	}else{
		long pre_hash = alias_map[i - 1];
		free(alias_map);
		return (partition_hash + pre_hash)/2;
	}
}

