
/* Copyright (C) 2010. sparkling.liang@hotmail.com. All rights reserved. */

#include "conhash.h"
#include "conhash_inter.h"

struct conhash_s* conhash_init(conhash_cb_hashfunc pfhash, u_int n, struct node_s *g_nodes, unsigned int *map)
{
    /* alloc memory and set to zero */
    struct conhash_s *conhash = (struct conhash_s*)calloc(1, sizeof(struct conhash_s));
    if(conhash == NULL)
    {
        return NULL;
    }
    do
	{
        /* setup callback functions */
        u_int i;
        u_int step = INT_MAX / n;
        u_int hash = step;
        if(pfhash != NULL)
        {
            conhash->cb_hashfunc = pfhash;
        }
        else
        {
            conhash->cb_hashfunc = __conhash_hash_java_def;
        }
	    util_rbtree_init(&conhash->vnode_tree);

	    for(i = 0; i < n; i ++, hash +=step){
	    	conhash_set_node(&g_nodes[i], hash % INT_MAX, 1);
	    	conhash_add_node(conhash, &g_nodes[i]);
            conhash_mapPartitionToHash(map, i, hash);
	    }
	    return conhash;

	}while(0);

    free(conhash);
    return NULL;
}

void conhash_fini(struct conhash_s *conhash)
{
	if(conhash != NULL)
	{
		/* free rb tree */
        while(!util_rbtree_isempty(&(conhash->vnode_tree)))
        {
            util_rbtree_node_t *rbnode = conhash->vnode_tree.root;
            util_rbtree_delete(&(conhash->vnode_tree), rbnode);
            __conhash_del_rbnode(rbnode);
        }
		free(conhash);
	}
}

void conhash_set_node(struct node_s *node, const u_int iden, u_int replica)
{
    node->iden = iden;
    node->replicas = replica;
    node->flag = NODE_FLAG_INIT;
}

int conhash_add_node(struct conhash_s *conhash, struct node_s *node)
{
    if((conhash==NULL) || (node==NULL))
    {
        return -1;
    }
    /* check node fisrt */
    if(!(node->flag&NODE_FLAG_INIT) || (node->flag&NODE_FLAG_IN))
    {
        return -1;
    }
    node->flag |= NODE_FLAG_IN;
    /* add replicas of server */
    __conhash_add_replicas(conhash, node);

    return 0;
}

int conhash_del_node(struct conhash_s *conhash, struct node_s *node)
{
   if((conhash==NULL) || (node==NULL))
    {
        return -1;
    }
    /* check node first */
    if(!(node->flag&NODE_FLAG_INIT) || !(node->flag&NODE_FLAG_IN))
    {
        return -1;
    }
    node->flag &= (~NODE_FLAG_IN);
    /* add replicas of server */
    __conhash_del_replicas(conhash, node);

    return 0;
}

const struct node_s* conhash_lookup(const struct conhash_s *conhash, const u_int object)
{
    long hash;
    const util_rbtree_node_t *rbnode;
    if((conhash==NULL) || (conhash->ivnodes==0))
    {
        return NULL;
    }
    /* calc hash value */
    hash = conhash->cb_hashfunc(object);
    rbnode = util_rbtree_lookup(&(conhash->vnode_tree), hash);
    if(rbnode != NULL)
    {
        struct virtual_node_s *vnode = rbnode->data;
        return vnode->node;
    }
    return NULL;
}

unsigned int conhash_getMigrationHash(unsigned int *map, int map_len, int partition_num, int factor)
{
	/*factor is used for fine grained hash chosen: unused now*/
	//char str[100]; /*the magic number 100 is for temperary usage*/
	int i;
	unsigned int *alias_map = malloc(map_len * sizeof(unsigned int));
	memcpy(alias_map, map, map_len*sizeof(unsigned int));
	/*for(i = 0; i < map_len; i ++){
	    printf("before: partition %d\t hash: %u\n", i, alias_map[i]);
	}*/
	__conhash_quicksort(alias_map, map_len);
	//puts("alias sorted hashes________________");
	/*for(i = 0; i < map_len; i ++){
	    printf("partition %d\t hash: %ld\n", i, alias_map[i]);
	}*/
	unsigned int partition_hash = map[partition_num];
	//printf("The hash for partition %d is %ld\n", partition_num, partition_hash);
	for(i = 0; i < map_len; ++i){
		if(alias_map[i] == partition_hash){
			break;
		}
	}
	if(i == map_len){
		/*didn't find the hash*/
		//printf("conhash_getMigrationHash: Cannot find partition %d's hash in the hash map\n", partition_num);
		free(alias_map);
		return -1;
	}
	if(i == 0){/*if the hash is the smallest one in the map*/
		free(alias_map);
		return partition_hash/factor;
	}else{
		unsigned int pre_hash = alias_map[i - 1];
		free(alias_map);
		return (partition_hash + pre_hash)/factor;
	}
}

void conhash_mapPartitionToHash(unsigned int *map, int partition_num, unsigned int hash)
{
    map[partition_num] = hash;
}

