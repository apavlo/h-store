
/* Copyright (C) 2010. sparkling.liang@hotmail.com. All rights reserved. */

#include "conhash.h"
#include "conhash_inter.h"

struct conhash_s* conhash_init(conhash_cb_hashfunc pfhash, int n, struct node_s *g_nodes)
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
        int i;
        char str[128];
        if(pfhash != NULL)
        {
            conhash->cb_hashfunc = pfhash;
        }
        else
        {
			puts("Default conhasher");
            conhash->cb_hashfunc = __conhash_hash_def;
        }
		util_rbtree_init(&conhash->vnode_tree);
		for(i = 0; i < n; i ++){
			sprintf(str, "%d", i);
			conhash_set_node(&g_nodes[i], str, 1);
			conhash_add_node(conhash, &g_nodes[i]);
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

void conhash_set_node(struct node_s *node, const char *iden, u_int replica)
{
	//puts("Guys, I am here!");
    strncpy(node->iden, iden, sizeof(node->iden)-1);
	//puts("Cannot believe I am here");
    node->replicas = replica;
    node->flag = NODE_FLAG_INIT;
	//puts("Impossible!!!");
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

const struct node_s* conhash_lookup(const struct conhash_s *conhash, const char *object)
{
    long hash;
    const util_rbtree_node_t *rbnode;
    if((conhash==NULL) || (conhash->ivnodes==0) || (object==NULL)) 
    {
        return NULL;
    }
    /* calc hash value */
    hash = conhash->cb_hashfunc(object);
   	printf("Hash value is %ld\n", hash); 
    rbnode = util_rbtree_lookup(&(conhash->vnode_tree), hash);
    if(rbnode != NULL)
    {
		printf("FIND NODE!!!!  ");
        struct virtual_node_s *vnode = rbnode->data;
		printf("%s\n", vnode->node->iden);
        return vnode->node;
    }
    return NULL;
}


