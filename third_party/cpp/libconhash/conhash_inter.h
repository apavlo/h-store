
/* Copyright (C) 2010. sparkling.liang@hotmail.com. All rights reserved. */

#ifndef __CONHASH_INTER_H_
#define __CONHASH_INTER_H_

#include "configure.h"
#include "md5.h"
#include "util_rbtree.h"


#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <string.h>

/* virtual node structure */
struct virtual_node_s
{
	long hash;
	struct node_s *node; /* pointer to node */
};

/* consistent hashing */
struct conhash_s
{
	util_rbtree_t vnode_tree; /* rbtree of virtual nodes */
    u_int ivnodes; /* virtual node number */
	long (*cb_hashfunc)(const u_int);
};

struct __get_vnodes_s
{
    long *values;
    long size, cur;
};


int __conhash_vnode_cmp(const void *v1, const void *v2);

void __conhash_node2string(const struct node_s *node, u_int replica_idx, u_int *buf, u_int *len);
long __conhash_hash_def(const char *instr);
long __conhash_hash_java_def(const u_int object);
void __conhash_add_replicas(struct conhash_s *conhash, struct node_s *node);
void __conhash_del_replicas(struct conhash_s *conhash, struct node_s *node);

util_rbtree_node_t *__conhash_get_rbnode(struct node_s *node, long hash);
void __conhash_del_rbnode(util_rbtree_node_t *rbnode);
int __conhash_quicksort(long *arr, int elements);

#endif /* end __CONHASH_INTER_H_ */
