
/* Copyright (C) 2010. sparkling.liang@hotmail.com. All rights reserved. */

#include "conhash_inter.h"
#include "conhash.h"
#include <math.h>
#define MAX_LEVELS  1000 /*For quick sort*/
#define NO 0
#define YES 1

/* 
 * the default hash function, using md5 algorithm
 * @instr: input string
 */
long __conhash_hash_def(const char *instr)
{
    int i;
    long hash = 0;
    unsigned char digest[16];
    conhash_md5_digest((const u_char*)instr, digest);

    /* use successive 4-bytes from hash as numbers */
    for(i = 0; i < 4; i++)
    {
        hash += ((long)(digest[i*4 + 3]&0xFF) << 24)
            | ((long)(digest[i*4 + 2]&0xFF) << 16)
            | ((long)(digest[i*4 + 1]&0xFF) <<  8)
            | ((long)(digest[i*4 + 0]&0xFF));
    }
	return abs(hash);
}

unsigned int __conhash_hash_java_def(const u_int hash){
    return (unsigned int)hash;
}

void __conhash_node2string(const struct node_s *node, u_int replica_idx, u_int *buf, u_int *len)
{
    *buf = node->iden / replica_idx;
}

void __conhash_add_replicas(struct conhash_s *conhash, struct node_s *node)
{
    u_int i, len;
    u_int hash;
    u_int buff;
    util_rbtree_node_t *rbnode;
    
   /* calc hash value of all virtual nodes */
   __conhash_node2string(node, node->replicas, &buff, &len);
   hash = conhash->cb_hashfunc(buff);
   /* add virtual node, check duplication */
   if(util_rbtree_search(&(conhash->vnode_tree), hash) == NULL)
   {
        rbnode = __conhash_get_rbnode(node, hash);
        if(rbnode != NULL)
        {
            util_rbtree_insert(&(conhash->vnode_tree), rbnode);
            conhash->ivnodes++; 
        }
    }
}

void __conhash_del_replicas(struct conhash_s *conhash, struct node_s *node)
{
    u_int i, len;
    long hash;
    u_int buff;
    struct virtual_node_s *vnode;
    util_rbtree_node_t *rbnode;
    for(i = 0; i < node->replicas; i++)
    {
        /* calc hash value of all virtual nodes */
        __conhash_node2string(node, i, &buff, &len);
        hash = conhash->cb_hashfunc(buff);
        rbnode = util_rbtree_search(&(conhash->vnode_tree), hash);
        if(rbnode != NULL)
        {
            vnode = rbnode->data;
            if((vnode->hash == hash) && (vnode->node == node))
            {
                conhash->ivnodes--;
                util_rbtree_delete(&(conhash->vnode_tree), rbnode);
                __conhash_del_rbnode(rbnode);
            }
        }
    }
}

util_rbtree_node_t *__conhash_get_rbnode(struct node_s *node, long hash)
{
    util_rbtree_node_t *rbnode;
    rbnode = (util_rbtree_node_t *)malloc(sizeof(util_rbtree_node_t));
    if(rbnode != NULL)
    {
        rbnode->key = hash;
        rbnode->data = malloc(sizeof(struct virtual_node_s));
        if(rbnode->data != NULL)
        {
            struct virtual_node_s *vnode = rbnode->data;
            vnode->hash = hash;
            vnode->node = node;
        }
        else
        {
            free(rbnode);
            rbnode = NULL;
        }
    }
    return rbnode;
}

void __conhash_del_rbnode(util_rbtree_node_t *rbnode)
{
    struct virtual_node_s *node;
    node = rbnode->data;
    free(node);
    free(rbnode);
}

long __conhash_getHash(const char *object){
	return __conhash_hash_def(object);
}

//  quickSort
//
//  This public-domain C implementation by Darel Rex Finley.
//
//  * Returns YES if sort was successful, or NO if the nested
//    pivots went too deep, in which case your array will have
//    been re-ordered, but probably not sorted correctly.
//
//  * This function assumes it is called with valid parameters.
//
//  * Example calls:
//    quickSort(&myArray[0],5); // sorts elements 0, 1, 2, 3, and 4
//    quickSort(&myArray[3],5); // sorts elements 3, 4, 5, 6, and 7

int __conhash_quicksort(unsigned int *arr, int elements) {

  unsigned int  piv, beg[MAX_LEVELS], end[MAX_LEVELS], i=0, L, R ;

  beg[0]=0; end[0]=elements;
  while (i>=0) {
    L=beg[i]; R=end[i]-1;
    if (L<R) {
      piv=arr[L]; if (i==MAX_LEVELS-1) return NO;
      while (L<R) {
        while (arr[R]>=piv && L<R) R--; if (L<R) arr[L++]=arr[R];
        while (arr[L]<=piv && L<R) L++; if (L<R) arr[R--]=arr[L]; }
      arr[L]=piv; beg[i+1]=L+1; end[i+1]=end[i]; end[i++]=L; }
    else {
      i--; }}
  return YES; }

