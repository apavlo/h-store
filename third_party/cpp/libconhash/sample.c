

#include <stdio.h>
#include <stdlib.h>

#include "conhash.h"

struct node_s g_nodes[64];
int main()
{
    int i;
    const struct node_s *node;
    char str[128];
	
    /* init conhash instance */
    Conhash *conhash = new Conhash(10);
	for(i=0; i<10; i++){
		sprintf(str, "%d", i);
		node = conhash->lookup(str);
		printf("Node id is %s\n", node->iden);
	}
    
    delete(conhash);
    return 0;
}
