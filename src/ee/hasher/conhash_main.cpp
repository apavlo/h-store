#include "libconhash/conhash.h"
#include "conhash_main.h"

Conhash::Conhash(int n, struct node_s *g_nodes)
{
	conhash = conhash_init(NULL, n, g_nodes);
}

Conhash::~Conhash()
{
	if(conhash != NULL)
	{
		puts("See you");
		conhash_fini(conhash);
	}
}

Conhash::lookup(const char *object)
{
	return conhash_lookup(conhash, object);
}

