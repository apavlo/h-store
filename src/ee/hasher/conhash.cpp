#include "libconhash/conhash.h"
#include "conhash.h"

Conhash::Conhash(unsigned int n)
{
	conhash = conhash_init(NULL, n, g_nodes);
}

Conhash::~Conhash()
{
	if(conhash != NULL)
	{
		conhash_fini(conhash);
	}
}

const struct node_s * Conhash::lookup(const unsigned int object)
{
	return conhash_lookup(conhash, object);
}

