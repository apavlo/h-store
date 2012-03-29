#include "libconhash/conhash.h"

class Conhash {
	struct conhash_s *conhash;
	struct node_s g_nodes[64];
public:
	Conhash(int n, g_nodes);
	~Conhash(void);
	struct node_s * lookup(const char *object);		
};

