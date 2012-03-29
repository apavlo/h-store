#include "libconhash/conhash.h"

class Conhash {
private:
    struct conhash_s *conhash;
	struct node_s g_nodes[64];
public:
	Conhash(int n);
	~Conhash(void);
    const struct node_s* lookup(const char *object);		
};

