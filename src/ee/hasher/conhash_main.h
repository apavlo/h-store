#include "libconhash/conhash.h"

class Conhash {
	struct conhash_s *conhash;
public:
	Conhash(int n);
	~Conhash(void);
	struct node_s * lookup(const char *object);		
}

