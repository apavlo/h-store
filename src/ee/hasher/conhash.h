#include "../../../third_party/cpp/libconhash/conhash.h"

#define HANLDED_PARTIONS 9999
class Conhash {
private:
    struct conhash_s *conhash;
	struct node_s g_nodes[HANLDED_PARTIONS];
public:
	Conhash(unsigned int n);
	~Conhash(void);
    const struct node_s* lookup(const unsigned object);		
};

