// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "randomgenerator.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <ctime>

#include "base/assert.h"

using std::vector;

RandomGenerator::RandomGenerator() {
#ifdef HAVE_RANDOM_R
    // Set the random state to zeros. glibc will attempt to access the old state if not NULL.
    memset(&state, 0, sizeof(state));
    int result = initstate_r(static_cast<unsigned int>(time(NULL)), state_array,
            sizeof(state_array), &state);
    ASSERT(result == 0);
#else
    seed(time(NULL));
#endif
}

int32_t RandomGenerator::random() {
    int32_t rand_int;
#ifdef HAVE_RANDOM_R
    int error = random_r(&state, &rand_int);
    ASSERT(error == 0);
#else
    rand_int = nrand48(state);
#endif
    ASSERT(0 <= rand_int && rand_int <= maximum());
    return rand_int;
}

void RandomGenerator::seed(unsigned int value) {
#ifdef HAVE_RANDOM_R
    int error = srandom_r(value, &state);
    ASSERT(error == 0);
#else
    int copy_bytes = std::min(sizeof(value), sizeof(state));
    int remaining_bytes = sizeof(value) - copy_bytes;
    memcpy(state, &value, copy_bytes);
    memset(state + copy_bytes, 0, remaining_bytes);
#endif
}

void RandomGenerator::shuffle(vector<int>* members, int num_shuffle) {
    assert(0 <= num_shuffle && num_shuffle <= (int) members->size());
    for (int i = 0; i < num_shuffle; ++i) {
        int id = random() % ((int) members->size() - i) + i;
        assert(0 <= id && id < members->size());

        // swap to assign the id to position i
        std::swap((*members)[id], (*members)[i]);
    }

    members->resize(num_shuffle);
}
