#include "kvstore.h"

extern void init_kvstore_htable();
extern void close_kvstore_htable();
extern int64_t* kvstore_htable_lookup(unsigned char* key);
extern void kvstore_htable_update(unsigned char* key, int64_t id);
extern void kvstore_htable_delete(unsigned char* key, int64_t id);

/*
 * Mapping a fingerprint (or feature) to the prefetching unit.
 */
extern struct structdestor destor;
void (*close_kvstore)();
int64_t* (*kvstore_lookup)(unsigned char *key);
void (*kvstore_update)(unsigned char *key, int64_t id);
void (*kvstore_delete)(unsigned char* key, int64_t id);

void init_kvstore() {

    switch(destor.index_key_value_store){
    	case INDEX_KEY_VALUE_HTABLE:
    		init_kvstore_htable();

    		close_kvstore = close_kvstore_htable;
    		kvstore_lookup = kvstore_htable_lookup;
    		kvstore_update = kvstore_htable_update;
    		kvstore_delete = kvstore_htable_delete;

    		break;
    	default:
    		WARNING("Invalid key-value store!");
    		exit(1);
    }
}
