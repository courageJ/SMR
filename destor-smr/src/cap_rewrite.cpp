
  
#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "backup.h"
#include "containerstore.h"
#include "kvstore.h"
#include "index.h"

struct structindexlock{
	/* g_mutex_init() is unnecessary if in static storage. */
	pthread_mutex_t mutex;
	pthread_cond_t cond; // index buffer is not full
	// index buffer is full, waiting
	// if threshold < 0, it indicates no threshold.
	int wait_threshold;
};
extern struct structindexlock index_lock;

extern struct structjcr jcr;
extern SyncQueue* dedup_queue;
static int64_t chunk_num;
extern SyncQueue* rewrite_queue;;
static GHashTable *top;
extern struct structdestor destor;
vector <chunk *> rewrite_buffer_chunk_pt;
GHashTable *real_containerid_to_tmp;
int64_t tmp_to_real_containerid[MAX_CONTAINER_COUNT];

struct containerchunkcount{
	int64_t id;
	int64_t chunkcount;
	bool operator < (const containerchunkcount &a) const {
		return chunkcount > a.chunkcount;
	}
	containerchunkcount() {
		id = -1;
		chunkcount = 0;
	}
}Node[MAX_CONTAINER_COUNT];

vector <string> all_fp[MAX_CONTAINER_COUNT];

bool is_container_selected[MAX_CONTAINER_COUNT];
vector <int> container_selected;

void *cap_rewrite(void* arg) {
	top = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);
	struct chunk *c;
	rewrite_buffer_chunk_pt.clear();
	for (int i = 0; i < MAX_CONTAINER_COUNT; i++) {
		all_fp[i].clear();
	}
	while (1) {
		struct chunk *c = (chunk *)sync_queue_pop(dedup_queue);

		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		if (!rewrite_buffer_push(c)) {
			rewrite_buffer_chunk_pt.push_back(c);
			//1. add all chunk pt into buffer
			TIMER_END(1, jcr.rewrite_time);
			continue;
		}

		//2. get all refered ids of all chunks
		//   add keep all chunk fp into the vector of the container they belongs to
		real_containerid_to_tmp = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);
		memset(tmp_to_real_containerid, -1, sizeof(tmp_to_real_containerid));
		int cur_container_count = 0;

		for (int chunk_id = 0; chunk_id < rewrite_buffer_chunk_pt.size(); chunk_id++) {
			c = rewrite_buffer_chunk_pt[chunk_id];
			if(!CHECK_CHUNK(c, CHUNK_DUPLICATE)) continue;
			//only check all duplicate chunks
			fingerprint cur_fp;
			fingerprint chunk_count_fp;
			fingerprint new_fp;
			for (int i = 0; i < 20; i++) {
				cur_fp[i] = c->fp[i];
				chunk_count_fp[i] = c->fp[i];
				new_fp[i] = c->fp[i];
			}
			chunk_count_fp[0] = 254;
			fingerprint *new_fp_pt =  &new_fp;
			
			pthread_mutex_lock(&index_lock.mutex);
			int64_t *chunk_count_fp_pt = kvstore_lookup(chunk_count_fp);
			int64_t chunk_count = 0;
			if (chunk_count_fp_pt != NULL) chunk_count = *chunk_count_fp_pt;
			for (int i = 1; i <= chunk_count; i++) {
				new_fp[0] = (unsigned char)i;
				int64_t *new_fp_id_pt = kvstore_lookup(new_fp);
				containerid realid = *kvstore_lookup(new_fp);
				containerid *idpt = (containerid *)g_hash_table_lookup(real_containerid_to_tmp, &realid);
				if (!idpt &&((cur_container_count + 1) > MAX_CONTAINER_COUNT)) {
					continue;
				}
		    	containerid *realidpt = (containerid *)malloc(sizeof(containerid));
				containerid *tmpidpt = (containerid *)malloc(sizeof(containerid));
		    	*realidpt = realid;
		    	containerid tmpid;
		    	if(idpt == NULL) {
		    		tmpid = cur_container_count++;
		    		if (cur_container_count > MAX_CONTAINER_COUNT) {
		    			continue;
		    		}
		    		*tmpidpt = tmpid;
		    		g_hash_table_insert(real_containerid_to_tmp, realidpt, tmpidpt);
			    	tmp_to_real_containerid[tmpid] = realid;
		    	}
		    	else {
		    		tmpid = *idpt;
		    	}	
		    	assert(tmp_to_real_containerid[tmpid] == realid);
		    	char code[41];
				hash2code(c->fp, code);
				code[40] = 0;
		    	all_fp[tmpid].push_back(code);	
			}
			pthread_mutex_unlock(&index_lock.mutex);
		}
		for (int i = 0; i < cur_container_count; i++) {
			Node[i].id = i;
			Node[i].chunkcount = all_fp[i].size();
		}
		sort(Node, Node + cur_container_count);		
	
		int32_t length = cur_container_count;		
		int32_t num;
		if (length > destor.rewrite_capping_level) {
			num = destor.rewrite_capping_level;
		}
		else {
			num = length;
		}
		
		GHashTable *chunk_cover;

		int chunk_dedup_ctr = 0, chunk_dedup_ctr_new = 0;
		chunk_cover = g_hash_table_new(g_str_hash, g_str_equal);
		for (int i = 0; i < num; i++) {
			containerid tmpid = Node[i].id;
			containerid realid = tmp_to_real_containerid[tmpid];
			assert(realid != -1);
			struct containerRecord* r = (struct containerRecord*) malloc(
					sizeof(struct containerRecord));
			r->cid = realid;
			r->size = all_fp[tmpid].size();
			r->out_of_order = 0;
			g_hash_table_insert(top, &r->cid, r);
			int j;
			for (j = 0; j < all_fp[tmpid].size(); j++) {
				containerid *idpt = (containerid *)g_hash_table_lookup(chunk_cover, all_fp[tmpid][j].c_str());
				if (idpt == NULL) {
					chunk_dedup_ctr_new++;
				}
				g_hash_table_insert(chunk_cover, (gpointer)all_fp[tmpid][j].c_str(), &r->cid);
			}
			chunk_dedup_ctr += all_fp[tmpid].size();
		}
		int chunk_dedup = 0;
		for (int chunk_id = 0; chunk_id < rewrite_buffer_chunk_pt.size(); chunk_id++) {
			c = rewrite_buffer_chunk_pt[chunk_id];	
			if(!CHECK_CHUNK(c, CHUNK_DUPLICATE)) continue;
			char code[41];
		    hash2code(c->fp, code);
		    code[40] = 0;
				
			containerid *idpt = (containerid *)g_hash_table_lookup(chunk_cover, &code);

		    if (idpt != NULL) {
		    	c->id = *idpt;
		    	chunk_dedup++;
		    }	
		}
		rewrite_buffer_chunk_pt.clear();
		for (int i = 0; i < MAX_CONTAINER_COUNT; i++) {
			all_fp[i].clear();
		}

		int64_t chunk_dedup_real = 0, chunk_all = 0;
		while ((c = rewrite_buffer_pop())) {
			if (!CHECK_CHUNK(c,	CHUNK_FILE_START) 
					&& !CHECK_CHUNK(c, CHUNK_FILE_END)
					&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) 
					&& !CHECK_CHUNK(c, CHUNK_SEGMENT_END)
					&& CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
				if (g_hash_table_lookup(top, &c->id) == NULL) {
					/* not in TOP */
					SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
					VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
							chunk_num, c->id);
				}
				else {
					chunk_dedup_real++;
				}
				chunk_all++;
				chunk_num++;
			}
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_push(rewrite_queue, c);
			TIMER_BEGIN(1);
		}
		g_hash_table_remove_all(top);
		g_hash_table_destroy(real_containerid_to_tmp);
		g_hash_table_remove_all(chunk_cover);
	}

	real_containerid_to_tmp = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);
	memset(tmp_to_real_containerid, -1, sizeof(tmp_to_real_containerid));
	int cur_container_count = 0;

	for (int chunk_id = 0; chunk_id < rewrite_buffer_chunk_pt.size(); chunk_id++) {
		c = rewrite_buffer_chunk_pt[chunk_id];
		if(!CHECK_CHUNK(c, CHUNK_DUPLICATE)) continue;
		//only check all duplicate chunks
		fingerprint cur_fp;
		fingerprint chunk_count_fp;
		fingerprint new_fp;
		for (int i = 0; i < 20; i++) {
			cur_fp[i] = c->fp[i];
			chunk_count_fp[i] = c->fp[i];
			new_fp[i] = c->fp[i];
		}
		chunk_count_fp[0] = 254;
		fingerprint *new_fp_pt =  &new_fp;
		
		pthread_mutex_lock(&index_lock.mutex);
		int64_t *chunk_count_fp_pt = kvstore_lookup(chunk_count_fp);
		int64_t chunk_count = 0;
		if (chunk_count_fp_pt != NULL) chunk_count = *chunk_count_fp_pt;
		for (int i = 1; i <= chunk_count; i++) {
			new_fp[0] = (unsigned char)i;
			int64_t *new_fp_id_pt = kvstore_lookup(new_fp);

			containerid realid = *kvstore_lookup(new_fp);
			containerid *idpt = (containerid *)g_hash_table_lookup(real_containerid_to_tmp, &realid);
			if (!idpt &&((cur_container_count + 1) > MAX_CONTAINER_COUNT)) {
				continue;
			}
	    	containerid *realidpt = (containerid *)malloc(sizeof(containerid));
			containerid *tmpidpt = (containerid *)malloc(sizeof(containerid));
	    	*realidpt = realid;
	    	containerid tmpid;
	    	if(idpt == NULL) {
	    		tmpid = cur_container_count++;
	    		if (cur_container_count > MAX_CONTAINER_COUNT) {
	    			continue;
	    		}
	    		*tmpidpt = tmpid;
	    		g_hash_table_insert(real_containerid_to_tmp, realidpt, tmpidpt);
		    	tmp_to_real_containerid[tmpid] = realid;
	    	}
	    	else {
	    		tmpid = *idpt;
	    	}	
	    	assert(tmp_to_real_containerid[tmpid] == realid);
	    	char code[41];
			hash2code(c->fp, code);
			code[40] = 0;
	    	all_fp[tmpid].push_back(code);		
		}
		pthread_mutex_unlock(&index_lock.mutex);
	}
	for (int i = 0; i < cur_container_count; i++) {
		Node[i].id = i;
		Node[i].chunkcount = all_fp[i].size();
	}
	sort(Node, Node + cur_container_count);
	int32_t length = cur_container_count;		
	int32_t num;
	if (length > destor.rewrite_capping_level) {
		num = destor.rewrite_capping_level;
	}
	else {
		num = length;
	}
	GHashTable *chunk_cover;
	chunk_cover = g_hash_table_new(g_str_hash, g_str_equal);
	for (int i = 0; i < num; i++) {
		containerid tmpid = Node[i].id;
		containerid realid = tmp_to_real_containerid[tmpid];
		assert(realid != -1);
		struct containerRecord* r = (struct containerRecord*) malloc(
				sizeof(struct containerRecord));
		r->cid = realid;
		r->size = all_fp[tmpid].size();
		r->out_of_order = 0;
		g_hash_table_insert(top, &r->cid, r);
		int j;
		for (j = 0; j < all_fp[tmpid].size(); j++) {
			g_hash_table_insert(chunk_cover, (gpointer)all_fp[tmpid][j].c_str(), &r->cid);
		}
	}
	for (int chunk_id = 0; chunk_id < rewrite_buffer_chunk_pt.size(); chunk_id++) {
		c = rewrite_buffer_chunk_pt[chunk_id];	
		if(!CHECK_CHUNK(c, CHUNK_DUPLICATE)) continue;
		char code[41];
	    hash2code(c->fp, code);
	    code[40] = 0;
			
		containerid *idpt = (containerid *)g_hash_table_lookup(chunk_cover, &code);
	    if (idpt != NULL) {
	    	c->id = *idpt;
	    }
	}


	rewrite_buffer_chunk_pt.clear();
	for (int i = 0; i < MAX_CONTAINER_COUNT; i++) {
		all_fp[i].clear();
	}

	while ((c = rewrite_buffer_pop())) {
		if (!CHECK_CHUNK(c,	CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
				&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) && !CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
			if (g_hash_table_lookup(top, &c->id) == NULL) {
				/* not in TOP */
				SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
				VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
						chunk_num, c->id);
			}
			chunk_num++;
		}
		sync_queue_push(rewrite_queue, c);
	}

	g_hash_table_remove_all(top);
	g_hash_table_destroy(real_containerid_to_tmp);
	g_hash_table_remove_all(chunk_cover);

	sync_queue_term(rewrite_queue);

	return NULL;
}
