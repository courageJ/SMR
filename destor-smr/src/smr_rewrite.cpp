
  
#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "backup.h"
#include "containerstore.h"
#include "kvstore.h"
#include "index.h"
#include <vector>
#include <string>
#include <queue>
#include <iostream>
#include <algorithm>
using namespace std;

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


extern vector <chunk *> rewrite_buffer_chunk_pt;
extern GHashTable *real_containerid_to_tmp;
extern int64_t tmp_to_real_containerid[MAX_CONTAINER_COUNT];

extern struct containerchunkcount{
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

extern vector <string> all_fp[MAX_CONTAINER_COUNT];

extern bool is_container_selected[MAX_CONTAINER_COUNT];
extern vector <int> container_selected;

void *smr_rewrite(void* arg) {
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
		//SMR
		//1. travel all containers to find the container with maximum chunkcount and set it the start
		//2. if num > 0, add the start to set V

		//3. for each num-1 round, travel to find the most benefit,
		//   update chunkcover, container id
		//		for each round
		//			check all chunks belonging to the container, that is newer to the chunkcover, and get the max chunk count(id)
		int32_t length = cur_container_count;		
		int32_t num;
		if (length > destor.rewrite_smr_level) {
			num = destor.rewrite_smr_level;
		}
		else {
			num = length;
		}

		memset(is_container_selected, 0, sizeof(is_container_selected));
		container_selected.clear();

		GHashTable *chunk_cover;
		chunk_cover = g_hash_table_new(g_str_hash, g_str_equal);
		if (num > 0) {
			for (int subround = 0; subround < num; subround++) {
				int64_t preDedup = -1;
				int64_t preCtrID = -1;
				for (int cur_ctr_id = 0; cur_ctr_id < cur_container_count; cur_ctr_id++) {
					if (is_container_selected[cur_ctr_id]) continue;
					int64_t curDedup = 0;
					for (int chunk_id = 0; chunk_id < all_fp[cur_ctr_id].size(); chunk_id++) {
						string cur_chunk = all_fp[cur_ctr_id][chunk_id];
						char code[41];
						code[40] = 0;
						strcpy(code, cur_chunk.c_str());
						containerid *idpt = (containerid *)g_hash_table_lookup(chunk_cover, &code);
					    if (!idpt) {
					    	curDedup++;
					    }
					}
					if (curDedup > preDedup) {
						preCtrID = cur_ctr_id;
						preDedup = curDedup;
					}
				}
				if (preDedup == -1) {
					break;//end early
				}
				container_selected.push_back(preCtrID);
				is_container_selected[preCtrID] = 1;
				containerid tmpid = preCtrID;
				containerid realid = tmp_to_real_containerid[tmpid];
				assert(realid != -1);
				struct containerRecord* r = (struct containerRecord*) malloc(
						sizeof(struct containerRecord));
				r->cid = realid;
				r->size = all_fp[tmpid].size();
				r->out_of_order = 0;
				g_hash_table_insert(top, &r->cid, r);
				for (int j = 0; j < all_fp[tmpid].size(); j++) {
					g_hash_table_insert(chunk_cover, (gpointer)all_fp[tmpid][j].c_str(), &r->cid);
				}
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
	int32_t length = cur_container_count;		
	int32_t num;
	if (length > destor.rewrite_smr_level) {
		num = destor.rewrite_smr_level;
	}
	else {
		num = length;
	}
	memset(is_container_selected, 0, sizeof(is_container_selected));
	container_selected.clear();

	GHashTable *chunk_cover;
	chunk_cover = g_hash_table_new(g_str_hash, g_str_equal);
	if (num > 0) {
		for (int subround = 0; subround < num; subround++) {
			int64_t preDedup = -1;
			int64_t preCtrID = -1;
			for (int cur_ctr_id = 0; cur_ctr_id < cur_container_count; cur_ctr_id++) {
				if (is_container_selected[cur_ctr_id]) continue;
				int64_t curDedup = 0;
				for (int chunk_id = 0; chunk_id < all_fp[cur_ctr_id].size(); chunk_id++) {
					string cur_chunk = all_fp[cur_ctr_id][chunk_id];
					char code[41];
					code[40] = 0;
					strcpy(code, cur_chunk.c_str());
					containerid *idpt = (containerid *)g_hash_table_lookup(chunk_cover, &code);
				    if (!idpt) {
				    	curDedup++;
				    }
				}
				if (curDedup > preDedup) {
					preCtrID = cur_ctr_id;
					preDedup = curDedup;
				}
			}
			if (preDedup == -1) {
				break;//end early
			}
			container_selected.push_back(preCtrID);
			is_container_selected[preCtrID] = 1;
			containerid tmpid = preCtrID;
			containerid realid = tmp_to_real_containerid[tmpid];
			assert(realid != -1);
			struct containerRecord* r = (struct containerRecord*) malloc(
					sizeof(struct containerRecord));
			r->cid = realid;
			r->size = all_fp[tmpid].size();
			r->out_of_order = 0;
			g_hash_table_insert(top, &r->cid, r);
			for (int j = 0; j < all_fp[tmpid].size(); j++) {
				g_hash_table_insert(chunk_cover, &all_fp[tmpid][j], &r->cid);
			}
		}
	}

	for (int chunk_id = 0; chunk_id < rewrite_buffer_chunk_pt.size(); chunk_id++) {
		c = rewrite_buffer_chunk_pt[chunk_id];	
		if(!CHECK_CHUNK(c, CHUNK_DUPLICATE)) continue;
		char code[41];
	    code2hash(c->fp, (unsigned char *)code);
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

