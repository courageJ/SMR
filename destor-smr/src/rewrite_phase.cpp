/*
 * In the phase,
 * we mark the chunks required to be rewriting.
 */
#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "backup.h"
extern struct structdestor destor;
static pthread_t rewrite_t;
SyncQueue* rewrite_queue;
extern SyncQueue* dedup_queue;
extern SyncQueue* chunk_queue;
/* Descending order */
struct structrewritebuffer rewrite_buffer;



extern struct structjcr jcr;
gint g_record_descmp_by_length(struct containerRecord* a,
		struct containerRecord* b, gpointer user_data) {
	return b->size - a->size;
}

gint g_record_cmp_by_id(struct containerRecord* a, struct containerRecord* b,
		gpointer user_data) {
	return a->cid - b->cid;
}

static void init_rewrite_buffer() {
	rewrite_buffer.chunk_queue = g_queue_new();
	rewrite_buffer.container_record_seq = g_sequence_new(free);
	rewrite_buffer.num = 0;
	rewrite_buffer.size = 0;
}

/*
 * return 1 if buffer is full;
 * return 0 if buffer is not full.
 */
int rewrite_buffer_push(struct chunk* c) {
	g_queue_push_tail(rewrite_buffer.chunk_queue, c);

	if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)
			|| CHECK_CHUNK(c, CHUNK_SEGMENT_START) || CHECK_CHUNK(c, CHUNK_SEGMENT_END))
		return 0;

	if (c->id != TEMPORARY_ID) {
		assert(CHECK_CHUNK(c, CHUNK_DUPLICATE));
		struct containerRecord tmp_record;
		tmp_record.cid = c->id;
		GSequenceIter *iter = g_sequence_lookup(
				rewrite_buffer.container_record_seq, &tmp_record,
				(GCompareDataFunc )g_record_cmp_by_id,
				NULL);
		if (iter == NULL) {
			struct containerRecord* record =(containerRecord*) malloc(
					sizeof(struct containerRecord));
			record->cid = c->id;
			record->size = c->size;
			/* We first assume it is out-of-order */
			record->out_of_order = 1;
			g_sequence_insert_sorted(rewrite_buffer.container_record_seq,
					record, (GCompareDataFunc )g_record_cmp_by_id, NULL);
		} else {
			struct containerRecord* record =(containerRecord*) g_sequence_get(iter);
			assert(record->cid == c->id);
			record->size += c->size;
		}
	}

	rewrite_buffer.num++;
	rewrite_buffer.size += c->size;

	if (rewrite_buffer.num >= destor.rewrite_algorithm[1]) {
		assert(rewrite_buffer.num == destor.rewrite_algorithm[1]);
		return 1;
	}

	return 0;
}

struct chunk* rewrite_buffer_top() {
	return (chunk*)g_queue_peek_head(rewrite_buffer.chunk_queue);
}

struct chunk* rewrite_buffer_pop() {
	struct chunk* c = (chunk*)g_queue_pop_head(rewrite_buffer.chunk_queue);

	if (c && !CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
			&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) && !CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
		/* A normal chunk */
		/*
		if (CHECK_CHUNK(c, CHUNK_DUPLICATE) && c->id != TEMPORARY_ID) {
			GSequenceIter *iter = g_sequence_lookup(
					rewrite_buffer.container_record_seq, &c->id,
					(GCompareDataFunc )g_record_cmp_by_id, NULL);
			assert(iter);
			struct containerRecord* record = (containerRecord*)g_sequence_get(iter);
			record->size -= c->size;
			if (record->size == 0)
				g_sequence_remove(iter);

        	// History-Aware Rewriting 
            if (destor.rewrite_enable_har && CHECK_CHUNK(c, CHUNK_DUPLICATE))
                har_check(c);
		}
		*/
		rewrite_buffer.num--;
		rewrite_buffer.size -= c->size;
	}

	return c;
}

/*
 * If rewrite is disable.
 */
static void* no_rewrite(void* arg) {
	while (1) {
		struct chunk* c = (chunk*)sync_queue_pop(dedup_queue);

		if (c == NULL)
			break;

		sync_queue_push(rewrite_queue, c);

        /* History-Aware Rewriting */
        if (destor.rewrite_enable_har && CHECK_CHUNK(c, CHUNK_DUPLICATE))
            har_check(c);
    }

    sync_queue_term(rewrite_queue);

    return NULL;
}

void start_rewrite_phase() {
    rewrite_queue = sync_queue_new(1000);

    init_rewrite_buffer();

    init_har();
    if (destor.rewrite_algorithm[0] == REWRITE_NO) {
        pthread_create(&rewrite_t, NULL, no_rewrite, NULL);
    } else if (destor.rewrite_algorithm[0]
            == REWRITE_CFL_SELECTIVE_DEDUPLICATION) {
        pthread_create(&rewrite_t, NULL, cfl_rewrite, NULL);
    } else if (destor.rewrite_algorithm[0] == REWRITE_CONTEXT_BASED) {
        pthread_create(&rewrite_t, NULL, cbr_rewrite, NULL);
		NOTICE("start_rewrite_phase: cbr_level: %f\n", destor.rewrite_cbr_minimal_utility);
    } else if (destor.rewrite_algorithm[0] == REWRITE_CAPPING) {
        pthread_create(&rewrite_t, NULL, cap_rewrite, NULL);
		NOTICE("start_rewrite_phase: capping_level: %d\n", destor.rewrite_capping_level);
    } else if (destor.rewrite_algorithm[0] == REWRITE_SMR) {
        pthread_create(&rewrite_t, NULL, smr_rewrite, NULL);
		NOTICE("start_rewrite_phase: smr_level: %d\n", destor.rewrite_smr_level);
    } else if (destor.rewrite_algorithm[0] == REWRITE_NED) {
        pthread_create(&rewrite_t, NULL, ned_rewrite, NULL);
		NOTICE("start_rewrite_phase: ned_level: %d\n", destor.rewrite_ned_level);
    } else {
        fprintf(stderr, "Invalid rewrite algorithm\n");
        exit(1);
    }

}

void stop_rewrite_phase() {
    pthread_join(rewrite_t, NULL);
    NOTICE("rewrite phase stops successfully!");
}
