#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "merge.hpp"
#include "header.hpp"
#include "serial.hpp"

void insert(struct merge *m, struct merge **head, int bytes) {
	while (*head != NULL && memcmp(m->map + m->start, (*head)->map + (*head)->start, bytes) > 0) {
		head = &((*head)->next);
	}

	m->next = *head;
	*head = m;
}

void do_merge(struct merge *merges, int nmerges, FILE *f, int bytes, long long nrec, int zoom) {
	unsigned long long mask = 0;
	if (zoom != 0) {
		mask = 0xFFFFFFFFFFFFFFFFULL << (64 - 2 * zoom);
	}

	int i;
	struct merge *head = NULL;
	long long along = 0;
	long long reported = -1;

	for (i = 0; i < nmerges; i++) {
		if (merges[i].start < merges[i].end) {
			insert(&(merges[i]), &head, bytes);
		}
	}

	unsigned long long current_index = 0;
	unsigned long long current_count = 0;

	while (head != NULL) {
		unsigned long long new_index = read64(head->map + head->start) & mask;
		unsigned long long count = read32(head->map + head->start + INDEX_BYTES);

		if (new_index < current_index) {
			fprintf(stderr, "Internal error: file out of order: %llx vs %llx\n", read64(head->map + head->start), current_index);
			exit(EXIT_FAILURE);
		}

		if (new_index != current_index || current_count + count > MAX_COUNT) {
			if (current_count != 0) {
				write64(f, current_index);
				write32(f, current_count);
			}

			current_index = new_index;
			current_count = 0;
		}
		current_count += count;

		head->start += bytes;

		struct merge *m = head;
		head = m->next;
		m->next = NULL;

		if (m->start < m->end) {
			insert(m, &head, bytes);
		}

		along++;
		long long report = 100 * along / nrec;
		if (report != reported) {
			fprintf(stderr, "Merging: %lld%%     \r", report);
			reported = report;
		}
	}

	if (current_count != 0) {
		write64(f, current_index);
		write32(f, current_count);
	}
}
