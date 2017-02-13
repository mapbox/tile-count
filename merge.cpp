#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <queue>
#include "merge.hpp"
#include "header.hpp"
#include "serial.hpp"

bool merge::operator<(const merge &m) const {
	// > 0 so that lowest quadkey comes first
	return memcmp(map + start, m.map + m.start, INDEX_BYTES) > 0;
}

void do_merge(struct merge *merges, int nmerges, FILE *f, int bytes, long long nrec, int zoom) {
	std::priority_queue<merge> q;

	unsigned long long mask = 0;
	if (zoom != 0) {
		mask = 0xFFFFFFFFFFFFFFFFULL << (64 - 2 * zoom);
	}

	int i;
	long long along = 0;
	long long reported = -1;

	for (i = 0; i < nmerges; i++) {
		if (merges[i].start < merges[i].end) {
			q.push(merges[i]);
		}
	}

	unsigned long long current_index = 0;
	unsigned long long current_count = 0;

	while (q.size() != 0) {
		merge head = q.top();
		q.pop();

		unsigned long long new_index = read64(head.map + head.start) & mask;
		unsigned long long count = read32(head.map + head.start + INDEX_BYTES);

		if (new_index < current_index) {
			fprintf(stderr, "Internal error: file out of order: %llx vs %llx\n", read64(head.map + head.start), current_index);
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

		head.start += bytes;
		if (head.start < head.end) {
			q.push(head);
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
