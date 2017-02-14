#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <queue>
#include <iterator>
#include <pthread.h>
#include "merge.hpp"
#include "header.hpp"
#include "serial.hpp"

bool merge::operator<(const merge &m) const {
	// > 0 so that lowest quadkey comes first
	return memcmp(map + start, m.map + m.start, INDEX_BYTES) > 0;
}


void do_merge1(struct merge *merges, size_t nmerges, FILE *f, int bytes, long long nrec, int zoom) {
	std::priority_queue<merge> q;

	unsigned long long mask = 0;
	if (zoom != 0) {
		mask = 0xFFFFFFFFFFFFFFFFULL << (64 - 2 * zoom);
	}

	long long along = 0;
	long long reported = -1;

	for (size_t i = 0; i < nmerges; i++) {
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

struct merger {
	unsigned char *start;
	unsigned char *end;
};

struct merge_arg {
	std::vector<merger> mergers;
	size_t off;
	unsigned char *out;
};

struct finder {
	unsigned char data[RECORD_BYTES];

	bool operator<(const finder &f) const {
		return memcmp(data, f.data, INDEX_BYTES) < 0;
	}
};

void do_merge(struct merge *merges, size_t nmerges, FILE *f, int bytes, long long nrec, int zoom) {
	unsigned long long mask = 0;
	if (zoom != 0) {
		mask = 0xFFFFFFFFFFFFFFFFULL << (64 - 2 * zoom);
	}

	size_t cpus = sysconf(_SC_NPROCESSORS_ONLN);
	unsigned long long beginning[cpus];

	std::vector<merge_arg> args;

	for (size_t i = 0; i < cpus; i++) {
		beginning[i] = 0;

		if (i != 0) {
			for (size_t j = 0; j < nmerges; j++) {
				size_t merge_nrec = (merges[j].end - merges[j].start) / bytes;
				size_t rec = merge_nrec * i / cpus;
				fprintf(stderr, "%zu: %zu of %zu: %llx\n", j, rec, merge_nrec, read64(merges[j].map + merges[j].start + bytes * rec));

				// Divide here instead of at the end to avoid overflow.
				// XXX Should the contribution to the average be weighted by the size of the file?
				beginning[i] += read64(merges[j].map + merges[j].start + bytes * rec) / nmerges;
			}
		}

		if (i > 0 && beginning[i] < beginning[i - 1]) {
			beginning[i] = beginning[i - 1];
		}

		printf("    %zu: avg %llx\n", i, beginning[i]);

		merge_arg ma;

		for (size_t j = 0; j < nmerges; j++) {
			finder *fs = (finder *) (merges[j].map + merges[j].start);
			finder *fe = (finder *) (merges[j].map + merges[j].end);

			finder look;
			write64(look.data, beginning[i] & mask);

			finder *l = std::lower_bound(fs, fe, look);
			if (l == fe) {
				printf("   at end\n");
			} else {
				printf("   %zu: found %llx\n", j, read64(l->data));
			}

			merger m;
			m.start = (unsigned char *) l;
			if (i == cpus - 1) {
				m.end = (unsigned char *) fe;
			}
			if (i > 0) {
				args[i - 1].mergers[j].end = m.start;
			}

			ma.mergers.push_back(m);
		}

		args.push_back(ma);

		printf("\n");
	}

	do_merge1(merges, nmerges, f, bytes, nrec, zoom);
}
