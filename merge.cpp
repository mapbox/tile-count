#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <queue>
#include <iterator>
#include <pthread.h>
#include <sys/mman.h>
#include "merge.hpp"
#include "header.hpp"
#include "serial.hpp"

struct merger {
	unsigned char *start;
	unsigned char *end;

	bool operator<(const merger &m) const {
		// > 0 so that lowest quadkey comes first
		return memcmp(start, m.start, INDEX_BYTES) > 0;
	}
};

unsigned char *do_merge1(std::vector<merger> &merges, size_t nmerges, unsigned char *f, int bytes, long long nrec, int zoom) {
	std::priority_queue<merger> q;

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
		merger head = q.top();
		q.pop();

		unsigned long long new_index = read64(head.start) & mask;
		unsigned long long count = read32(head.start + INDEX_BYTES);

		if (new_index < current_index) {
			fprintf(stderr, "Internal error: file out of order: %llx vs %llx\n", read64(head.start), current_index);
			exit(EXIT_FAILURE);
		}

		if (new_index != current_index || current_count + count > MAX_COUNT) {
			if (current_count != 0) {
				write64(&f, current_index);
				write32(&f, current_count);
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
		write64(&f, current_index);
		write32(&f, current_count);
	}

	return f;
}

struct merge_arg {
	std::vector<merger> mergers;
	size_t off;
	size_t outlen;
	size_t len;
	unsigned char *out;
	int zoom;
};

struct finder {
	unsigned char data[RECORD_BYTES];

	bool operator<(const finder &f) const {
		return memcmp(data, f.data, INDEX_BYTES) < 0;
	}
};

void *run_merge(void *va) {
	merge_arg *a = (merge_arg *) va;

	// XXX fix progress
	size_t nrec = 0;
	for (size_t i = 0; i < a->mergers.size(); i++) {
		nrec += (a->mergers[i].end - a->mergers[i].start) / RECORD_BYTES;
	}

	unsigned char *end = do_merge1(a->mergers, a->mergers.size(), a->out + a->off, RECORD_BYTES, nrec, a->zoom);
	a->outlen = end - (a->out + a->off);

	return NULL;
}

void do_merge(struct merge *merges, size_t nmerges, int f, int bytes, long long nrec, int zoom) {
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
			unsigned char *p = look.data;
			write64(&p, beginning[i] & mask);

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

	size_t off = HEADER_LEN;
	for (size_t i = 0; i < cpus; i++) {
		args[i].off = off;

		for (size_t j = 0; j < nmerges; j++) {
			printf("range: %zu: %zu\n", j, (args[i].mergers[j].end - args[i].mergers[j].start));
			off += args[i].mergers[j].end - args[i].mergers[j].start;
		}

		args[i].len = off - args[i].off;
	}

	if (off != (size_t)(nrec * bytes + HEADER_LEN)) {
		fprintf(stderr, "Internal error: Wrong total size: %zu vs %lld * %d == %lld\n", off, nrec, bytes, nrec * bytes);
		exit(EXIT_FAILURE);
	}

	if (ftruncate(f, off) != 0) {
		perror("resize output file");
		exit(EXIT_FAILURE);
	}

	void *map = mmap(NULL, off, PROT_READ | PROT_WRITE, MAP_SHARED, f, 0);
	if (map == MAP_FAILED) {
		perror("mmap output file");
		exit(EXIT_FAILURE);
	}

	memcpy(map, header_text, HEADER_LEN);

	pthread_t threads[cpus];
	for (size_t i = 0; i < cpus; i++) {
		args[i].out = (unsigned char *) map;
		args[i].zoom = zoom;

		if (pthread_create(&threads[i], NULL, run_merge, &args[i]) != 0) {
			perror("pthread_create");
			exit(EXIT_FAILURE);
		}
	}

	size_t outpos = HEADER_LEN;
	size_t inpos = HEADER_LEN;

	for (size_t i = 0; i < cpus; i++) {
		void *ret;
		if (pthread_join(threads[i], &ret) != 0) {
			perror("pthread_join");
			exit(EXIT_FAILURE);
		}

		if (inpos != outpos) {
			memmove((unsigned char *) map + outpos, (unsigned char *) map + inpos, args[i].outlen);
		}
		outpos += args[i].outlen;
		inpos += args[i].len;
	}

	if (munmap(map, off) != 0) {
		perror("munmap");
		exit(EXIT_FAILURE);
	}

	if (ftruncate(f, outpos) != 0) {
		perror("shrink output file");
		exit(EXIT_FAILURE);
	}
}
