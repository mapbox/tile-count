#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <queue>
#include <iterator>
#include <algorithm>
#include <pthread.h>
#include <sys/mman.h>
#include "merge.hpp"
#include "header.hpp"
#include "serial.hpp"
#include "algorithm_mod.hpp"

struct merger {
	unsigned char *start;
	unsigned char *end;

	bool operator<(const merger &m) const {
		// > 0 so that lowest quadkey comes first
		return memcmp(start, m.start, INDEX_BYTES) > 0;
	}
};

unsigned char *do_merge1(std::vector<merger> &merges, size_t nmerges, unsigned char *f, int bytes, long long nrec, int zoom, bool quiet, volatile int *progress, size_t shard, size_t nshards, size_t also_todo, size_t also_did) {
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
		long long report = 100 * (along + also_did) / (nrec + also_todo);
		if (report != reported) {
			progress[shard] = report;
			int sum = 0;
			for (size_t i = 0; i < nshards; i++) {
				sum += progress[i];
			}
			sum /= nshards;

			if (!quiet) {
				fprintf(stderr, "Merging: %d%%     \r", sum);
			}
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
	bool quiet;
	size_t also_todo;
	size_t also_did;

	int *progress;
	size_t shard;
	size_t nshards;
};

struct finder {
	unsigned char data[RECORD_BYTES];

	bool operator<(const finder &f) const {
		return memcmp(data, f.data, INDEX_BYTES) < 0;
	}
};

void *run_merge(void *va) {
	merge_arg *a = (merge_arg *) va;

	size_t nrec = 0;
	for (size_t i = 0; i < a->mergers.size(); i++) {
		nrec += (a->mergers[i].end - a->mergers[i].start) / RECORD_BYTES;
	}

	unsigned char *end = do_merge1(a->mergers, a->mergers.size(), a->out + a->off, RECORD_BYTES, nrec, a->zoom, a->quiet, a->progress, a->shard, a->nshards, a->also_todo, a->also_did);
	a->outlen = end - (a->out + a->off);

	return NULL;
}

void do_merge(struct merge *merges, size_t nmerges, int f, int bytes, long long nrec, int zoom, bool quiet, size_t cpus, size_t also_todo, size_t also_did) {
	unsigned long long mask = 0;
	if (zoom != 0) {
		mask = 0xFFFFFFFFFFFFFFFFULL << (64 - 2 * zoom);
	}

	unsigned long long beginning[cpus];

	struct val {
		unsigned long long index;
		size_t weight;

		val(long long i, size_t w) {
			index = i;
			weight = w;
		}

		bool operator<(val const &v) const {
			return index < v.index;
		};
	};

	std::vector<val> vals;
	size_t total_weight = 0;
	for (size_t j = 0; j < nmerges; j++) {
		size_t merge_nrec = (merges[j].end - merges[j].start) / bytes;
		if (merge_nrec != 0) {
			for (size_t i = 0; i < cpus; i++) {
				size_t rec = merge_nrec * i / cpus;

				// fprintf(stderr, "%zu: %zu of %zu: %llx\n", j, rec, merge_nrec, read64(merges[j].map + merges[j].start + bytes * rec));

				vals.push_back(val(read64(merges[j].map + merges[j].start + bytes * rec), merge_nrec));
				total_weight += merge_nrec;
			}
		}
	}

	std::sort(vals.begin(), vals.end());

	size_t weight = 0;
	size_t n = 0;
	for (size_t i = 0; i < vals.size(); i++) {
		weight += vals[i].weight;
		if (weight >= total_weight * n / cpus) {
			beginning[n] = vals[i].index;
			n++;

			if (n >= cpus) {
				break;
			}
		}
	}
	for (; n < cpus; n++) {
		if (n == 0) {
			beginning[n] = 0;
		} else {
			beginning[n] = beginning[n - 1];
		}
	}

	int progress[cpus];
	std::vector<merge_arg> args;

	for (size_t i = 0; i < cpus; i++) {
		merge_arg ma;

		progress[i] = 0;
		ma.progress = progress;
		ma.shard = i;
		ma.nshards = cpus;
		ma.also_todo = also_todo;
		ma.also_did = also_did;

		for (size_t j = 0; j < nmerges; j++) {
			if ((merges[j].end - merges[j].start) % sizeof(finder) != 0) {
				fprintf(stderr, "File size is not a multiple of the count unit\n");
				exit(EXIT_FAILURE);
			}

			finder *fs = (finder *) (merges[j].map + merges[j].start);
			finder *fe = (finder *) (merges[j].map + merges[j].end);

			if (fs > fe) {
				fprintf(stderr, "Region being merged ends before it begins: %p to %p\n", fs, fe);
				exit(EXIT_FAILURE);
			}

			finder look;
			unsigned char *p = look.data;
			write64(&p, beginning[i] & mask);

			finder *l = lower_bound1(fs, fe, look);

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

		ma.quiet = quiet;
		args.push_back(ma);
	}

	size_t off = HEADER_LEN;
	for (size_t i = 0; i < cpus; i++) {
		args[i].off = off;

		for (size_t j = 0; j < nmerges; j++) {
			// printf("range: %zu: %zu\n", j, (args[i].mergers[j].end - args[i].mergers[j].start));
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
