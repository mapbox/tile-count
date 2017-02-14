#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "tippecanoe/projection.hpp"
#include "header.hpp"
#include "serial.hpp"
#include "merge.hpp"

extern "C" {
#include "jsonpull/jsonpull.h"
}

void usage(char **argv) {
	fprintf(stderr, "Usage: %s -o out.count [-z zoom] [in.csv ...]\n", argv[0]);
}

void write_point(FILE *out, long long &seq, double lon, double lat, unsigned long long count) {
	if (seq % 100000 == 0) {
		fprintf(stderr, "Read %.1f million records\r", seq / 1000000.0);
	}
	seq++;

	long long x, y;
	projection->project(lon, lat, 32, &x, &y);
	unsigned long long index = encode(x, y);

	while (count > MAX_COUNT) {
		write64(out, index);
		write32(out, MAX_COUNT);

		count -= MAX_COUNT;
	}

	write64(out, index);
	write32(out, count);
}

void read_json(FILE *out, FILE *in, const char *fname, long long &seq) {
	json_pull *jp = json_begin_file(in);

	while (1) {
		json_object *j = json_read(jp);
		if (j == NULL) {
			if (jp->error != NULL) {
				fprintf(stderr, "%s:%d: %s\n", fname, jp->line, jp->error);
			}

			json_free(jp->root);
			break;
		}

		if (j->type == JSON_HASH) {
			json_free(j);
		} else if (j->type == JSON_ARRAY) {
			if (j->length >= 2) {
				if (j->array[0]->type == JSON_NUMBER && j->array[1]->type == JSON_NUMBER) {
					write_point(out, seq, j->array[0]->number, j->array[1]->number, 1);
				}
			}
			json_free(j);
		}
	}

	json_end(jp);
}

void read_into(FILE *out, FILE *in, const char *fname, long long &seq) {
	int c = getc(in);
	if (c != EOF) {
		ungetc(c, in);
	}
	if (c == '{') {
		read_json(out, in, fname, seq);
		return;
	}

	size_t line = 0;
	char s[2000];
	while (fgets(s, 2000, in)) {
		double lon, lat;
		unsigned long long count;

		line++;
		size_t n = sscanf(s, "%lf,%lf,%llu", &lon, &lat, &count);
		if (n == 2) {
			count = 1;
		} else if (n != 3) {
			fprintf(stderr, "%s:%zu: Can't understand %s", fname, line, s);
			continue;
		}

		write_point(out, seq, lon, lat, count);
	}
}

int indexcmp(const void *p1, const void *p2) {
	return memcmp(p1, p2, INDEX_BYTES);
}

void *run_sort(void *p) {
	struct merge *m = (struct merge *) p;

	void *map = mmap(NULL, m->end - m->start, PROT_READ | PROT_WRITE, MAP_PRIVATE, m->fd, m->start);
	if (map == MAP_FAILED) {
		perror("mmap (sort)");
		exit(EXIT_FAILURE);
	}

	qsort(map, (m->end - m->start) / RECORD_BYTES, RECORD_BYTES, indexcmp);

	// Sorting and then copying avoids the need to
	// write out intermediate stages of the sort.

	void *map2 = mmap(NULL, m->end - m->start, PROT_READ | PROT_WRITE, MAP_SHARED, m->fd, m->start);
	if (map2 == MAP_FAILED) {
		perror("mmap (write)");
		exit(EXIT_FAILURE);
	}

	memcpy(map2, map, m->end - m->start);

	munmap(map, m->end - m->start);
	munmap(map2, m->end - m->start);

	return NULL;
}

void sort_and_merge(int fd, int out, int zoom) {
	size_t cpus = sysconf(_SC_NPROCESSORS_ONLN);

	struct stat st;
	if (fstat(fd, &st) < 0) {
		perror("stat");
		exit(EXIT_FAILURE);
	}

	long long to_sort = st.st_size;
	int bytes = RECORD_BYTES;

	int page = sysconf(_SC_PAGESIZE);
	long long unit = (50 * 1024 * 1024 / bytes) * bytes;
	while (unit % page != 0) {
		unit += bytes;
	}

	size_t nmerges = (to_sort + unit - 1) / unit;
	struct merge merges[nmerges];

	long long start;
	for (start = 0; start < to_sort; start += unit) {
		long long end = start + unit;
		if (end > to_sort) {
			end = to_sort;
		}

		merges[start / unit].start = start;
		merges[start / unit].end = end;
		merges[start / unit].fd = fd;
	}

	for (size_t i = 0; i < nmerges; i += cpus) {
		fprintf(stderr, "Sorting part %zu of %zu     \r", i + 1, nmerges);

		pthread_t pthreads[cpus];
		for (size_t j = 0; j < cpus && i + j < nmerges; j++) {
			if (pthread_create(&pthreads[j], NULL, run_sort, &merges[i + j]) != 0) {
				perror("pthread_create (sort)");
				exit(EXIT_FAILURE);
			}
		}

		for (size_t j = 0; j < cpus && i + j < nmerges; j++) {
			void *retval;

			if (pthread_join(pthreads[j], &retval) != 0) {
				perror("pthread_join (sort)");
				exit(EXIT_FAILURE);
			}
		}
	}

	if (write(out, header_text, HEADER_LEN) != HEADER_LEN) {
		perror("write header");
		exit(EXIT_FAILURE);
	}

	if (to_sort > 0) {
		void *map = mmap(NULL, to_sort, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
		if (map == MAP_FAILED) {
			perror("mmap (for merge)");
			exit(EXIT_FAILURE);
		}

		for (size_t i = 0; i < nmerges; i++) {
			merges[i].map = (unsigned char *) map;
		}

		do_merge(merges, nmerges, out, bytes, to_sort / bytes, zoom);
		munmap(map, st.st_size);
	}
}

int main(int argc, char **argv) {
	extern int optind;
	extern char *optarg;

	char *outfile = NULL;
	int zoom = 32;

	int i;
	while ((i = getopt(argc, argv, "fz:o:")) != -1) {
		switch (i) {
		case 'z':
			zoom = atoi(optarg);
			break;

		case 'o':
			outfile = optarg;
			break;

		default:
			usage(argv);
			exit(EXIT_FAILURE);
		}
	}

	if (outfile == NULL) {
		usage(argv);
		exit(EXIT_FAILURE);
	}

	int fd = open(outfile, O_RDWR | O_CREAT | O_TRUNC, 0777);
	if (fd < 0) {
		perror(outfile);
		exit(EXIT_FAILURE);
	}
	int fd2 = dup(fd);
	if (fd2 < 0) {
		perror("dup output file");
		exit(EXIT_FAILURE);
	}
	FILE *fp = fdopen(fd2, "wb");
	if (fp == NULL) {
		perror("fdopen output file");
		exit(EXIT_FAILURE);
	}
	if (unlink(outfile) != 0) {
		perror("unlink output file");
		exit(EXIT_FAILURE);
	}

	long long seq = 0;
	if (optind == argc) {
		read_into(fp, stdin, "standard input", seq);
	} else {
		for (; optind < argc; optind++) {
			FILE *in = fopen(argv[optind], "r");
			if (in == NULL) {
				perror(argv[optind]);
				exit(EXIT_FAILURE);
			} else {
				read_into(fp, in, argv[optind], seq);
				fclose(in);
			}
		}
	}

	if (fflush(fp) != 0) {
		perror("flush output file");
		exit(EXIT_FAILURE);
	}
	if (fclose(fp) != 0) {
		perror("close output file");
		exit(EXIT_FAILURE);
	}

	int f = open(outfile, O_CREAT | O_TRUNC | O_RDWR, 0777);
	if (f < 0) {
		perror(outfile);
		exit(EXIT_FAILURE);
	}
	sort_and_merge(fd, f, zoom);
	if (close(f) != 0) {
		perror("close");
	}

	return 0;
}
