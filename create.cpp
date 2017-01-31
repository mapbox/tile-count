#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "projection.hpp"

void usage(char **argv) {
	fprintf(stderr, "Usage: %s -o out.count [-z zoom] [in.csv ...]\n", argv[0]);
}

#define HEADER_SIZE 16
void write_header(FILE *fp) {
	//          "0123456789ABCDEF"
	fprintf(fp, "tile-count ver 1");
}

struct index {
	unsigned long long index;
	unsigned long long count;
};

int indexcmp(const void *p1, const void *p2) {
	return memcmp(p1, p2, 8);
}

void write64(FILE *out, unsigned long long v) {
	// Big-endian so memcmp() sorts numerically
	for (ssize_t i = 64 - 8; i >= 0; i -= 8) {
		if (putc((v >> i) & 0xFF, out) == EOF) {
			perror("write data\n");
			exit(EXIT_FAILURE);
		}
	}
}

unsigned long long read64(unsigned char *c) {
	unsigned long long out = 0;

	for (ssize_t i = 0; i < 8; i++) {
		out = (out << 8) | c[i];
	}

	return out;
}

void read_into(FILE *out, FILE *in, const char *fname, long long &seq, int maxzoom) {
	size_t line = 0;
	char s[2000];

	unsigned long long mask = 0xFFFFFFFF;
	if (maxzoom != 32) {
		mask = mask << (32 - maxzoom);
	}
	mask &= 0xFFFFFFFF;

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

		if (seq % 100000 == 0) {
			fprintf(stderr, "Read %.1f million records\r", seq / 1000000.0);
		}
		seq++;

		long long x, y;
		projection->project(lon, lat, 32, &x, &y);
		x &= mask;
		y &= mask;
		unsigned long long index = encode(x, y);

		write64(out, index);
		write64(out, count);
	}
}

struct merge {
	long long start;
	long long end;

	struct merge *next;
};

void insert(struct merge *m, struct merge **head, unsigned char *map, int bytes) {
	while (*head != NULL && memcmp(map + m->start, map + (*head)->start, bytes) > 0) {
		head = &((*head)->next);
	}

	m->next = *head;
	*head = m;
}

void merge(struct merge *merges, int nmerges, unsigned char *map, FILE *f, int bytes, long long nrec) {
	int i;
	struct merge *head = NULL;
	long long along = 0;
	long long reported = -1;

	for (i = 0; i < nmerges; i++) {
		if (merges[i].start < merges[i].end) {
			insert(&(merges[i]), &head, map, bytes);
		}
	}

	unsigned char current_index[8] = {0};
	unsigned long long current_count = 0;

	while (head != NULL) {
		if (memcmp(map + head->start, current_index, 8) != 0) {
			if (current_count != 0) {
				fwrite(current_index, 1, 8, f);
				write64(f, current_count);
			}

			memcpy(current_index, map + head->start, 8);
			current_count = 0;
		}
		current_count += read64(map + head->start + 8);

		head->start += bytes;

		struct merge *m = head;
		head = m->next;
		m->next = NULL;

		if (m->start < m->end) {
			insert(m, &head, map, bytes);
		}

		along++;
		long long report = 100 * along / nrec;
		if (report != reported) {
			fprintf(stderr, "Merging: %lld%%\r", report);
			reported = report;
		}
	}

	if (current_count != 0) {
		fwrite(current_index, 1, 8, f);
		write64(f, current_count);
	}
}

void sort_and_merge(int fd, FILE *out) {
	struct stat st;
	if (fstat(fd, &st) < 0) {
		perror("stat");
		exit(EXIT_FAILURE);
	}

	long long to_sort = st.st_size;
	int bytes = 16;  // 8 of index, 8 of count

	int page = sysconf(_SC_PAGESIZE);
	long long unit = (50 * 1024 * 1024 / bytes) * bytes;
	while (unit % page != 0) {
		unit += bytes;
	}

	int nmerges = (to_sort + unit - 1) / unit;
	struct merge merges[nmerges];

	long long start;
	for (start = 0; start < to_sort; start += unit) {
		long long end = start + unit;
		if (end > to_sort) {
			end = to_sort;
		}

		fprintf(stderr, "Sorting part %lld of %d\r", start / unit + 1, nmerges);

		merges[start / unit].start = start;
		merges[start / unit].end = end;
		merges[start / unit].next = NULL;

		void *map = mmap(NULL, end - start, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, start);
		if (map == MAP_FAILED) {
			perror("mmap (sort)");
			exit(EXIT_FAILURE);
		}

		qsort(map, (end - start) / bytes, bytes, indexcmp);

		// Sorting and then copying avoids the need to
		// write out intermediate stages of the sort.

		void *map2 = mmap(NULL, end - start, PROT_READ | PROT_WRITE, MAP_SHARED, fd, start);
		if (map2 == MAP_FAILED) {
			perror("mmap (write)");
			exit(EXIT_FAILURE);
		}

		memcpy(map2, map, end - start);

		munmap(map, end - start);
		munmap(map2, end - start);
	}

	void *map = mmap(NULL, to_sort, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
	if (map == MAP_FAILED) {
		perror("mmap (for merge)");
		exit(EXIT_FAILURE);
	}

	write_header(out);
	merge(merges, nmerges, (unsigned char *) map, out, bytes, to_sort / bytes);
	munmap(map, st.st_size);
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
		read_into(fp, stdin, "standard input", seq, zoom);
	} else {
		for (; optind < argc; optind++) {
			FILE *in = fopen(argv[optind], "r");
			if (in == NULL) {
				perror(argv[optind]);
				exit(EXIT_FAILURE);
			} else {
				read_into(fp, in, argv[optind], seq, zoom);
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

	fp = fopen(outfile, "wb");
	if (fp == NULL) {
		perror(outfile);
		exit(EXIT_FAILURE);
	}

	sort_and_merge(fd, fp);
	fclose(fp);
}
