#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "header.hpp"
#include "serial.hpp"
#include "merge.hpp"

bool quiet = false;

void usage(char **argv) {
	fprintf(stderr, "Usage: %s -s binsize -o out.count in.count ...\n", argv[0]);
}

long long find_max(unsigned char *map, size_t size, int zoom) {
	long long max = 0;
	unsigned long long mask = 0;
	unsigned long long curindex = 0;
	long long curcount = 0;
	int oprogress = 0;

	if (zoom != 0) {
		mask = 0xFFFFFFFFFFFFFFFFULL << (64 - 2 * zoom);
	}

	for (size_t i = 0; i < size; i += RECORD_BYTES) {
		unsigned long long index = read64(map + i) & mask;
		unsigned long long count = read32(map + i + INDEX_BYTES);

		int progress = 100 * i / size;
		if (progress != oprogress) {
			fprintf(stderr, "Find max: %d%%\r", progress);
			oprogress = progress;
		}

		if (index != curindex) {
			if (curcount > max) {
				max = curcount;
			}

			curindex = index;
			curcount = 0;
		}

		curcount += count;
	}

	return max;
}

void normalize1(unsigned char *map, int zoom, FILE *out, size_t start, size_t end, unsigned long long base, unsigned long long max) {
	for (size_t i = start; i < end; i += RECORD_BYTES) {
		unsigned long long index = read64(map + i);
		unsigned long long count = read32(map + i + INDEX_BYTES);

		count *= (double) max / base;

		write64(out, index);
		write32(out, count);
	}
}

void normalize(unsigned char *map, size_t size, int zoom, FILE *out, unsigned long long max) {
	unsigned long long mask = 0;
	unsigned long long curindex = 0;
	long long curcount = 0;
	int oprogress = 0;
	size_t start = 0;

	if (zoom != 0) {
		mask = 0xFFFFFFFFFFFFFFFFULL << (64 - 2 * zoom);
	}

	for (size_t i = 0; i < size; i += RECORD_BYTES) {
		unsigned long long index = read64(map + i) & mask;
		unsigned long long count = read32(map + i + INDEX_BYTES);

		int progress = 100 * i / size;
		if (progress != oprogress) {
			fprintf(stderr, "Normalize: %d%%\r", progress);
			oprogress = progress;
		}

		if (index != curindex) {
			normalize1(map, zoom, out, start, i, curcount, max);

			curindex = index;
			curcount = 0;
			start = i;
		}

		curcount += count;
	}
}

int main(int argc, char **argv) {
	extern int optind;
	extern char *optarg;

	char *outfile = NULL;
	int zoom = 32;
	size_t cpus = sysconf(_SC_NPROCESSORS_ONLN);

	int i;
	while ((i = getopt(argc, argv, "o:s:qp:")) != -1) {
		switch (i) {
		case 's':
			zoom = atoi(optarg);
			break;

		case 'o':
			outfile = optarg;
			break;

		case 'p':
			cpus = atoi(optarg);
			break;

		case 'q':
			quiet = true;
			break;

		default:
			usage(argv);
			exit(EXIT_FAILURE);
		}
	}

	if (argc - optind != 1) {
		usage(argv);
		exit(EXIT_FAILURE);
	}

	int fd = open(argv[optind], O_RDONLY);
	if (fd < 0) {
		perror(argv[optind]);
		exit(EXIT_FAILURE);
	}

	struct stat st;
	if (fstat(fd, &st) != 0) {
		perror("stat");
		exit(EXIT_FAILURE);
	}

	unsigned char *maps = (unsigned char *) mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);

	if (st.st_size < HEADER_LEN || memcmp(maps, header_text, HEADER_LEN) != 0) {
		fprintf(stderr, "%s:%s: Not a tile-count file\n", argv[0], argv[optind]);
		exit(EXIT_FAILURE);
	}

	if (close(fd) < 0) {
		perror("close");
		exit(EXIT_FAILURE);
	}

	long long max = find_max(maps + HEADER_LEN, st.st_size - HEADER_LEN, zoom);

	FILE *out = fopen(outfile, "wb");
	if (out == NULL) {
		perror(outfile);
		exit(EXIT_FAILURE);
	}

	if (fwrite(header_text, HEADER_LEN, 1, out) != 1) {
		perror("write header");
		exit(EXIT_FAILURE);
	}

	normalize(maps + HEADER_LEN, st.st_size - HEADER_LEN, zoom, out, max);

	if (fclose(out) != 0) {
		perror("fclose");
		exit(EXIT_FAILURE);
	}

	return 0;
}
