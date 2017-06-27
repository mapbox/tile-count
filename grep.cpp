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

bool quiet = false;
unsigned long long mask = 0;

void usage(char **argv) {
	fprintf(stderr, "Usage: %s [-v] [-s binsize] in.count [in.json ...]\n", argv[0]);
}

int maskcmp(const void *a, const void *b) {
	unsigned char *ca = (unsigned char *) a;
	unsigned char *cb = (unsigned char *) b;

	unsigned long la = read64(ca) & mask;
	unsigned long lb = read64(cb) & mask;

	if (la < lb) {
		return -1;
	} else if (la > lb) {
		return 1;
	} else {
		return 0;
	}
}

bool check_coordinates(json_pull *jp, json_object *j, json_object *coordinates, const char *map, size_t maplen) {
	if (coordinates->type == JSON_ARRAY) {
		if (coordinates->length >= 2 && coordinates->array[0]->type == JSON_NUMBER &&
		    coordinates->array[1]->type == JSON_NUMBER) {
			long long x, y;
			projection->project(coordinates->array[0]->number, coordinates->array[1]->number, 32, &x, &y);
			unsigned long long index = encode(x, y);

			unsigned char ix[INDEX_BYTES];
			unsigned char *p = ix;
			write64(&p, index);

			void *found = bsearch(ix, map + HEADER_LEN, (maplen - HEADER_LEN) / RECORD_BYTES, RECORD_BYTES, maskcmp);

			if (found != NULL) {
				return true;
			}
		} else {
			for (size_t i = 0; i < coordinates->length; i++) {
				if (check_coordinates(jp, j, coordinates->array[i], map, maplen)) {
					return true;
				}
			}
		}
	}

	return false;
}

void read_json(FILE *in, const char *fname, long long &seq, const char *map, size_t maplen, bool reverse) {
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

		json_object *type = json_hash_get(j, "type");
		if (type == NULL || type->type != JSON_STRING) {
			continue;
		}

		if (strcmp(type->string, "Feature") != 0) {
			if (strcmp(type->string, "FeatureCollection") == 0) {
				json_free(j);
			}

			continue;
		}

		json_object *geometry = json_hash_get(j, "geometry");
		if (geometry == NULL) {
			fprintf(stderr, "%s:%d: feature with no geometry\n", fname, jp->line);
			json_free(j);
			continue;
		}

		json_object *coordinates = json_hash_get(geometry, "coordinates");
		if (coordinates == NULL) {
			fprintf(stderr, "%s:%d: geometry with no coordinates\n", fname, jp->line);
			json_free(j);
			continue;
		}

		if (seq % 100000 == 0) {
			if (!quiet) {
				fprintf(stderr, "Read %.1f million records\r", seq / 1000000.0);
			}
		}
		seq++;

		if (check_coordinates(jp, j, coordinates, map, maplen) != reverse) {
			const char *out = json_stringify(j);
			printf("%s\n", out);
			free((void *) out);
		}

		json_free(j);
	}

	json_end(jp);
}

int main(int argc, char **argv) {
	extern int optind;
	extern char *optarg;

	bool reverse = false;
	int zoom = 32;
	size_t cpus = sysconf(_SC_NPROCESSORS_ONLN);

	int i;
	while ((i = getopt(argc, argv, "s:vq")) != -1) {
		switch (i) {
		case 's':
			zoom = atoi(optarg);
			break;

		case 'v':
			reverse = true;
			break;

		case 'q':
			quiet = true;
			break;

		default:
			usage(argv);
			exit(EXIT_FAILURE);
		}
	}

	if (optind >= argc) {
		usage(argv);
		exit(EXIT_FAILURE);
	}

        if (zoom != 0) {
                mask = 0xFFFFFFFFFFFFFFFFULL << (64 - 2 * zoom);
        }

	const char *countfname = argv[optind];
	int fd = open(countfname, O_RDONLY);
	if (fd < 0) {
		perror(countfname);
		exit(EXIT_FAILURE);
	}

	struct stat st;
	if (fstat(fd, &st) != 0) {
		perror(countfname);
		exit(EXIT_FAILURE);
	}

	const char *map = (const char *) mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
	if (map == MAP_FAILED) {
		perror(countfname);
		exit(EXIT_FAILURE);
	}

	optind++;

	long long seq = 0;
	if (optind == argc) {
		read_json(stdin, "standard input", seq, map, st.st_size, reverse);
	} else {
		for (; optind < argc; optind++) {
			FILE *in = fopen(argv[optind], "r");
			if (in == NULL) {
				perror(argv[optind]);
				exit(EXIT_FAILURE);
			} else {
				read_json(in, argv[optind], seq, map, st.st_size, reverse);
				fclose(in);
			}
		}
	}

	return 0;
}
