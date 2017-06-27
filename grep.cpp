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

void usage(char **argv) {
	fprintf(stderr, "Usage: %s [-v] [-s binsize] in.count [in.json ...]\n", argv[0]);
}

void write_point(FILE *out, long long &seq, double lon, double lat, unsigned long long count) {
	if (seq % 100000 == 0) {
		if (!quiet) {
			fprintf(stderr, "Read %.1f million records\r", seq / 1000000.0);
		}
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

void check_coordinates(json_pull *jp, json_object *j, json_object *coordinates) {
}

void read_json(FILE *in, const char *fname, long long &seq) {
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

		check_coordinates(jp, j, coordinates);
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
	while ((i = getopt(argc, argv, "s:v")) != -1) {
		switch (i) {
		case 's':
			zoom = atoi(optarg);
			break;

		case 'v':
			reverse = true;
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

	const char *countfname = argv[optind];
	optind++;

	long long seq = 0;
	if (optind == argc) {
		read_json(stdin, "standard input", seq);
	} else {
		for (; optind < argc; optind++) {
			FILE *in = fopen(argv[optind], "r");
			if (in == NULL) {
				perror(argv[optind]);
				exit(EXIT_FAILURE);
			} else {
				read_json(in, argv[optind], seq);
				fclose(in);
			}
		}
	}

	return 0;
}
