#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include "projection.hpp"

void usage(char **argv) {
	fprintf(stderr, "Usage: %s -o out.count [-z zoom] [in.csv ...]\n", argv[0]);
}

void write_header(FILE *fp) {
	//          "0123456789ABCDEF"
	fprintf(fp, "tile-count ver 1");
}

struct index {
	unsigned long long index;
	unsigned long long count;
};

void write64(FILE *out, unsigned long long v) {
	for (size_t i = 0; i < 64; i += 8) {
		putc((v >> i) & 0xFF, out);
	}
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
	FILE *fp = fdopen(fd, "wb");
	if (fp == NULL) {
		perror("fdopen output file");
	}
	write_header(fp);

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

	fflush(fp);

	// XXX sort and merge
}
