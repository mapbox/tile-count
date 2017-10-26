#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <string>
#include "tippecanoe/projection.hpp"
#include "header.hpp"
#include "serial.hpp"
#include "milo/dtoa_milo.h"

void usage(char **argv) {
	fprintf(stderr, "Usage: %s file.count ...\n", argv[0]);
}

int main(int argc, char **argv) {
	extern int optind;

	int i;
	while ((i = getopt(argc, argv, "")) != -1) {
		switch (i) {
		default:
			usage(argv);
			exit(EXIT_FAILURE);
		}
	}

	if (optind == argc) {
		usage(argv);
		exit(EXIT_FAILURE);
	}

	for (; optind < argc; optind++) {
		FILE *f = fopen(argv[optind], "rb");
		if (f == NULL) {
			perror(optind[argv]);
			exit(EXIT_FAILURE);
		}

		char header[HEADER_LEN];
		if (fread(header, HEADER_LEN, 1, f) != 1) {
			perror("read header");
			exit(EXIT_FAILURE);
		}

		if (memcmp(header, header_text, HEADER_LEN) != 0) {
			fprintf(stderr, "%s: not a tile-count file\n", argv[optind]);
			exit(EXIT_FAILURE);
		}

		unsigned char buf[RECORD_BYTES];
		while (fread(buf, RECORD_BYTES, 1, f) == 1) {
			unsigned long long index = read64(buf);
			unsigned long long count = read32(buf + INDEX_BYTES);

			unsigned x, y;
			decode(index, &x, &y);

			double lon, lat;
			projection->unproject(x, y, 32, &lon, &lat);
			printf("%s,%s,%llu\n", milo::dtoa_milo(lon).c_str(), milo::dtoa_milo(lat).c_str(), count);
		}

		fclose(f);
	}
}
