#include <stdio.h>
#include <stdlib.h>
#include "serial.hpp"

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
