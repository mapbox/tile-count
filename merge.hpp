struct merge {
	long long start;
	long long end;
	unsigned char *map;

	struct merge *next;
};

void merge(struct merge *merges, int nmerges, FILE *f, int bytes, long long nrec);
