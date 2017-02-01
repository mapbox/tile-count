struct merge {
	long long start;
	long long end;
	unsigned char *map;  // used for merge
	int fd;		     // used for sort

	struct merge *next;
};

void merge(struct merge *merges, int nmerges, FILE *f, int bytes, long long nrec, int zoom);
