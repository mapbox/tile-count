struct merge {
	long long start;
	long long end;
	unsigned char *map;  // used for merge
	int fd;		     // used for sort

	bool operator<(const merge &m) const;
};

void do_merge(struct merge *merges, size_t nmerges, FILE *f, int bytes, long long nrec, int zoom);
