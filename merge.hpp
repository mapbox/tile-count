struct merge {
	long long start;
	long long end;
	unsigned char *map;  // used for merge
	int fd;		     // used for sort
};

void do_merge(struct merge *merges, size_t nmerges, int f, int bytes, long long nrec, int zoom, bool quiet, size_t cpus, size_t also_todo, size_t also_did);
