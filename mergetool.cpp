#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include "header.hpp"
#include "serial.hpp"
#include "merge.hpp"

void submerge(std::vector<std::string> fnames, int out, const char *argv0, int zoom, int cpus, size_t *also_todo, size_t *also_did);

bool quiet = false;

void usage(char **argv) {
	fprintf(stderr, "Usage: %s -o merged.count file.count ...\n", argv[0]);
}

void trim(char *s) {
	for (; *s != '\0'; s++) {
		if (*s == '\n') {
			*s = '\0';
			break;
		}
	}
}

void addfiles(std::vector<std::string> &list) {
	char s[2000];
	while (fgets(s, 2000, stdin)) {
		trim(s);
		list.push_back(s);
	}
}

int main(int argc, char **argv) {
	extern int optind;
	extern char *optarg;

	char *outfile = NULL;
	int zoom = 32;
	size_t cpus = sysconf(_SC_NPROCESSORS_ONLN);
	bool readfiles = false;

	int i;
	while ((i = getopt(argc, argv, "o:s:qp:F")) != -1) {
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

		case 'F':
			readfiles = true;
			break;

		default:
			usage(argv);
			exit(EXIT_FAILURE);
		}
	}

	std::vector<std::string> fnames;
	for (i = optind; i < argc; i++) {
		fnames.push_back(argv[i]);
	}

	if (readfiles) {
		addfiles(fnames);
	}

	if (fnames.size() == 0) {
		usage(argv);
		exit(EXIT_FAILURE);
	}

	int out = open(outfile, O_CREAT | O_TRUNC | O_RDWR, 0777);
	if (out < 0) {
		perror(outfile);
		exit(EXIT_FAILURE);
	}

	size_t also_todo = 0, also_did = 0;
	submerge(fnames, out, argv[0], zoom, cpus, &also_todo, &also_did);

	return 0;
}

#define MAX_MERGE 50

void submerge(std::vector<std::string> fnames, int out, const char *argv0, int zoom, int cpus, size_t *also_todo, size_t *also_did) {
	std::vector<std::string> todelete;

	if (fnames.size() > MAX_MERGE) {
		size_t subs = MAX_MERGE;

		std::vector<std::string> temps;
		std::vector<int> tempfds;
		std::vector<std::vector<std::string>> subfnames;
		for (size_t i = 0; i < subs; i++) {
			char s[2000] = "/tmp/count.XXXXXX";
			int fd = mkstemp(s);
			if (fd < 0) {
				perror("mkstemp");
			}

			temps.push_back(s);
			tempfds.push_back(fd);
			subfnames.push_back(std::vector<std::string>());
		}

		for (size_t i = 0; i < fnames.size(); i++) {
			subfnames[i % subs].push_back(fnames[i]);

			struct stat st;
			if (stat(fnames[i].c_str(), &st) == 0) {
				*also_todo += st.st_size / RECORD_BYTES;
			}

			if ((st.st_size - HEADER_LEN) % RECORD_BYTES != 0) {
				fprintf(stderr, "%s: file size not a multiple of record length\n", fnames[i].c_str());
				exit(EXIT_FAILURE);
			}
		}

		for (size_t i = 0; i < subs; i++) {
			submerge(subfnames[i], tempfds[i], argv0, zoom, cpus, also_todo, also_did);
			// submerge will have closed the temp fds
		}

		fnames = temps;
		todelete = temps;
	}

	size_t nmerges = fnames.size();
	struct merge merges[nmerges];
	int fds[nmerges];
	unsigned char *maps[nmerges];
	long long to_sort = 0;

	for (size_t i = 0; i < nmerges; i++) {
		fds[i] = open(fnames[i].c_str(), O_RDONLY);
		if (fds[i] < 0) {
			perror(fnames[i].c_str());
			exit(EXIT_FAILURE);
		}

		struct stat st;
		if (fstat(fds[i], &st) != 0) {
			perror("stat");
			exit(EXIT_FAILURE);
		}

		maps[i] = (unsigned char *) mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fds[i], 0);
		if (maps[i] == MAP_FAILED) {
			perror(fnames[i].c_str());
			exit(EXIT_FAILURE);
		}

		if (st.st_size < HEADER_LEN || memcmp(maps[i], header_text, HEADER_LEN) != 0) {
			fprintf(stderr, "%s:%s: Not a tile-count file\n", argv0, fnames[i].c_str());
			exit(EXIT_FAILURE);
		}

		merges[i].start = HEADER_LEN;
		merges[i].end = st.st_size;
		merges[i].map = maps[i];

		to_sort += st.st_size - HEADER_LEN;

		if (close(fds[i]) < 0) {
			perror("close");
			exit(EXIT_FAILURE);
		}
	}

	if (write(out, header_text, HEADER_LEN) != HEADER_LEN) {
		perror("write header");
		exit(EXIT_FAILURE);
	}

	do_merge(merges, nmerges, out, RECORD_BYTES, to_sort / RECORD_BYTES, zoom, quiet, cpus, *also_todo, *also_did);
	if (close(out) != 0) {
		perror("close");
		exit(EXIT_FAILURE);
	}

	*also_did += to_sort / RECORD_BYTES;

	for (size_t i = 0; i < todelete.size(); i++) {
		if (unlink(todelete[i].c_str()) < 0) {
			perror(todelete[i].c_str());
			exit(EXIT_FAILURE);
		}
	}
}
