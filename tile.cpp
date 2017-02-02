#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sqlite3.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <set>
#include <map>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include "tippecanoe/projection.hpp"
#include "header.hpp"
#include "serial.hpp"
#include "tippecanoe/mvt.hpp"
#include "tippecanoe/mbtiles.hpp"

void usage(char **argv) {
	fprintf(stderr, "Usage: %s [-fs] -z zoom -o out.mbtiles file.count\n", argv[0]);
}

struct tile {
	long long x;
	long long y;
	int z;
	std::vector<long long> count;
	bool active;

	tile(size_t dim, int zoom) {
		x = -1;
		y = -1;
		z = zoom;
		count.resize((1 << dim) * (1 << dim), 0);
		active = false;
	}
};

struct tiler {
	std::vector<tile> tiles;
	std::vector<tile> partial_tiles;
	size_t start;
	size_t end;
	long long bbox[4];
	long long midx, midy;

	unsigned char *map;
	size_t zooms;
	size_t detail;
	sqlite3 *outdb;
	bool square;
};

void make_tile(sqlite3 *outdb, tile const &tile, int z, int detail, bool square) {
	mvt_layer layer;
	layer.name = "count";
	layer.version = 2;
	layer.extent = 4096;

	for (size_t y = 0; y < (1 << detail); y++) {
		for (size_t x = 0; x < (1 << detail); x++) {
			long long count = tile.count[y * (1 << detail) + x];

			if (count != 0) {
				mvt_feature feature;
				feature.type = mvt_point;
				feature.geometry.push_back(mvt_geometry(mvt_moveto, x << (12 - detail), y << (12 - detail)));

				if (square) {
					feature.type = mvt_polygon;
					feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 1) << (12 - detail), (y + 0) << (12 - detail)));
					feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 1) << (12 - detail), (y + 1) << (12 - detail)));
					feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 0) << (12 - detail), (y + 1) << (12 - detail)));
					feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 0) << (12 - detail), (y + 0) << (12 - detail)));
				}

				mvt_value val;
				val.type = mvt_uint;
				val.numeric_value.uint_value = count;
				layer.tag(feature, "count", val);
				layer.features.push_back(feature);
			}
		}
	}

	mvt_tile mvt;
	mvt.layers.push_back(layer);

	std::string compressed = mvt.encode();
	if (compressed.size() > 500000) {
		fprintf(stderr, "Tile is too big: %zu\n", compressed.size());
		exit(EXIT_FAILURE);
	}

	static pthread_mutex_t db_lock = PTHREAD_MUTEX_INITIALIZER;

	if (pthread_mutex_lock(&db_lock) != 0) {
		perror("pthread_mutex_lock");
		exit(EXIT_FAILURE);
	}

	mbtiles_write_tile(outdb, z, tile.x, tile.y, compressed.data(), compressed.size());

	if (pthread_mutex_unlock(&db_lock) != 0) {
		perror("pthread_mutex_unlock");
		exit(EXIT_FAILURE);
	}
}

void *run_tile(void *p) {
	tiler *t = (tiler *) p;

	long long seq = 0;
	long long percent = -1;
	long long max = 0;

	unsigned long long oindex = 0;
	for (size_t i = t->start; i < t->end; i++) {
		unsigned long long index = read64(t->map + HEADER_LEN + i * RECORD_BYTES);
		unsigned long long count = read32(t->map + HEADER_LEN + i * RECORD_BYTES + INDEX_BYTES);
		seq++;

		if (oindex > index) {
			fprintf(stderr, "out of order: %llx vs %llx\n", oindex, index);
		}
		oindex = index;

		long long npercent = 100 * seq / (t->end - t->start);
		if (npercent != percent) {
			percent = npercent;
			fprintf(stderr, "  %lld%%\r", percent);
		}

		unsigned wx, wy;
		decode(index, &wx, &wy);

		if (wx < t->bbox[0]) {
			t->bbox[0] = wx;
		}
		if (wy < t->bbox[1]) {
			t->bbox[1] = wy;
		}
		if (wx > t->bbox[2]) {
			t->bbox[2] = wx;
		}
		if (wy > t->bbox[3]) {
			t->bbox[3] = wy;
		}

		for (size_t z = 0; z < t->zooms; z++) {
			unsigned tx = wx, ty = wy;
			if (z + t->detail != 32) {
				tx >>= (32 - (z + t->detail));
				ty >>= (32 - (z + t->detail));
			}

			unsigned px = tx, py = ty;
			if (t->detail != 32) {
				px &= ((1 << t->detail) - 1);
				py &= ((1 << t->detail) - 1);

				tx >>= t->detail;
				ty >>= t->detail;
			} else {
				tx = 0;
				ty = 0;
			}

			if (t->tiles[z].x != tx || t->tiles[z].y != ty) {
				if (t->tiles[z].active) {
					make_tile(t->outdb, t->tiles[z], z, t->detail, t->square);
				}

				t->tiles[z].active = true;
				t->tiles[z].x = tx;
				t->tiles[z].y = ty;
				t->tiles[z].count.resize(0);
				t->tiles[z].count.resize((1 << t->detail) * (1 << t->detail), 0);
			}

			t->tiles[z].count[py * (1 << t->detail) + px] += count;

			if (t->tiles[z].count[py * (1 << t->detail) + px] > max) {
				max = t->tiles[z].count[py * (1 << t->detail) + px];
				t->midx = wx;
				t->midy = wy;
			}
		}
	}

	for (size_t z = 0; z < t->zooms; z++) {
		if (t->tiles[z].active) {
			make_tile(t->outdb, t->tiles[z], z, t->detail, t->square);
		}
	}

	return NULL;
}

int main(int argc, char **argv) {
	extern int optind;
	extern char *optarg;

	char *outfile = NULL;
	int zoom = -1;
	bool force = false;
	bool square = false;

	int i;
	while ((i = getopt(argc, argv, "fz:o:s")) != -1) {
		switch (i) {
		case 'f':
			force = true;
			break;

		case 's':
			square = true;
			break;

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

	if (optind + 1 != argc || zoom < 0 || outfile == NULL) {
		usage(argv);
		exit(EXIT_FAILURE);
	}

	if (force) {
		unlink(outfile);
	}
	sqlite3 *outdb = mbtiles_open(outfile, argv, false);

	size_t detail = 9;
	size_t zooms = zoom - detail + 1;
	if (zoom < (signed) (detail + 1)) {
		zooms = 1;
		detail = zoom;
	}

	size_t cpus = sysconf(_SC_NPROCESSORS_ONLN);
	std::vector<tiler> tilers;
	tilers.resize(cpus);

	for (size_t j = 0; j < cpus; j++) {
		for (size_t z = 0; z < zooms; z++) {
			tilers[j].tiles.push_back(tile(detail, z));
		}
		tilers[j].bbox[0] = tilers[j].bbox[1] = UINT_MAX;
		tilers[j].bbox[2] = tilers[j].bbox[3] = 0;
		tilers[j].midx = tilers[j].midy = 0;
		tilers[j].zooms = zooms;
		tilers[j].detail = detail;
		tilers[j].outdb = outdb;
		tilers[j].square = square;
	}

	for (; optind < argc; optind++) {
		struct stat st;
		if (stat(argv[optind], &st) != 0) {
			perror(optind[argv]);
			exit(EXIT_FAILURE);
		}

		int fd = open(argv[optind], O_RDONLY);
		if (fd < 0) {
			perror(argv[optind]);
			exit(EXIT_FAILURE);
		}
		unsigned char *map = (unsigned char *) mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
		if (map == MAP_FAILED) {
			perror("mmap");
			exit(EXIT_FAILURE);
		}

		if (memcmp(map, header_text, HEADER_LEN) != 0) {
			fprintf(stderr, "%s: not a tile-count file\n", argv[optind]);
			exit(EXIT_FAILURE);
		}

		size_t records = (st.st_size - HEADER_LEN) / RECORD_BYTES;
		for (size_t j = 0; j < cpus; j++) {
			tilers[j].map = map;
			tilers[j].start = j * records / cpus;
			if (j > 0) {
				tilers[j - 1].end = tilers[j].start;
			}
		}
		tilers[cpus - 1].end = records;

		pthread_t pthreads[cpus];
		for (size_t j = 0; j < cpus; j++) {
			if (pthread_create(&pthreads[j], NULL, run_tile, &tilers[j]) != 0) {
				perror("pthread_create");
				exit(EXIT_FAILURE);
			}
		}

		for (size_t j = 0; j < cpus; j++) {
			void *retval;

			if (pthread_join(pthreads[j], &retval) != 0) {
				perror("pthread_join");
				exit(EXIT_FAILURE);
			}
		}
	}

	long long file_bbox[4] = {UINT_MAX, UINT_MAX, 0, 0};
	for (size_t j = 0; j < cpus; j++) {
		if (tilers[j].bbox[0] < file_bbox[0]) {
			file_bbox[0] = tilers[j].bbox[0];
		}
		if (tilers[j].bbox[1] < file_bbox[1]) {
			file_bbox[1] = tilers[j].bbox[1];
		}
		if (tilers[j].bbox[2] > file_bbox[2]) {
			file_bbox[2] = tilers[j].bbox[2];
		}
		if (tilers[j].bbox[3] > file_bbox[3]) {
			file_bbox[3] = tilers[j].bbox[3];
		}
	}

	double minlat = 0, minlon = 0, maxlat = 0, maxlon = 0, midlat = 0, midlon = 0;

	tile2lonlat(tilers[0].midx, tilers[0].midy, 32, &midlon, &midlat);  // XXX unstable
	tile2lonlat(file_bbox[0], file_bbox[1], 32, &minlon, &maxlat);
	tile2lonlat(file_bbox[2], file_bbox[3], 32, &maxlon, &minlat);

	type_and_string tas;
	tas.type = VT_NUMBER;
	tas.string = "count";

	layermap_entry lme(0);
	lme.file_keys.insert(tas);
	lme.minzoom = 0;
	lme.maxzoom = zooms - 1;

	std::map<std::string, layermap_entry> lm;
	lm.insert(std::pair<std::string, layermap_entry>("count", lme));

	mbtiles_write_metadata(outdb, outfile, 0, zooms - 1, minlat, minlon, maxlat, maxlon, midlat, midlon, false, "", lm);
	mbtiles_close(outdb, argv);
}
