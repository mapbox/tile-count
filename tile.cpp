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
#include <algorithm>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <limits.h>
#include <math.h>
#include "tippecanoe/projection.hpp"
#include "header.hpp"
#include "serial.hpp"
#include "kll.hpp"
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
	std::vector<kll<long long>> quantiles;
	size_t pass;
	size_t start;
	size_t end;
	long long bbox[4];
	long long midx, midy;

	unsigned char *map;
	size_t zooms;
	size_t detail;
	sqlite3 *outdb;
	bool square;
	int maxzoom;

	volatile int *progress;
	size_t shard;
	size_t cpus;
};

std::vector<mvt_geometry> merge_rings(std::vector<mvt_geometry> g) {
	std::multimap<mvt_geometry, mvt_geometry> trimmed;

	for (size_t i = 1; i < g.size(); i++) {
		if (g[i].op == mvt_lineto) {
			bool found = false;

			auto r = trimmed.equal_range(g[i]);
			for (auto a = r.first; a != r.second; a++) {
				if (a->second == g[i - 1]) {
					found = true;
					trimmed.erase(a);
					break;
				}
			}

			if (!found) {
				trimmed.insert(std::pair<mvt_geometry, mvt_geometry>(g[i - 1], g[i]));
			}
		}
	}

	std::vector<mvt_geometry> out;

	while (trimmed.size() != 0) {
		auto a = trimmed.begin();
		mvt_geometry start = a->first;
		mvt_geometry here = a->second;
		trimmed.erase(a);
		out.push_back(mvt_geometry(mvt_moveto, start.x, start.y));
		out.push_back(mvt_geometry(mvt_lineto, here.x, here.y));

		while (1) {
			auto there = trimmed.find(here);
			if (there == trimmed.end()) {
				fprintf(stderr, "Internal error: no path");
				for (size_t i = 0; i < g.size(); i++) {
					if (g[i].op == mvt_moveto) {
						fprintf(stderr, "\n");
					}
					fprintf(stderr, "%d,%d ", g[i].x, g[i].y);
				}
				fprintf(stderr, "\n");
				exit(EXIT_FAILURE);
			}

			here = there->second;
			trimmed.erase(there);
			out.push_back(mvt_geometry(mvt_lineto, here.x, here.y));

			if (here == start) {
				break;
			}
		}
	}

	return out;
}

void gather_quantile(kll<long long> &kll, tile const &tile) {
}

void make_tile(sqlite3 *outdb, tile const &tile, int z, int detail, bool square, int maxzoom) {
	mvt_layer layer;
	layer.name = "count";
	layer.version = 2;
	layer.extent = 4096;

	std::vector<long long> values;
	for (size_t y = 0; y < (1U << detail); y++) {
		for (size_t x = 0; x < (1U << detail); x++) {
			long long count = tile.count[y * (1 << detail) + x];
			if (count != 0) {
				values.push_back(count);
			}
		}
	}

	std::sort(values.begin(), values.end());

	size_t buckets = 100;
	std::vector<mvt_feature> features;
	features.resize(buckets);

	std::vector<long long> largest;
	for (size_t i = 0; i < buckets; i++) {
		largest.push_back(0);
	}

	for (size_t y = 0; y < (1U << detail); y++) {
		for (size_t x = 0; x < (1U << detail); x++) {
			long long count = tile.count[y * (1 << detail) + x];
			if (count != 0) {
				auto bound = std::upper_bound(values.begin(), values.end(), count);
				size_t index;

				if (bound == values.end()) {
					index = values.size() - 1;
				} else if (bound == values.begin()) {
					fprintf(stderr, "Shouldn't have been able to find the first element\n");
					exit(EXIT_FAILURE);
				} else {
					index = bound - values.begin() - 1;
				}

				size_t bucket = index * buckets / values.size();
				// printf("%lld: %zu\n", count, bucket);
				if (bucket >= features.size()) {
					fprintf(stderr, "internal error: bucket lookup %zu in %zu\n", bucket, features.size());
				}
				mvt_feature &feature = features[bucket];

				if (count > largest[bucket]) {
					largest[bucket] = count;
				}

				if (square) {
					feature.type = mvt_polygon;

					feature.geometry.push_back(mvt_geometry(mvt_moveto, x << (12 - detail), y << (12 - detail)));
					feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 1) << (12 - detail), (y + 0) << (12 - detail)));
					feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 1) << (12 - detail), (y + 1) << (12 - detail)));
					feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 0) << (12 - detail), (y + 1) << (12 - detail)));
					feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 0) << (12 - detail), (y + 0) << (12 - detail)));
				}
			}
		}
	}

	double scale = exp(log(2.5) * (z - maxzoom));

#define FIRST_PERCENTILE 5

	for (size_t i = FIRST_PERCENTILE; i < features.size(); i++) {
		if (features[i].geometry.size() != 0) {
			// features[i].geometry = merge_rings(features[i].geometry);

			{
				mvt_value val;
				val.type = mvt_double;
				val.numeric_value.double_value = largest[i] * scale;
				layer.tag(features[i], "density", val);
			}

#if 0
			{
				mvt_value val;
				val.type = mvt_uint;
				val.numeric_value.uint_value = largest[i];
				layer.tag(features[i], "count", val);
			}
#endif

			layer.features.push_back(features[i]);
		}
	}

	if (layer.features.size() > 0) {
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
}

void calc_tile_edges(size_t z, long long x, long long y, unsigned long long &start, unsigned long long &end) {
	start = encode(x << (32 - z), y << (32 - z));
	end = start;

	for (size_t i = 0; i < 32 - z; i++) {
		end |= 3ULL << (2 * i);
	}
}

void *run_tile(void *p) {
	tiler *t = (tiler *) p;

	if (t->start >= t->end) {
		return NULL;
	}

	unsigned long long first = read64(t->map + HEADER_LEN + t->start * RECORD_BYTES);
	unsigned long long last = read64(t->map + HEADER_LEN + (t->end - 1) * RECORD_BYTES);

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
			t->progress[t->shard] = percent;

			int sum = 0;
			for (size_t j = 0; j < t->cpus; j++) {
				sum += t->progress[j];
			}
			sum /= t->cpus;

			fprintf(stderr, "  %lu%%\r", sum / 2 + 50 * t->pass);
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
					unsigned long long first_for_tile, last_for_tile;
					calc_tile_edges(z, t->tiles[z].x, t->tiles[z].y, first_for_tile, last_for_tile);

					// printf("%zu/%lld/%lld: %llx (%llx %llx) %llx\n", z, t->tiles[z].x, t->tiles[z].y, first, first_for_tile, last_for_tile, last);

					if (first_for_tile >= first && last_for_tile <= last) {
						if (t->pass == 0) {
							gather_quantile(t->quantiles[z], t->tiles[z]);
						} else {
							make_tile(t->outdb, t->tiles[z], z, t->detail, t->square, t->maxzoom);
						}
					} else {
						t->partial_tiles.push_back(t->tiles[z]);
					}
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
			unsigned long long first_for_tile, last_for_tile;
			calc_tile_edges(z, t->tiles[z].x, t->tiles[z].y, first_for_tile, last_for_tile);

			// printf("%zu/%lld/%lld: %llx (%llx %llx) %llx\n", z, t->tiles[z].x, t->tiles[z].y, first, first_for_tile, last_for_tile, last);

			if (first_for_tile >= first && last_for_tile <= last) {
				if (t->pass == 0) {
					gather_quantile(t->quantiles[z], t->tiles[z]);
				} else {
					make_tile(t->outdb, t->tiles[z], z, t->detail, t->square, t->maxzoom);
				}
			} else {
				t->partial_tiles.push_back(t->tiles[z]);
			}
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
	bool square = true;

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

	size_t cpus = sysconf(_SC_NPROCESSORS_ONLN);
	double minlat = 0, minlon = 0, maxlat = 0, maxlon = 0, midlat = 0, midlon = 0;

	for (size_t pass = 0; pass < 2; pass++) {
		volatile int progress[cpus];
		std::vector<tiler> tilers;
		tilers.resize(cpus);

		for (size_t j = 0; j < cpus; j++) {
			for (size_t z = 0; z < zooms; z++) {
				tilers[j].tiles.push_back(tile(detail, z));
				tilers[j].quantiles.push_back(kll<long long>());
			}
			tilers[j].bbox[0] = tilers[j].bbox[1] = UINT_MAX;
			tilers[j].bbox[2] = tilers[j].bbox[3] = 0;
			tilers[j].midx = tilers[j].midy = 0;
			tilers[j].zooms = zooms;
			tilers[j].detail = detail;
			tilers[j].outdb = outdb;
			tilers[j].square = square;
			tilers[j].progress = progress;
			tilers[j].progress[j] = 0;
			tilers[j].cpus = cpus;
			tilers[j].shard = j;
			tilers[j].maxzoom = zooms - 1;
			tilers[j].pass = pass;
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

		// Collect and consolidate partially counted tiles

		std::map<std::vector<unsigned>, tile> partials;
		for (size_t j = 0; j < cpus; j++) {
			for (size_t k = 0; k < tilers[j].partial_tiles.size(); k++) {
				std::vector<unsigned> key;
				key.push_back(tilers[j].partial_tiles[k].z);
				key.push_back(tilers[j].partial_tiles[k].x);
				key.push_back(tilers[j].partial_tiles[k].y);

				auto a = partials.find(key);

				if (a == partials.end()) {
					partials.insert(std::pair<std::vector<unsigned>, tile>(key, tilers[j].partial_tiles[k]));
				} else {
					for (size_t x = 0; x < (1U << detail) * (1U << detail); x++) {
						a->second.count[x] += tilers[j].partial_tiles[k].count[x];
					}
				}
			}
		}

		for (auto a = partials.begin(); a != partials.end(); a++) {
			if (pass == 0) {
				gather_quantile(tilers[0].quantiles[a->second.z], a->second);
			} else {
				make_tile(outdb, a->second, a->second.z, detail, square, zooms - 1);
			}
		}

		if (pass == 0) {
		} else {
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

			tile2lonlat(tilers[0].midx, tilers[0].midy, 32, &midlon, &midlat);  // XXX unstable
			tile2lonlat(file_bbox[0], file_bbox[1], 32, &minlon, &maxlat);
			tile2lonlat(file_bbox[2], file_bbox[3], 32, &maxlon, &minlat);
		}
	}

	type_and_string tas;
	tas.type = VT_NUMBER;
	tas.string = "count";

	type_and_string tas2;
	tas2.type = VT_NUMBER;
	tas2.string = "density";

	layermap_entry lme(0);
	lme.file_keys.insert(tas);
	lme.file_keys.insert(tas2);
	lme.minzoom = 0;
	lme.maxzoom = zooms - 1;

	std::map<std::string, layermap_entry> lm;
	lm.insert(std::pair<std::string, layermap_entry>("count", lme));

	mbtiles_write_metadata(outdb, outfile, 0, zooms - 1, minlat, minlon, maxlat, maxlon, midlat, midlon, false, "", lm);
	mbtiles_close(outdb, argv);
}
