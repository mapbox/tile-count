#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sqlite3.h>
#include <string>
#include <vector>
#include <set>
#include <map>
#include "projection.hpp"
#include "header.hpp"
#include "serial.hpp"
#include "mvt.hpp"
#include "mbtiles.hpp"

void usage(char **argv) {
	fprintf(stderr, "Usage: %s -z zoom -o out.mbtiles file.count\n", argv[0]);
}

struct tile {
	long long x;
	long long y;
	std::vector<long long> count;
	bool active;

	tile(size_t dim) {
		x = -1;
		y = -1;
		count.resize((1 << dim) * (1 << dim), 0);
		active = false;
	}
};

void make_tile(sqlite3 *outdb, tile const &tile, int z, int detail) {
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

	mbtiles_write_tile(outdb, z, tile.x, tile.y, compressed.data(), compressed.size());
}

int main(int argc, char **argv) {
	extern int optind;
	extern char *optarg;

	char *outfile = NULL;
	int zoom = -1;
	bool force = false;

	int i;
	while ((i = getopt(argc, argv, "fz:o:")) != -1) {
		switch (i) {
		case 'f':
			force = true;
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

	size_t zooms = zoom - 7;
	size_t detail = 7;

	std::vector<tile> tiles;
	for (size_t z = 0; z < zooms; z++) {
		tiles.push_back(tile(detail));
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

		unsigned char buf[16];
		while (fread(buf, 16, 1, f) == 1) {
			unsigned long long index = read64(buf);
			unsigned long long count = read64(buf + 8);

			unsigned wx, wy;
			decode(index, &wx, &wy);

			for (size_t z = 0; z < zooms; z++) {
				unsigned tx = wx, ty = wy;
				if (z + detail != 32) {
					tx >>= (32 - (z + detail));
					ty >>= (32 - (z + detail));
				}

				unsigned px = tx, py = ty;
				if (detail != 32) {
					px &= ((1 << detail) - 1);
					py &= ((1 << detail) - 1);

					tx >>= detail;
					ty >>= detail;
				} else {
					tx = 0;
					ty = 0;
				}

				if (tiles[z].x != tx || tiles[z].y != ty) {
					if (tiles[z].active) {
						make_tile(outdb, tiles[z], z, detail);
					}

					tiles[z].active = true;
					tiles[z].x = tx;
					tiles[z].y = ty;
					tiles[z].count.resize(0);
					tiles[z].count.resize((1 << detail) * (1 << detail), 0);
				}

				tiles[z].count[py * (1 << detail) + px] += count;
			}
		}

		fclose(f);

		for (size_t z = 0; z < zooms; z++) {
			if (tiles[z].active) {
				make_tile(outdb, tiles[z], z, detail);
			}
		}
	}

	double minlat = 0, minlon = 0, maxlat = 0, maxlon = 0, midlat = 0, midlon = 0;  // XXX

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
